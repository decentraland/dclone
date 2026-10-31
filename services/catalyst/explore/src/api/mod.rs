//! The explorer-api surface unity-explorer hits at boot, served in-process:
//! the realm-provider about doc over this node's own sync status
//! ([`realm_provider`]), the asset-bundle registry reads as a cached upstream
//! reader ([`registry`]), and four mirrors with a local overlay and a
//! persisted pass marker: dcl-lists ([`lists`]), the config denylist
//! ([`denylist`]), the feature flags ([`feature_flags`]) and the
//! api.decentraland.org tile grid behind `map.png` ([`map`]), plus the
//! asset-bundle CDN as a bounded read-through cache of upstream's
//! ([`ab_cdn`]), and the explorer statics the old gateway served beside
//! them: the satellite pyramid over mirrored region renders
//! ([`satellite`]), the genesis `world-manifest.json` ([`world_manifest`]),
//! the event poster bucket ([`posters`]) and the OpenSea metadata resolver
//! (`explore::opensea`), each keyed into the ONE blob store or its own
//! cache table. The wire
//! contract is the explorer-boot corpus
//! (`tests/fixtures/explorer-boot/corpus.json`): each
//! service answers under its gateway prefix ([`router`]) or as its own
//! vhost router ([`service_routers`]).
//!
//! Sources are explicit upstream endpoints ([`Sources`]); a private, local or
//! self-referential source is refused at startup ([`Settings::validate`]),
//! and every upstream call re-checks the SSRF guard ([`mirror::fetch`]).

pub mod ab_cdn;
pub mod auth_api;
pub mod denylist;
pub mod feature_flags;
pub mod lists;
pub mod map;
pub mod mirror;
pub mod onboarding;
pub mod posters;
pub mod realm_provider;
pub mod registry;
pub mod runtime_config;
pub mod satellite;
#[cfg(test)]
mod statics_tests;
#[cfg(test)]
mod tests;
pub mod world_manifest;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;
use sqlx::PgPool;
use tower_http::cors::CorsLayer;

use crate::config::Config;
use crate::content::ContentState;
use crate::jobs::{Jobs, PeriodicCfg};
use crate::opensea;

/// The upstream endpoints the readers and mirrors talk to. Base URLs without
/// a trailing slash, except the two that name one document.
#[derive(Clone, Debug)]
pub struct Sources {
    /// dcl-lists: `POST {lists}/pois`, `POST {lists}/banned-names`.
    pub lists: String,
    /// The denylist document itself.
    pub denylist: String,
    /// The feature-flags document itself (`.../explorer.json`).
    pub feature_flags: String,
    /// asset-bundle registry: `POST {registry}/entities/active`, `/profiles`, ...;
    /// empty reads none.
    pub registry: String,
    /// atlas API: `GET {map}/v2/tiles/info`, `GET {map}/v2/tiles`.
    pub map: String,
    /// asset-bundle CDN: `GET {ab_cdn}/v{n}/{cid}_{platform}`, `/LOD/...`, `/manifest/...`;
    /// empty reads none.
    pub ab_cdn: String,
}

impl Sources {
    pub const DEFAULT_LISTS: &str = "https://dcl-lists.decentraland.org";
    pub const DEFAULT_DENYLIST: &str = "https://config.decentraland.org/denylist.json";
    pub const DEFAULT_FEATURE_FLAGS: &str = "https://feature-flags.decentraland.org/explorer.json";
    pub const DEFAULT_REGISTRY: &str = "https://asset-bundle-registry.decentraland.org";
    pub const DEFAULT_MAP: &str = "https://api.decentraland.org";
    pub const DEFAULT_AB_CDN: &str = ab_cdn::DEFAULT_UPSTREAM;

    pub fn defaults() -> Self {
        Self {
            lists: Self::DEFAULT_LISTS.into(),
            denylist: Self::DEFAULT_DENYLIST.into(),
            feature_flags: Self::DEFAULT_FEATURE_FLAGS.into(),
            registry: Self::DEFAULT_REGISTRY.into(),
            map: Self::DEFAULT_MAP.into(),
            ab_cdn: Self::DEFAULT_AB_CDN.into(),
        }
    }

    /// The six sources from [`Config`]; a blank one is none (the mirror
    /// posture sets Decentraland's): its mirror does not run and its
    /// on-demand reader reads nothing.
    pub fn from_config(config: &Config) -> Self {
        Self {
            lists: value_or(&config.explore.explorer_lists_source_url, String::new()),
            denylist: value_or(&config.explore.explorer_denylist_source_url, String::new()),
            feature_flags: value_or(
                &config.explore.explorer_feature_flags_source_url,
                String::new(),
            ),
            registry: value_or(&config.explore.explorer_registry_source_url, String::new()),
            map: value_or(&config.explore.explorer_map_source_url, String::new()),
            ab_cdn: value_or(
                config.ab_cdn.abgen_upstream_ab_cdn.as_deref().unwrap_or(""),
                String::new(),
            ),
        }
    }

    pub fn all(&self) -> [(&'static str, &str); 6] {
        [
            ("EXPLORER_LISTS_SOURCE_URL", &self.lists),
            ("EXPLORER_DENYLIST_SOURCE_URL", &self.denylist),
            ("EXPLORER_FEATURE_FLAGS_SOURCE_URL", &self.feature_flags),
            ("EXPLORER_REGISTRY_SOURCE_URL", &self.registry),
            ("EXPLORER_MAP_SOURCE_URL", &self.map),
            ("ABGEN_UPSTREAM_AB_CDN", &self.ab_cdn),
        ]
    }
}

fn value_or(value: &str, default: String) -> String {
    let v = value.trim().trim_end_matches('/');
    if v.is_empty() { default } else { v.to_string() }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub sources: Sources,
    /// What the realm doc advertises as this node: `PUBLIC_URL`, trailing slash stripped.
    pub public_url: String,
    /// `REALM_NAME`, `main` when unset (the name upstream's genesis realm answers with).
    pub realm_name: String,
    pub network_id: u64,
    /// `COMMS_ADAPTER` / `COMMS_FIXED_ADAPTER` for the realm doc's `comms` block.
    pub comms_adapter: String,
    pub comms_fixed_adapter: String,
    /// `BFF_URL`, `/bff` by default.
    pub bff_url: String,
    /// The one app whose flags document is mirrored (`explorer`).
    pub feature_flags_app: String,
    /// Floor between two downloads of the 38 MB tile grid (`EXPLORER_MAP_MIN_REFRESH_SECS`, 3600).
    pub map_min_refresh: Duration,
    pub version: String,
    pub commit_hash: String,
    /// Tests only: lets a loopback mock stand in for an upstream. Never set
    /// from configuration; `validate` and `mirror::fetch` refuse private
    /// hosts otherwise.
    pub allow_private_sources: bool,
    /// Every host this node answers at (`content::own_hosts`); a source on
    /// one of them is a mirror feeding itself.
    pub own_hosts: Vec<String>,
    /// `EXPLORER_MIRRORS_ENABLED`: whether `spawn_jobs` starts the four
    /// mirrors. The sources are validated either way (the registry reader
    /// uses its on demand).
    pub mirrors_enabled: bool,
    /// What the LAND/Estate metadata's `image` points at (`MAP_IMAGE_BASE_URL`;
    /// `<PUBLIC_URL>/api/v1` by default, the old `.../v1`), no trailing slash.
    pub map_image_base_url: String,
    /// `MAP_EXTERNAL_BASE_URL`: the marketplace the metadata's `external_url`
    /// links to; unset yields no link rather than one to production.
    pub map_external_base_url: Option<String>,
    /// The LAND and Estate registries of `ETH_NETWORK`, lowercase (the
    /// `/v2/contracts/{address}/tokens/{id}` dispatch and `external_url`).
    pub land_contract_address: String,
    pub estate_contract_address: String,
    /// `AB_CDN_ENABLED`: whether `/ab-cdn/*` reads through to the upstream
    /// CDN (`Sources::ab_cdn`); off, every object answers 404.
    pub ab_cdn_enabled: bool,
    /// `AB_CDN_CACHE_DIR` / `AB_CDN_CACHE_MAX_BYTES`: the bounded file
    /// cache of fetched objects (`/var/lib/catalyst/ab-cdn`, 20 GiB).
    pub ab_cdn_cache_dir: std::path::PathBuf,
    pub ab_cdn_cache_max_bytes: u64,
    /// `SATELLITE_MIRROR_UPSTREAM_URL`: the directory serving `manifest.json`
    /// and `<cx>/<cy>.png` region renders (unset: no satellite mirror);
    /// `SATELLITE_MIRROR_INTERVAL_SECS`.
    pub satellite_upstream: Option<String>,
    pub satellite_interval: Duration,
    /// `SATELLITE_MIRROR_ALLOW_PRIVATE_UPSTREAM`: the render rig may be a
    /// private (tailnet) address; the pass reads it unguarded.
    pub satellite_allow_private: bool,
    /// `SATELLITE_SOURCE_BUDGET_MB` (decoded regions) and
    /// `SATELLITE_OUTPUT_ENTRIES` (encoded tiles), the old map service's caches.
    pub satellite_source_budget_bytes: usize,
    pub satellite_output_entries: usize,
    /// `WORLD_MANIFEST_UPSTREAM_URL` / `WORLD_MANIFEST_MIRROR_INTERVAL_SECS`.
    pub world_manifest_upstream: String,
    pub world_manifest_interval: Duration,
    /// `EVENTS_POSTER_UPSTREAM_URL`, read through only while the events
    /// catalogue is mirrored (`EVENTS_MIRROR_UPSTREAM`, the old
    /// `upstream.mirrorEvents` gate); `None`: the poster index alone answers.
    pub poster_upstream: Option<String>,
    pub opensea: opensea::Settings,
    /// `ONBOARDING_API_KEY`: the bearer of `/onboarding/*`; unset, both routes answer 401.
    pub onboarding_api_key: Option<String>,
}

impl Settings {
    pub fn from_config(config: &Config, sources: Sources) -> Self {
        let public_url = config
            .node
            .public_url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(|u| u.trim_end_matches('/').to_string())
            .unwrap_or_else(|| format!("http://{}", config.node.bind_addr()));
        let land = crate::land_authz::Settings::for_network(&config.node.eth_network, None)
            .unwrap_or_else(crate::land_authz::Settings::for_tests);
        Self {
            sources,
            public_url: public_url.clone(),
            realm_name: config
                .node
                .realm_name
                .as_deref()
                .map(str::trim)
                .filter(|r| !r.is_empty())
                .unwrap_or("main")
                .to_string(),
            network_id: network_id(&config.node.eth_network),
            comms_adapter: value_or(&config.explore.comms_adapter, "offline:offline".into()),
            comms_fixed_adapter: value_or(
                config.explore.comms_fixed_adapter.as_deref().unwrap_or(""),
                "offline:offline".into(),
            ),
            bff_url: value_or(&config.explore.bff_url, "/bff".into()),
            feature_flags_app: "explorer".into(),
            map_min_refresh: Duration::from_secs(
                config.explore.explorer_map_min_refresh_secs.max(1),
            ),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit_hash: config.commit_hash().unwrap_or_default(),
            allow_private_sources: false,
            own_hosts: crate::content::own_hosts(config),
            mirrors_enabled: config.explore.explorer_mirrors_enabled,
            map_image_base_url: format!("{public_url}/api/v1"),
            map_external_base_url: None,
            land_contract_address: land.land_registry,
            estate_contract_address: land.estate_registry,
            ab_cdn_enabled: true,
            ab_cdn_cache_dir: std::path::PathBuf::from(&config.explore.ab_cdn_cache_dir),
            ab_cdn_cache_max_bytes: config.explore.ab_cdn_cache_max_bytes,
            satellite_upstream: None,
            satellite_interval: satellite::DEFAULT_INTERVAL,
            satellite_allow_private: false,
            satellite_source_budget_bytes: satellite::DEFAULT_SOURCE_BUDGET_BYTES,
            satellite_output_entries: satellite::DEFAULT_OUTPUT_ENTRIES,
            world_manifest_upstream: value_or(
                &config.explore.world_manifest_upstream_url,
                String::new(),
            ),
            world_manifest_interval: Duration::from_secs(
                config.explore.world_manifest_mirror_interval_secs.max(60),
            ),
            poster_upstream: config
                .explore
                .events_mirror_upstream
                .then(|| value_or(&config.explore.events_poster_upstream_url, String::new()))
                .filter(|u| !u.is_empty()),
            opensea: {
                let rpc_ethereum = opensea::Settings::pick_rpc(
                    config
                        .rpc
                        .rpc_upstream_mainnet
                        .as_deref()
                        .or(config.rpc.rpc_upstream_ethereum.as_deref()),
                    "RPC_UPSTREAM_MAINNET",
                );
                let rpc_polygon = opensea::Settings::pick_rpc(
                    config
                        .rpc
                        .rpc_upstream_polygon
                        .as_deref()
                        .or(config.rpc.rpc_upstream_matic.as_deref()),
                    "RPC_UPSTREAM_POLYGON",
                );
                // CryptoKitties is an Ethereum contract: its API is a remote
                // leg like the chain's, read only beside an Ethereum upstream
                opensea::Settings {
                    fallback: Some(value_or(
                        &config.explore.opensea_fallback_url,
                        String::new(),
                    ))
                    .filter(|u| !u.is_empty()),
                    cryptokitties_api: if rpc_ethereum.is_empty() {
                        String::new()
                    } else {
                        value_or(&config.explore.opensea_cryptokitties_api, String::new())
                    },
                    ipfs_gateways: config
                        .explore
                        .opensea_ipfs_gateways
                        .split(',')
                        .map(str::trim)
                        .filter(|g| !g.is_empty())
                        .map(|g| format!("{}/", g.trim_end_matches('/')))
                        .collect(),
                    rpc_ethereum,
                    rpc_polygon,
                }
            },
            onboarding_api_key: config
                .node
                .onboarding_api_key
                .clone()
                .filter(|k| !k.trim().is_empty()),
        }
    }

    pub fn for_tests(sources: Sources) -> Self {
        Self {
            sources,
            public_url: "http://localhost".into(),
            realm_name: "main".into(),
            network_id: 1,
            comms_adapter: "offline:offline".into(),
            comms_fixed_adapter: "offline:offline".into(),
            bff_url: "/bff".into(),
            feature_flags_app: "explorer".into(),
            map_min_refresh: Duration::from_secs(3600),
            version: "test".into(),
            commit_hash: String::new(),
            allow_private_sources: true,
            own_hosts: vec!["localhost".into()],
            mirrors_enabled: true,
            map_image_base_url: "http://localhost/api/v1".into(),
            map_external_base_url: None,
            land_contract_address: crate::land_authz::LAND_REGISTRY_MAINNET.into(),
            estate_contract_address: crate::land_authz::ESTATE_REGISTRY_MAINNET.into(),
            ab_cdn_enabled: true,
            ab_cdn_cache_dir: std::env::temp_dir()
                .join(format!("catalyst-ab-cdn-tests-{}", std::process::id())),
            ab_cdn_cache_max_bytes: u64::MAX,
            satellite_upstream: None,
            satellite_interval: satellite::DEFAULT_INTERVAL,
            satellite_allow_private: false,
            satellite_source_budget_bytes: 64 << 20,
            satellite_output_entries: 256,
            world_manifest_upstream: String::new(),
            world_manifest_interval: world_manifest::DEFAULT_INTERVAL,
            poster_upstream: None,
            opensea: opensea::Settings {
                fallback: None,
                ..opensea::Settings::defaults()
            },
            onboarding_api_key: None,
        }
    }

    /// Every source must be a public `http(s)` endpoint that is not this
    /// node: a private or single-label host is an SSRF target, and a source
    /// on any of our own hosts (PUBLIC_URL, CONTENT_URL, LAMBDAS_URL, the
    /// bind address) is a mirror feeding itself.
    pub fn validate(&self) -> anyhow::Result<()> {
        let mut own_hosts = self.own_hosts.clone();
        own_hosts.extend(
            url::Url::parse(&self.public_url)
                .ok()
                .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase())),
        );
        for (name, source) in self.sources.all() {
            if source.is_empty() {
                continue;
            }
            let parsed = self
                .guarded_url(source)
                .with_context(|| format!("{name}={source}"))?;
            let host = parsed
                .host_str()
                .map(|h| h.to_ascii_lowercase())
                .unwrap_or_default();
            if own_hosts.contains(&host) {
                anyhow::bail!(
                    "{name}={source} points at this node's own host {host} (PUBLIC_URL, CONTENT_URL, \
                     LAMBDAS_URL or the bind address); a mirror source must be an upstream endpoint, \
                     never a local URL"
                );
            }
        }
        let statics = [
            (
                "SATELLITE_MIRROR_UPSTREAM_URL",
                self.satellite_upstream.as_deref(),
                self.satellite_allow_private,
            ),
            (
                "WORLD_MANIFEST_UPSTREAM_URL",
                Some(self.world_manifest_upstream.as_str()).filter(|u| !u.is_empty()),
                false,
            ),
            (
                "EVENTS_POSTER_UPSTREAM_URL",
                self.poster_upstream.as_deref(),
                false,
            ),
            (
                "OPENSEA_FALLBACK_URL",
                self.opensea.fallback.as_deref(),
                false,
            ),
        ];
        for (name, source, private_ok) in statics {
            let Some(source) = source else { continue };
            let parsed = if private_ok {
                reqwest::Url::parse(source)
                    .ok()
                    .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
                    .with_context(|| format!("{name} is not an http(s) URL"))?
            } else {
                self.guarded_url(source).with_context(|| name.to_string())?
            };
            let host = parsed
                .host_str()
                .map(|h| h.to_ascii_lowercase())
                .unwrap_or_default();
            if own_hosts.contains(&host) {
                anyhow::bail!(
                    "{name} points at this node's own host {host}; an upstream must never be a local URL"
                );
            }
        }
        Ok(())
    }

    /// The SSRF guard every upstream call goes through.
    /// The refusals name the host at most, never the URL: a hosted source
    /// (`ABGEN_UPSTREAM_AB_CDN`, a registry) may carry a token in its path.
    pub fn guarded_url(&self, url: &str) -> anyhow::Result<reqwest::Url> {
        let parsed = reqwest::Url::parse(url).context("source is not a URL")?;
        let host = parsed.host_str().unwrap_or("").to_string();
        if !matches!(parsed.scheme(), "http" | "https") {
            anyhow::bail!("source host {host}: only http(s) sources are allowed");
        }
        if !self.allow_private_sources && !crate::http::is_safe_http_url(url) {
            anyhow::bail!(
                "source host {host}: refused by the SSRF guard (private, internal or single-label host)"
            );
        }
        Ok(parsed)
    }
}

fn network_id(eth_network: &str) -> u64 {
    match eth_network {
        "sepolia" => 11155111,
        "goerli" => 5,
        _ => 1,
    }
}

pub struct State {
    pub settings: Settings,
    pub pool: PgPool,
    pub http: crate::http::Outbound,
    /// Sync status and public URLs for the realm doc; the profiles reader's
    /// local-first path arrives with the lambdas module.
    pub content: Arc<ContentState>,
    pub registry: registry::Cache,
    pub denylist: denylist::Snapshot,
    pub flags: feature_flags::Snapshot,
    pub map: map::MapState,
    pub ab_cdn: ab_cdn::Cache,
    pub satellite: satellite::SatelliteState,
    pub world_manifest: world_manifest::WorldManifestState,
    pub posters: posters::PosterState,
    pub opensea: opensea::Runtime,
    /// The `/auth-api` sign-in relay's requests and identities (process state).
    pub auth_api: auth_api::AuthApiState,
    /// The operator's runtime key/value map (process state).
    pub runtime_config: runtime_config::RuntimeConfigState,
    /// The onboarding checkpoints (process state).
    pub onboarding: onboarding::OnboardingState,
    /// The ONE EIP-1654 validator, attached after construction (`attach_eip1654`).
    pub eip1654: std::sync::OnceLock<Arc<dyn crate::auth::eip1654::Eip1654Validator>>,
}

pub type Shared = Arc<State>;

impl State {
    /// Validates the sources, then loads what the mirrors last persisted so
    /// every route answers from the database before the first pass.
    pub async fn new(
        settings: Settings,
        pool: PgPool,
        http: crate::http::Outbound,
        content: Arc<ContentState>,
    ) -> anyhow::Result<Shared> {
        settings.validate()?;
        let app = settings.feature_flags_app.clone();
        let settings_budget = (
            settings.satellite_source_budget_bytes,
            settings.satellite_output_entries,
        );
        let ab_cdn = ab_cdn::Cache::new(
            settings.ab_cdn_cache_dir.clone(),
            settings.ab_cdn_cache_max_bytes,
        )?;
        let state = Arc::new(Self {
            settings,
            pool,
            http,
            content,
            registry: registry::Cache::new(),
            denylist: denylist::Snapshot::default(),
            flags: feature_flags::Snapshot::default(),
            map: map::MapState::new(),
            ab_cdn,
            satellite: satellite::SatelliteState::new(settings_budget.0, settings_budget.1),
            world_manifest: world_manifest::WorldManifestState::default(),
            posters: posters::PosterState::default(),
            opensea: opensea::Runtime::default(),
            auth_api: auth_api::AuthApiState::default(),
            runtime_config: runtime_config::RuntimeConfigState::default(),
            onboarding: onboarding::OnboardingState::default(),
            eip1654: std::sync::OnceLock::new(),
        });
        state
            .denylist
            .reload(&state.pool)
            .await
            .context("loading the mirrored denylist")?;
        state
            .flags
            .reload(&state.pool, &app)
            .await
            .context("loading the mirrored feature flags")?;
        state
            .map
            .load(&state.pool)
            .await
            .context("loading the mirrored tile grid")?;
        state
            .satellite
            .load(&state.pool)
            .await
            .context("loading the satellite region index")?;
        state
            .world_manifest
            .load(&state.pool, &state.content.storage)
            .await
            .context("loading the mirrored world manifest")?;
        Ok(state)
    }

    /// The four mirror loops, each a periodic job: a failed pass backs off
    /// (doubling to an hour) and the next one starts over from the persisted
    /// marker. Nothing is spawned unless `EXPLORER_MIRRORS_ENABLED` is set,
    /// like every other mirror's flag.
    pub fn spawn_jobs(self: &Arc<Self>, jobs: &Jobs) {
        auth_api::spawn_identity_sweeper(self, jobs);
        let pool = self.pool.clone();
        jobs.spawn_periodic_leased(
            "opensea-cache-expiry",
            PeriodicCfg::every(Duration::from_secs(3600)).after_first_interval(),
            move |ctx| {
                let pool = pool.clone();
                async move {
                    let expired = opensea::expire(&pool, chrono::Utc::now().naive_utc()).await?;
                    ctx.progress(format!("expired {expired} OpenSea cache rows"));
                    Ok(())
                }
            },
        );
        if self.settings.ab_cdn_enabled {
            let store = self.ab_cdn.store.clone();
            jobs.spawn_periodic(
                "ab-cdn-maintenance",
                PeriodicCfg::every(ab_cdn::MAINTENANCE_INTERVAL).after_first_interval(),
                move |_ctx| {
                    let store = store.clone();
                    async move {
                        tokio::task::spawn_blocking(move || store.maintenance_pass())
                            .await
                            .context("ab-cdn cache maintenance")?;
                        Ok(())
                    }
                },
            );
        }
        if !self.settings.mirrors_enabled {
            tracing::info!(
                "EXPLORER_MIRRORS_ENABLED=false: no explorer mirrors; dcl-lists, the denylist, the feature \
                 flags, the tile grid, the satellite regions and the world manifest stay at their last \
                 mirrored state"
            );
            return;
        }
        let cfg = |secs: u64, jitter: u64| {
            PeriodicCfg::every(Duration::from_secs(secs))
                .with_jitter(Duration::from_secs(jitter))
                .with_error_backoff(Duration::from_secs(3600))
        };
        // A blank source is no upstream: that one mirror does not run.
        let named = |key: &str, source: &str, job: &str| {
            if source.is_empty() {
                tracing::info!(
                    "EXPLORER_MIRRORS_ENABLED=true with an empty {key}: no {job}; its data stays at the \
                     last mirrored state"
                );
            }
            !source.is_empty()
        };
        let sources = &self.settings.sources;
        if named(
            "EXPLORER_LISTS_SOURCE_URL",
            &sources.lists,
            "explorer-lists-mirror",
        ) {
            let s = self.clone();
            jobs.spawn_periodic_leased("explorer-lists-mirror", cfg(900, 60), move |ctx| {
                let s = s.clone();
                async move { lists::run_pass(&s, &ctx).await }
            });
        }
        if named(
            "EXPLORER_DENYLIST_SOURCE_URL",
            &sources.denylist,
            "explorer-denylist-mirror",
        ) {
            let s = self.clone();
            jobs.spawn_periodic_leased("explorer-denylist-mirror", cfg(300, 30), move |ctx| {
                let s = s.clone();
                async move { denylist::run_pass(&s, &ctx).await }
            });
        }
        if named(
            "EXPLORER_FEATURE_FLAGS_SOURCE_URL",
            &sources.feature_flags,
            "explorer-flags-mirror",
        ) {
            let s = self.clone();
            jobs.spawn_periodic_leased("explorer-flags-mirror", cfg(300, 30), move |ctx| {
                let s = s.clone();
                async move { feature_flags::run_pass(&s, &ctx).await }
            });
        }
        if named(
            "EXPLORER_MAP_SOURCE_URL",
            &sources.map,
            "explorer-map-mirror",
        ) {
            let s = self.clone();
            jobs.spawn_periodic_leased("explorer-map-mirror", cfg(300, 30), move |ctx| {
                let s = s.clone();
                async move { map::run_pass(&s, &ctx).await }
            });
        }
        if self.settings.satellite_upstream.is_some() {
            let s = self.clone();
            jobs.spawn_periodic_leased(
                "explorer-satellite-mirror",
                cfg(self.settings.satellite_interval.as_secs().max(60), 60),
                move |ctx| {
                    let s = s.clone();
                    async move { satellite::run_pass(&s, &ctx, chrono::Utc::now().naive_utc()).await }
                },
            );
        } else {
            tracing::info!(
                "SATELLITE_MIRROR_UPSTREAM_URL unset: no satellite mirror; the satellite regions stay at \
                 their last mirrored state"
            );
        }
        if named(
            "WORLD_MANIFEST_UPSTREAM_URL",
            &self.settings.world_manifest_upstream,
            "explorer-world-manifest-mirror",
        ) {
            let s = self.clone();
            jobs.spawn_periodic_leased(
                "explorer-world-manifest-mirror",
                cfg(self.settings.world_manifest_interval.as_secs().max(60), 60),
                move |ctx| {
                    let s = s.clone();
                    async move { world_manifest::run_pass(&s, &ctx).await }
                },
            );
        }
    }
}

/// One router per upstream service, keyed by the subdomain the explorer
/// derives it from (`realm-provider-ea.<domain>`, `api.<domain>`, ...), for
/// a deployment that serves the raw-subdomain form.
pub fn service_routers(state: Shared) -> Vec<(&'static str, Router)> {
    vec![
        (
            "realm-provider-ea",
            realm_provider::routes().with_state(state.clone()),
        ),
        ("auth-api", auth_api::routes().with_state(state.clone())),
        ("onboarding", onboarding::routes().with_state(state.clone())),
        (
            "asset-bundle-registry",
            registry::routes().with_state(state.clone()),
        ),
        ("dcl-lists", lists::routes().with_state(state.clone())),
        ("config", denylist::routes().with_state(state.clone())),
        (
            "feature-flags",
            feature_flags::routes().with_state(state.clone()),
        ),
        ("api", map::routes().with_state(state.clone())),
        ("ab-cdn", ab_cdn::routes().with_state(state.clone())),
        (
            posters::HOST_PREFIX,
            posters::routes().with_state(state.clone()),
        ),
        ("opensea", opensea::routes().with_state(state)),
    ]
}

/// Every service under its gateway prefix, the form the boot corpus captures
/// (`/asset-bundle-registry/entities/active`, `/realm-provider-ea/main/about`,
/// `/api/v1/map.png`), plus `GET /mirrors`: every mirror's pass marker, and
/// the statics the main vhost served at the root: `/world-manifest.json`,
/// `/satellite/{z}/{x}/{y}` (web.nix's `/satellite/` location) and the
/// poster bucket's `/poster[-vertical]/{file}` (the vhost's own paths).
pub fn router(state: Shared) -> Router {
    let mut app = Router::new()
        .route("/mirrors", get(mirror::status))
        .route("/world-manifest.json", get(world_manifest::get))
        .route("/satellite/{z}/{x}/{y}", get(satellite::tile))
        .merge(posters::routes())
        .with_state(state.clone());
    for (prefix, service) in service_routers(state) {
        app = app.nest(&format!("/{prefix}"), service);
    }
    app.layer(CorsLayer::permissive())
}

/// The unavailable envelope (`marketplace::unavailable`'s shape) for a
/// static whose mirror has not landed a copy yet: 503 with `Retry-After`.
pub(crate) fn unavailable(source: &str, configure: &str, message: &str) -> Response {
    let body = json!({ "error": "Service Unavailable", "message": message, "source": source, "configure": configure });
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::RETRY_AFTER, "60"),
        ],
        body.to_string(),
    )
        .into_response()
}

impl State {
    /// Attaches the ONE EIP-1654 validator (contract-wallet signed fetches);
    /// `false` if one was attached already.
    pub fn attach_eip1654(
        &self,
        validator: Option<Arc<dyn crate::auth::eip1654::Eip1654Validator>>,
    ) -> bool {
        validator.is_some_and(|v| self.eip1654.set(v).is_ok())
    }
}

pub(crate) fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, axum::Json(json!({ "error": message.into() }))).into_response()
}

pub(crate) fn internal_error(context: &str, err: impl std::fmt::Display) -> Response {
    tracing::error!(%err, "{context}");
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("{context}: {err}"),
    )
}

#[cfg(test)]
pub(crate) mod tests_support {
    /// A state over a pool that never connects (the auth-api tests).
    pub async fn state_without_db() -> super::Shared {
        super::tests::node_without_db_state(super::Settings::for_tests(super::Sources::defaults()))
            .await
    }
}
