//! Content server: the read routes of the Catalyst content API over the
//! `content.*` schema and the blob store. The wire contract is the fixture set
//! under `tests/fixtures/` (copied from catalyrst-conformance), replayed by
//! `tests`.
//!
//! Everything a handler needs hangs off [`ContentState`]; the old per-concern
//! traits (`ContentStorage`, `Database`, `Denylist`, `SynchronizationState`,
//! `SnapshotGenerator`, `ChallengeSupervisor`, `AcceptingUsers`,
//! `ContentCluster`, `Deployer`) each had one implementation and are inlined
//! here as plain fields and functions.
//!
//! The local write path (`POST /entities`, `DELETE /scenes/{coord}`;
//! [`create_entity`], [`unpublish_scene`], [`land_publish`], the
//! [`write_deployer`] with the [`validator`] attached) answers behind
//! `CONTENT_WRITES_ENABLED` ([`writes`], default off: 503 with
//! `Retry-After`) and the console's read-only toggle (403, the old gate).

// The crates this one builds on, under their module names (AGENTS.md).
pub(crate) use catalyst_chain_index as chain_index;
pub(crate) use catalyst_config::config;
#[cfg(test)]
pub(crate) use catalyst_db as db;
pub(crate) use catalyst_jobs as jobs;
pub(crate) use catalyst_land_authz as land_authz;
pub(crate) use catalyst_shared as shared;
pub(crate) use catalyst_storage as storage;
#[cfg(test)]
pub(crate) use catalyst_testing as testing;
pub(crate) use dcl_auth as auth;
pub(crate) use dcl_http as http;

pub use crate::shared::cache;
mod connection;
pub use crate::shared::{cors, nul_guard, query_params};
pub mod entity;
pub mod errors;
pub mod formatters;
pub mod queries;
pub mod sync_types;
pub mod validation;
pub mod validator;
pub mod wire_types;

// The write path peer sync drives: remote entity fetch + hash proof, auth-chain
// verification, batched persist, failure records; and the local deploy function.
pub mod batch_deployer;
pub mod bloom_filter;
pub mod deploy_remote_entity;
pub mod sync_deployer;
pub mod write_deployer;

// The local write path: the multipart deploy, the LAND publish rules and the
// unpublish, the operator's durable denylist and the CONTENT_WRITES_ENABLED gate;
// the upstream forwarding of local deploys (CONTENT_FORWARD_UPSTREAM_URL).
pub mod create_entity;
pub mod denylist;
pub mod forward;
pub mod land_publish;
#[cfg(test)]
pub(crate) mod sync_test_support;
pub mod unpublish_scene;
pub mod writes;

// Peer sync: snapshots, pointer changes, cursors, retries and the orchestrator.
pub mod sync;

// The other direction: the time-windowed snapshots peers sync from this node.
pub mod snapshot_gen;
pub mod time_range;

// Read-through: a local miss filled from the sync sources, verified, stored as synced
// (READ_THROUGH_ENABLED).
pub mod read_through;

// A sync-only node's history prune: the superseded deployments, their
// content_files and the blobs nothing else names (CONTENT_HISTORY_PRUNE_*).
pub mod history_prune;

mod about;
pub mod about_comms;
mod active_entities;
mod audit;
pub mod available_content;
mod deployments;
pub mod entity_thumbnail;
mod failed_deployments;
pub mod get_content;
mod pointer_changes;
mod snapshots;
mod status;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod write_path_tests;

pub use status::challenge_text;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use bytes::Bytes;
use parking_lot::RwLock;
use sqlx::PgPool;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultOnFailure, TraceLayer};

use crate::shared::cache::TtlMap;
pub use crate::storage::Denylist;
use crate::storage::Storage;

pub const DEPLOYMENTS_CACHE_TTL: Duration = Duration::from_secs(5);
pub const DEPLOYMENTS_CACHE_MAX_ENTRIES: usize = 1000;

/// The subset of [`crate::config::Config`] the content routes read, with the
/// derived defaults (public URLs, timeouts) resolved once at startup.
#[derive(Clone, Debug)]
pub struct Settings {
    pub version: String,
    pub commit_hash: Option<String>,
    pub eth_network: String,
    pub content_public_url: String,
    pub lambdas_public_url: String,
    /// This node's `/profile-images/`, which serves an entity's own snapshot
    /// file, a cached render or a fresh one: where avatar snapshots point when
    /// the profile carries no snapshot hashes and no CDN is configured.
    pub profile_images_public_url: String,
    pub realm_name: Option<String>,
    pub entities_cache_control_max_age: u64,
    /// nginx `X-Accel-Redirect` prefix for GET /contents; `None` streams the blob itself.
    pub storage_x_accel_base: Option<String>,
    /// `MAP_SATELLITE_BASE_URL`; unset, /about advertises no satellite view.
    pub map_satellite_base_url: Option<String>,
    pub map_parcel_view_url: String,
    /// The parcel view is this node's own minimap on an island or journey
    /// node: /about advertises it only while the tile grid it draws from is
    /// loaded ([`ContentState::attach_parcel_grid`]).
    pub map_parcel_view_needs_grid: bool,
    pub deployments_query_timeout: Duration,
    pub request_timeout: Option<Duration>,
    /// `CATALYRST_CONTENT_ADMIN_TOKEN`: the bearer of the operator routes
    /// (`/forwarding`); unset refuses them.
    pub admin_token: AdminToken,
}

/// An operator bearer whose `Debug` never prints it; empty is unset.
#[derive(Clone, Default)]
pub struct AdminToken(Option<String>);

impl AdminToken {
    pub fn new(token: Option<String>) -> Self {
        Self(token.filter(|t| !t.trim().is_empty()))
    }

    pub fn get(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

impl std::fmt::Debug for AdminToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "<set>" } else { "<unset>" })
    }
}

fn non_empty(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// The hosts this node answers at (PUBLIC_URL, CONTENT_URL, LAMBDAS_URL, the
/// bind address), lowercased: an upstream or mirror source naming one of
/// them is the node reading its own copy back, refused at startup.
pub fn own_hosts(config: &crate::config::Config) -> Vec<String> {
    let content = Settings::from_config(config);
    [
        config.node.public_url.clone(),
        Some(content.content_public_url),
        Some(content.lambdas_public_url),
        Some(format!("http://{}", config.node.bind_addr())),
    ]
    .into_iter()
    .flatten()
    .filter_map(|u| url::Url::parse(u.trim()).ok())
    .filter_map(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
    .collect()
}

impl Settings {
    pub fn from_config(config: &crate::config::Config) -> Self {
        let public_url = config.node.own_url();
        let map = config.map_urls();
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit_hash: config.commit_hash(),
            eth_network: config.node.eth_network.clone(),
            content_public_url: non_empty(&config.node.content_url)
                .unwrap_or_else(|| format!("{public_url}/content/")),
            lambdas_public_url: non_empty(&config.node.lambdas_url)
                .unwrap_or_else(|| format!("{public_url}/lambdas/")),
            profile_images_public_url: format!("{public_url}/profile-images/"),
            realm_name: non_empty(&config.node.realm_name),
            entities_cache_control_max_age: config.node.entities_cache_control_max_age,
            storage_x_accel_base: non_empty(&config.node.storage_x_accel_base),
            map_satellite_base_url: map.satellite_base,
            map_parcel_view_url: map.parcel_view,
            map_parcel_view_needs_grid: config.node.map_parcel_view_url.trim().is_empty()
                && matches!(
                    config.node.node_posture.as_deref().map(str::trim),
                    Some("island" | "journey")
                ),
            deployments_query_timeout: Duration::from_secs(
                config.node.deployments_query_timeout_secs,
            ),
            request_timeout: match config.node.request_timeout_secs {
                0 => None,
                n => Some(Duration::from_secs(n)),
            },
            admin_token: AdminToken::new(config.writes.catalyrst_content_admin_token.clone()),
        }
    }

    /// The parcel view to advertise given whether the tile grid is loaded.
    pub fn parcel_view_url(&self, grid_loaded: bool) -> Option<&str> {
        (grid_loaded || !self.map_parcel_view_needs_grid)
            .then_some(self.map_parcel_view_url.as_str())
    }

    pub fn for_tests() -> Self {
        Self {
            version: "test".to_string(),
            commit_hash: None,
            eth_network: "mainnet".to_string(),
            content_public_url: "http://localhost/content/".to_string(),
            lambdas_public_url: "http://localhost/lambdas/".to_string(),
            profile_images_public_url: "http://localhost/profile-images/".to_string(),
            realm_name: None,
            entities_cache_control_max_age: 10,
            storage_x_accel_base: None,
            map_satellite_base_url: Some("http://127.0.0.1:5162/satellite".to_string()),
            map_parcel_view_url: "http://127.0.0.1:5162/v1/minimap.png".to_string(),
            map_parcel_view_needs_grid: false,
            deployments_query_timeout: Duration::from_secs(10),
            request_timeout: None,
            admin_token: AdminToken::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncState {
    Bootstrapping,
    PartiallySynced { ready_types: Vec<String> },
    Syncing,
    Failed,
}

impl SyncState {
    /// The `content.system_properties` value the sync leader publishes
    /// (`sync::status_mirror`): the wire name, plus the ready types of a
    /// partial sync after a colon.
    pub fn to_property(&self) -> String {
        match self {
            SyncState::Bootstrapping => "Bootstrapping".to_string(),
            SyncState::Syncing => "Syncing".to_string(),
            SyncState::Failed => "Failed".to_string(),
            SyncState::PartiallySynced { ready_types } => {
                format!("PartiallySynced:{}", ready_types.join(","))
            }
        }
    }

    /// `None` for a value no binary of this schema writes.
    pub fn from_property(value: &str) -> Option<Self> {
        match value {
            "Bootstrapping" => Some(SyncState::Bootstrapping),
            "Syncing" => Some(SyncState::Syncing),
            "Failed" => Some(SyncState::Failed),
            other => {
                let types = other.strip_prefix("PartiallySynced:")?;
                Some(SyncState::PartiallySynced {
                    ready_types: types
                        .split(',')
                        .filter(|t| !t.is_empty())
                        .map(str::to_string)
                        .collect(),
                })
            }
        }
    }
}

/// What the sync orchestrator ([`sync::orchestrator`]) reports and the status
/// routes read; one shared `Arc` per process. A node with no sync attached is
/// `Bootstrapping`: it has nothing and claims nothing, until the process
/// mirrors what the sync leader of its fleet published
/// ([`sync::status_mirror`]). The process running the orchestrator (or one
/// serving unsynced) holds the authority: its own state is the truth and the
/// mirror leaves it alone.
pub struct SyncStatus {
    state: RwLock<SyncState>,
    frontier_ms: AtomicI64,
    heartbeat_ms: AtomicI64,
    authority: AtomicBool,
    unsynced_copy: AtomicBool,
    /// `SERVE_UNSYNCED_DEPLOY_TYPES`: what an unsynced copy still takes.
    copy_deploy_types: RwLock<Vec<String>>,
}

impl Default for SyncStatus {
    fn default() -> Self {
        Self {
            state: RwLock::new(SyncState::Bootstrapping),
            frontier_ms: AtomicI64::new(-1),
            heartbeat_ms: AtomicI64::new(-1),
            authority: AtomicBool::new(false),
            unsynced_copy: AtomicBool::new(false),
            copy_deploy_types: RwLock::new(Vec::new()),
        }
    }
}

impl SyncStatus {
    /// True while this process owns the state: it runs the sync orchestrator
    /// or serves unsynced. False: the state is mirrored from the database.
    pub fn is_authority(&self) -> bool {
        self.authority.load(Ordering::Relaxed)
    }

    pub fn set_authority(&self, authority: bool) {
        self.authority.store(authority, Ordering::Relaxed);
    }

    pub fn state(&self) -> SyncState {
        self.state.read().clone()
    }

    pub fn set_state(&self, state: SyncState) {
        *self.state.write() = state;
    }

    /// The wire name; a partially synced node already answers as `Syncing`.
    pub fn state_name(&self) -> &'static str {
        match &*self.state.read() {
            SyncState::Bootstrapping => "Bootstrapping",
            SyncState::PartiallySynced { .. } | SyncState::Syncing => "Syncing",
            SyncState::Failed => "Failed",
        }
    }

    /// Set when the process serves unsynced while `SYNC_SOURCE` names a peer
    /// ([`sync::worker::serve_unsynced`]): the forged state is not this node's.
    pub fn set_unsynced_copy(&self, copy: bool) {
        self.unsynced_copy.store(copy, Ordering::Relaxed);
    }

    /// The entity types an unsynced copy still takes deploys of
    /// (`SERVE_UNSYNCED_DEPLOY_TYPES`, development only).
    pub fn set_copy_deploy_types(&self, types: Vec<String>) {
        *self.copy_deploy_types.write() = types;
    }

    /// False while a deploy has to wait: the node is bootstrapping, or it
    /// serves an unsynced copy of a peer.
    pub fn takes_deploys(&self) -> bool {
        self.state_name() != "Bootstrapping" && !self.unsynced_copy.load(Ordering::Relaxed)
    }

    /// Whether a deploy of `entity_type` is taken: any while
    /// [`Self::takes_deploys`], and on an unsynced copy the types it names.
    pub fn takes_deploys_of(&self, entity_type: &str) -> bool {
        self.takes_deploys()
            || (self.state_name() != "Bootstrapping"
                && self
                    .copy_deploy_types
                    .read()
                    .iter()
                    .any(|t| t == entity_type))
    }

    /// Whether some deploy may be taken: the gate before the form is read.
    pub fn takes_some_deploys(&self) -> bool {
        self.takes_deploys()
            || (self.state_name() != "Bootstrapping" && !self.copy_deploy_types.read().is_empty())
    }

    pub fn is_type_ready(&self, entity_type: &str) -> bool {
        match &*self.state.read() {
            SyncState::Syncing => true,
            SyncState::PartiallySynced { ready_types } => {
                ready_types.iter().any(|t| t == entity_type)
            }
            SyncState::Bootstrapping | SyncState::Failed => false,
        }
    }

    pub fn sync_frontier_ms(&self) -> Option<i64> {
        Some(self.frontier_ms.load(Ordering::Relaxed)).filter(|v| *v >= 0)
    }

    pub fn set_sync_frontier_ms(&self, ms: i64) {
        self.frontier_ms.store(ms, Ordering::Relaxed);
    }

    /// GREATEST-monotonic, like the persisted frontier it mirrors.
    pub fn advance_sync_frontier_ms(&self, ms: i64) {
        self.frontier_ms.fetch_max(ms, Ordering::Relaxed);
    }

    pub fn heartbeat_ms(&self) -> Option<i64> {
        Some(self.heartbeat_ms.load(Ordering::Relaxed)).filter(|v| *v >= 0)
    }

    pub fn beat(&self) {
        self.set_heartbeat_ms(chrono::Utc::now().timestamp_millis());
    }

    /// The instant the sync worker persisted as its heartbeat.
    pub fn set_heartbeat_ms(&self, ms: i64) {
        self.heartbeat_ms.store(ms, Ordering::Relaxed);
    }
}

/// The local write path as `serve` attaches it ([`ContentState::attach_writes`]).
pub struct Writes {
    /// `CONTENT_WRITES_ENABLED`.
    pub enabled: bool,
    pub deployer: Arc<write_deployer::WriteDeployer>,
}

pub struct ContentState {
    pub pool: PgPool,
    pub settings: Settings,
    pub storage: Arc<Storage>,
    pub sync: Arc<SyncStatus>,
    pub denylist: Arc<Denylist>,
    /// Memo for the two polled history routes (deployments, pointer changes)
    /// and the snapshot list; cleared when a deployment lands or a local
    /// scene is unpublished.
    pub deployments_cache: TtlMap<String, Bytes>,
    /// The console's read-only toggle (process state, off at start as the
    /// old `READ_ONLY` default was): while set, `POST /entities` and
    /// `DELETE /scenes/{coord}` answer the old 403.
    pub read_only: AtomicBool,
    /// The console's accepting-users toggle (process state, on at start):
    /// `/about` reports `acceptingUsers` while the node is healthy and this
    /// is set.
    pub accepting_users: AtomicBool,
    /// The write path; unattached (a node without it, the tests of the read
    /// routes) every write answers the `CONTENT_WRITES_ENABLED` 503.
    writes: OnceLock<Writes>,
    /// The `/about` `comms` block (`COMMS_FIXED_ADAPTER`); unattached, the
    /// block is left out, as the old server without its probe URLs did.
    about_comms: OnceLock<Arc<about_comms::AboutCommsState>>,
    /// `READ_THROUGH_ENABLED`; unattached, a miss is a miss.
    read_through: OnceLock<Arc<read_through::ReadThrough>>,
    /// Whether the explorer's tile grid is loaded (the minimap draws from it).
    parcel_grid: OnceLock<GridReady>,
}

/// See [`ContentState::attach_parcel_grid`].
pub type GridReady = Arc<dyn Fn() -> bool + Send + Sync>;

pub type SharedState = Arc<ContentState>;

impl ContentState {
    pub fn new(pool: PgPool, settings: Settings, storage: Arc<Storage>) -> Self {
        Self::with_sync(pool, settings, storage, Arc::new(SyncStatus::default()))
    }

    /// `sync` is the handle the orchestrator writes to; the status routes read it.
    pub fn with_sync(
        pool: PgPool,
        settings: Settings,
        storage: Arc<Storage>,
        sync: Arc<SyncStatus>,
    ) -> Self {
        let denylist = storage.denylist().clone();
        Self {
            pool,
            settings,
            storage,
            sync,
            denylist,
            deployments_cache: TtlMap::bounded(
                DEPLOYMENTS_CACHE_TTL,
                DEPLOYMENTS_CACHE_MAX_ENTRIES,
            ),
            read_only: AtomicBool::new(false),
            accepting_users: AtomicBool::new(true),
            writes: OnceLock::new(),
            about_comms: OnceLock::new(),
            read_through: OnceLock::new(),
            parcel_grid: OnceLock::new(),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// Answers the value now in force.
    pub fn set_read_only(&self, read_only: bool) -> bool {
        self.read_only.store(read_only, Ordering::Relaxed);
        read_only
    }

    pub fn is_accepting_users(&self) -> bool {
        self.accepting_users.load(Ordering::Relaxed)
    }

    pub fn set_accepting_users(&self, accepting: bool) -> bool {
        self.accepting_users.store(accepting, Ordering::Relaxed);
        accepting
    }

    /// Attaches the write path once; `false` if one was attached already.
    pub fn attach_writes(
        &self,
        enabled: bool,
        deployer: Arc<write_deployer::WriteDeployer>,
    ) -> bool {
        self.writes.set(Writes { enabled, deployer }).is_ok()
    }

    /// `CONTENT_WRITES_ENABLED` as attached; `false` without a write path.
    pub fn writes_enabled(&self) -> bool {
        self.writes.get().is_some_and(|w| w.enabled)
    }

    /// Attaches the `/about` comms block once; `false` if one was attached already.
    pub fn attach_about_comms(&self, about: Arc<about_comms::AboutCommsState>) -> bool {
        self.about_comms.set(about).is_ok()
    }

    pub fn about_comms(&self) -> Option<&Arc<about_comms::AboutCommsState>> {
        self.about_comms.get()
    }

    /// Attaches the read-through once; `false` if one was attached already.
    pub fn attach_read_through(&self, read_through: Arc<read_through::ReadThrough>) -> bool {
        self.read_through.set(read_through).is_ok()
    }

    pub fn read_through(&self) -> Option<&Arc<read_through::ReadThrough>> {
        self.read_through.get()
    }

    /// Attaches the tile grid's readiness once; `false` if one was attached already.
    pub fn attach_parcel_grid(&self, ready: GridReady) -> bool {
        self.parcel_grid.set(ready).is_ok()
    }

    /// The parcel view /about and /main/about advertise: the configured one,
    /// or this node's own minimap while it has a grid to draw (an island
    /// without one answers it 503, so the clients are not sent there).
    pub fn parcel_view_url(&self) -> Option<&str> {
        self.settings
            .parcel_view_url(self.parcel_grid.get().is_some_and(|ready| ready()))
    }

    /// The deployer, only while writes are enabled.
    pub fn deployer(&self) -> Option<&Arc<write_deployer::WriteDeployer>> {
        self.writes.get().filter(|w| w.enabled).map(|w| &w.deployer)
    }

    /// The upstream forwarder (`CONTENT_FORWARD_UPSTREAM_URL`), writes enabled
    /// or not: its worker drains what earlier deploys queued either way.
    pub fn forwarder(&self) -> Option<&Arc<forward::Forwarder>> {
        self.writes.get().and_then(|w| w.deployer.forwarder())
    }
}

/// `GET /scenes`: the worlds-content-server listing shape; a catalyst node
/// has no world scenes to list, so the engine's realm probe reads it empty.
async fn empty_scenes() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "scenes": [], "total": 0 }))
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not found")
}

fn content_routes() -> Router<SharedState> {
    Router::new()
        .route("/challenge", get(status::get_challenge))
        .route("/entities/{type}", get(active_entities::get_entities))
        .route(
            "/entities/active/collections/{collectionUrn}",
            get(active_entities::get_entities_by_collection),
        )
        .route(
            "/entities/active",
            post(active_entities::get_active_entities).layer(DefaultBodyLimit::max(256 * 1024)),
        )
        .route(
            "/contents/{hashId}",
            get(get_content::get_content).head(get_content::get_content),
        )
        .route(
            "/contents/{hashId}/active-entities",
            get(active_entities::get_active_entities_by_hash),
        )
        .route(
            "/available-content",
            get(available_content::get_available_content),
        )
        .route("/audit/{type}/{entityId}", get(audit::get_audit))
        .route("/deployments", get(deployments::get_deployments))
        .route(
            "/failed-deployments",
            get(failed_deployments::get_failed_deployments),
        )
        .route(
            "/pointer-changes",
            get(pointer_changes::get_pointer_changes),
        )
        .route("/snapshots", get(snapshots::get_snapshots))
        .route("/snapshot", get(snapshots::get_snapshot))
        .route("/snapshot/{type}", get(snapshots::get_snapshot_by_type))
        .route("/status", get(status::get_status))
        .route(
            "/queries/items/{pointer}/thumbnail",
            get(entity_thumbnail::get_entity_thumbnail)
                .head(entity_thumbnail::get_entity_thumbnail),
        )
        .route(
            "/queries/items/{pointer}/image",
            get(entity_thumbnail::get_entity_image).head(entity_thumbnail::get_entity_image),
        )
        .route(
            "/queries/erc721/{chainId}/{contract}/{option}",
            get(validator::erc721::get_erc721_entity),
        )
        .route(
            "/queries/erc721/{chainId}/{contract}/{option}/{emission}",
            get(validator::erc721::get_erc721_entity),
        )
        .route(
            "/entities",
            post(create_entity::create_entity)
                .layer(DefaultBodyLimit::max(create_entity::MAX_DEPLOYMENT_BYTES)),
        )
        .route("/scenes/{coord}", delete(unpublish_scene::unpublish_scene))
        .route("/forwarding", get(forward::list_forwards))
        .route("/forwarding/{entityId}", get(forward::get_forward))
        .route("/forwarding/{entityId}/retry", post(forward::retry_forward))
}

/// The same routes at the root and under `/content`, as the old server served
/// them (the reverse proxy strips nothing).
pub fn build_router(state: SharedState) -> Router {
    let request_timeout = state.settings.request_timeout;
    let mut app = Router::new()
        .route("/about", get(about::get_about))
        .route("/lambdas/explore/realms", get(about::get_explore_realms))
        .route("/scenes", get(empty_scenes))
        .route(
            "/lambdas/collections/contents/{pointer}/thumbnail",
            get(entity_thumbnail::get_entity_thumbnail)
                .head(entity_thumbnail::get_entity_thumbnail),
        )
        .route(
            "/lambdas/collections/contents/{pointer}/image",
            get(entity_thumbnail::get_entity_image).head(entity_thumbnail::get_entity_image),
        )
        .merge(content_routes())
        .nest("/content", content_routes())
        .fallback(not_found)
        .layer(
            TraceLayer::new_for_http()
                .on_failure(DefaultOnFailure::new().level(tracing::Level::DEBUG)),
        )
        .layer(axum::middleware::from_fn(nul_guard::nul_guard_middleware))
        .layer(axum::middleware::from_fn(cors::cors_middleware))
        .layer(axum::middleware::from_fn(connection::keep_alive_hint));
    if let Some(timeout) = request_timeout {
        app = app.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ));
    }
    app.with_state(state)
}

#[cfg(test)]
mod settings_tests {
    use super::Settings;
    use crate::config::Config;

    #[test]
    fn runtime_commit_hash_overrides_the_build_time_one() {
        let config = Config::parse_for_tests(&[
            "x",
            "--database-url",
            "postgres:///c",
            "--commit-hash",
            " abc ",
        ])
        .unwrap();
        assert_eq!(
            Settings::from_config(&config).commit_hash.as_deref(),
            Some("abc")
        );

        let config =
            Config::parse_for_tests(&["x", "--database-url", "postgres:///c", "--commit-hash", ""])
                .unwrap();
        let build_time = option_env!("CATALYST_COMMIT_HASH").map(str::to_string);
        assert_eq!(Settings::from_config(&config).commit_hash, build_time);
    }
}

#[cfg(test)]
mod sync_status_tests {
    use super::{SyncState, SyncStatus};

    #[test]
    fn a_node_without_a_worker_is_bootstrapping_and_nothing_is_ready() {
        let sync = SyncStatus::default();
        assert_eq!(sync.state(), SyncState::Bootstrapping);
        assert_eq!(sync.state_name(), "Bootstrapping");
        assert!(!sync.is_type_ready("scene"));
        assert!(sync.sync_frontier_ms().is_none());
        assert!(sync.heartbeat_ms().is_none());
    }

    #[test]
    fn partially_synced_answers_syncing_but_gates_per_type() {
        let sync = SyncStatus::default();
        sync.set_state(SyncState::PartiallySynced {
            ready_types: vec!["profile".into()],
        });
        assert_eq!(sync.state_name(), "Syncing");
        assert!(sync.is_type_ready("profile"));
        assert!(!sync.is_type_ready("scene"));
        sync.set_state(SyncState::Syncing);
        assert!(sync.is_type_ready("scene"));
    }
}
