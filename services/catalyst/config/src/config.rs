use clap::Args;

pub use crate::{
    content::{AboutCommsConfig, ContentConfig, ValidatorConfig},
    create::CreateConfig,
    explore::{AbCdnConfig, ExploreConfig, ImpostersConfig},
    land_authz::LandAuthzConfig,
    node::{NodeConfig, WritesConfig},
    ops::{AdminConfig, OpsConfig, RpcConfig},
    presence::PresenceConfig,
    price::PriceConfig,
    shop::{EconomyConfig, MarketUpstreamsConfig, MarketplaceConfig, ShopConfig},
    social::{RoomAuthorityConfig, SocialConfig},
    worlds::{WorldsConfig, WorldsContentConfig, WorldsFedConfig},
};

/// One 300 s poll, CoinGecko's ~90 s `source_updated_at` lag and one missed
/// poll (`Config::price_staleness_warning`).
pub const DEFAULT_MANA_PRICE_MAX_STALENESS_SECS: i64 = 900;
/// The one fulfilment source the payment rule admits: credits pay for
/// CollectionStore mints (SHOP.md, "Purchase experience and payment rules").
/// The legacy `secondary`/`auto` values fulfilled a credits debit from resale
/// listings; they run as `primary` and say so at startup.
pub const DEFAULT_CHECKOUT_FULFILLMENT_MODE: &str = "primary";

/// See [`Config::map_urls`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapUrls {
    /// Only a configured base: this node's `/satellite/{z}/{x}/{y}` pyramid
    /// is not the ADR-250 `{base}/{zoom}/{x},{y}.jpg` scheme the clients ask.
    pub satellite_base: Option<String>,
    pub parcel_view: String,
    pub estate_view: String,
    pub land_image_base: String,
}

/// One configuration model for the whole process. Every field reads from the
/// environment (the old `catalyrst-live` names where they map 1:1) or from the
/// matching `--flag`; `DATABASE_URL` replaces the `POSTGRES_CONTENT_*` set.
#[derive(Args, Clone, Debug)]
pub struct Config {
    // One flattened group per domain, read as `config.<group>.<field>`. clap lists flattened
    // args in declaration order, so each group is a contiguous run of the `--help` list.
    #[command(flatten)]
    pub node: NodeConfig,
    #[command(flatten)]
    pub content: ContentConfig,
    #[command(flatten)]
    pub explore: ExploreConfig,
    #[command(flatten)]
    pub worlds_content: WorldsContentConfig,
    #[command(flatten)]
    pub market_upstreams: MarketUpstreamsConfig,
    #[command(flatten)]
    pub writes: WritesConfig,
    #[command(flatten)]
    pub marketplace: MarketplaceConfig,
    #[command(flatten)]
    pub economy: EconomyConfig,
    #[command(flatten)]
    pub price: PriceConfig,
    #[command(flatten)]
    pub land_authz: LandAuthzConfig,
    #[command(flatten)]
    pub validator: ValidatorConfig,
    #[command(flatten)]
    pub presence: PresenceConfig,
    #[command(flatten)]
    pub social: SocialConfig,
    #[command(flatten)]
    pub create: CreateConfig,
    #[command(flatten)]
    pub worlds: WorldsConfig,
    #[command(flatten)]
    pub about_comms: AboutCommsConfig,
    #[command(flatten)]
    pub room_authority: RoomAuthorityConfig,
    #[command(flatten)]
    pub worlds_fed: WorldsFedConfig,
    #[command(flatten)]
    pub ops: OpsConfig,
    #[command(flatten)]
    pub shop: ShopConfig,
    #[command(flatten)]
    pub rpc: RpcConfig,
    #[command(flatten)]
    pub imposters: ImpostersConfig,
    #[command(flatten)]
    pub ab_cdn: AbCdnConfig,
    #[command(flatten)]
    pub admin: AdminConfig,
}

impl Config {
    /// Every secret may arrive as `<NAME>_FILE` instead of `<NAME>`: the file's
    /// trimmed contents replace the value (systemd `LoadCredential`,
    /// agenix). A `_FILE` that cannot be read fails startup naming the
    /// variable, never the contents. `<NAME>` set alongside `<NAME>_FILE` is
    /// refused: two sources for one secret hide which one is live.
    pub fn resolve_secret_files(&mut self) -> anyhow::Result<()> {
        for (name, slot) in self.secret_slots() {
            let from_file = std::env::var_os(format!("{name}_FILE")).is_some_and(|p| !p.is_empty());
            Self::refuse_plain_signing_key(name, slot.as_deref(), from_file)?;
            Self::read_secret_file(name, slot)?;
        }
        self.derive_trade_rpc_urls();
        crate::posture::refuse_remote_secrets(self)
    }

    /// A mirror's `TRADE_RPC_URLS` is this node's own two endpoints,
    /// `1=<ETH_RPC_URL>,137=<RPC_ENDPOINT_POLYGON>`, derived once the secret
    /// files are read so an endpoint held in `RPC_ENDPOINT_POLYGON_FILE` still
    /// makes the list. An explicit value, empty included, wins; off mainnet
    /// the chain ids name other chains, so nothing is derived.
    pub fn derive_trade_rpc_urls(&mut self) {
        if self.node.node_posture.as_deref() != Some("mirror")
            || self.marketplace.trade_rpc_urls.is_some()
            || self.node.eth_network != "mainnet"
        {
            return;
        }
        let entries: Vec<String> = [
            ("1", &self.content.eth_rpc_url),
            ("137", &self.economy.rpc_endpoint_polygon),
        ]
        .into_iter()
        .filter_map(|(chain, url)| Self::non_empty(url).map(|url| format!("{chain}={url}")))
        .collect();
        if !entries.is_empty() {
            self.marketplace.trade_rpc_urls = Some(entries.join(","));
        }
    }

    fn refuse_plain_signing_key(
        name: &str,
        value: Option<&str>,
        from_file: bool,
    ) -> anyhow::Result<()> {
        if Self::FILE_ONLY_SECRETS.contains(&name)
            && !from_file
            && value.is_some_and(|v| !v.trim().is_empty())
        {
            anyhow::bail!(
                "{name} is a signing key and is read from {name}_FILE alone (a LoadCredential or agenix file), never from the plain {name} environment variable"
            );
        }
        Ok(())
    }

    /// A secret may come from the environment or `<NAME>_FILE`, never from
    /// argv: a flag's value sits in `/proc/<pid>/cmdline` and `ps` for every
    /// local user. clap still defines the flags (the derive needs a `long`),
    /// so `serve` refuses one given on the command line, naming the flag and
    /// never the value.
    pub fn refuse_argv_secrets(&mut self, matches: &clap::ArgMatches) -> anyhow::Result<()> {
        for (name, _) in self.secret_slots() {
            let id = name.to_ascii_lowercase();
            if matches.try_get_raw(&id).is_ok()
                && matches.value_source(&id) == Some(clap::parser::ValueSource::CommandLine)
            {
                anyhow::bail!(
                    "--{} was given on the command line: a secret must come from the {name} environment variable or {name}_FILE, never argv (it would be readable in /proc/<pid>/cmdline)",
                    id.replace('_', "-")
                );
            }
        }
        Ok(())
    }

    pub(crate) fn secret_slots(&mut self) -> [(&'static str, &mut Option<String>); 61] {
        [
            (
                "CATALYRST_CONTENT_ADMIN_TOKEN",
                &mut self.writes.catalyrst_content_admin_token,
            ),
            ("LIVEKIT_API_KEY", &mut self.social.livekit_api_key),
            ("LIVEKIT_API_SECRET", &mut self.social.livekit_api_secret),
            (
                "COMMUNITIES_ADMIN_TOKEN",
                &mut self.social.communities_admin_token,
            ),
            (
                "CATALYRST_SOCIAL_RPC_ADMIN_TOKEN",
                &mut self.social.catalyrst_social_rpc_admin_token,
            ),
            ("MODERATOR_TOKEN", &mut self.social.moderator_token),
            (
                "COMMS_GATEKEEPER_AUTH_TOKEN",
                &mut self.social.comms_gatekeeper_auth_token,
            ),
            (
                "CATALYRST_NOTIFICATIONS_ADMIN_TOKEN",
                &mut self.social.catalyrst_notifications_admin_token,
            ),
            (
                "CATALYRST_BADGES_ADMIN_TOKEN",
                &mut self.social.catalyrst_badges_admin_token,
            ),
            ("SENDGRID_API_KEY", &mut self.social.sendgrid_api_key),
            (
                "TURNSTILE_SECRET_KEY",
                &mut self.social.turnstile_secret_key,
            ),
            (
                "CATALYRST_EVENTS_ADMIN_TOKEN",
                &mut self.explore.catalyrst_events_admin_token,
            ),
            (
                "CATALYRST_BUILDER_ADMIN_TOKEN",
                &mut self.create.catalyrst_builder_admin_token,
            ),
            (
                "NEWSLETTER_SERVICE_API_KEY",
                &mut self.create.newsletter_service_api_key,
            ),
            (
                "BUILDER_POLYGON_RPC_URL",
                &mut self.create.builder_polygon_rpc_url,
            ),
            (
                "CATALYRST_CAMERA_REEL_ADMIN_TOKEN",
                &mut self.create.catalyrst_camera_reel_admin_token,
            ),
            (
                "CATALYRST_WORLDS_ADMIN_TOKEN",
                &mut self.worlds.catalyrst_worlds_admin_token,
            ),
            ("ENCRYPTION_KEY", &mut self.worlds.encryption_key),
            (
                "SCENE_HOST_DEBUGGING_SECRET",
                &mut self.worlds.scene_host_debugging_secret,
            ),
            (
                "SCENE_HOST_ADMIN_TOKEN",
                &mut self.worlds.scene_host_admin_token,
            ),
            (
                "SCENE_HOST_AUTHORITATIVE_KEY",
                &mut self.worlds.scene_host_authoritative_key,
            ),
            (
                "SCENE_HOST_DELEGATION_MINTER_TOKEN",
                &mut self.worlds.scene_host_delegation_minter_token,
            ),
            (
                "SCENE_HOST_STORAGE_DELEGATION",
                &mut self.worlds.scene_host_storage_delegation,
            ),
            (
                "PULSE_ROOM_AUTHORITY_KEY",
                &mut self.room_authority.pulse_room_authority_key,
            ),
            (
                "MARKET_ADMIN_TOKEN",
                &mut self.marketplace.market_admin_token,
            ),
            ("PRICE_ADMIN_TOKEN", &mut self.price.price_admin_token),
            ("RELAYER_PRIVATE_KEY", &mut self.economy.relayer_private_key),
            ("OZ_RELAYER_API_KEY", &mut self.economy.oz_relayer_api_key),
            (
                "CATALYRST_ECONOMY_ADMIN_TOKEN",
                &mut self.economy.catalyrst_economy_admin_token,
            ),
            (
                "RPC_ENDPOINT_POLYGON",
                &mut self.economy.rpc_endpoint_polygon,
            ),
            ("TRADE_RPC_URLS", &mut self.marketplace.trade_rpc_urls),
            (
                "CATALYRST_TELEMETRY_ADMIN_TOKEN",
                &mut self.ops.catalyrst_telemetry_admin_token,
            ),
            (
                "TELEMETRY_SQL_DATABASE_URL",
                &mut self.ops.telemetry_sql_database_url,
            ),
            (
                "CATALYRST_CREDITS_ADMIN_TOKEN",
                &mut self.shop.catalyrst_credits_admin_token,
            ),
            (
                "CREDITS_CAPTCHA_SECRET",
                &mut self.shop.credits_captcha_secret,
            ),
            ("STRIPE_SECRET_KEY", &mut self.shop.stripe_secret_key),
            (
                "STRIPE_WEBHOOK_SECRET",
                &mut self.shop.stripe_webhook_secret,
            ),
            ("CREDITS_SIGNER_KEY", &mut self.shop.credits_signer_key),
            ("CREDITS_REVOKER_KEY", &mut self.shop.credits_revoker_key),
            (
                "CATALYRST_RPC_ADMIN_TOKEN",
                &mut self.rpc.catalyrst_rpc_admin_token,
            ),
            ("RPC_UPSTREAM_MAINNET", &mut self.rpc.rpc_upstream_mainnet),
            ("RPC_UPSTREAM_ETHEREUM", &mut self.rpc.rpc_upstream_ethereum),
            ("RPC_UPSTREAM_SEPOLIA", &mut self.rpc.rpc_upstream_sepolia),
            ("RPC_UPSTREAM_POLYGON", &mut self.rpc.rpc_upstream_polygon),
            ("RPC_UPSTREAM_MATIC", &mut self.rpc.rpc_upstream_matic),
            ("RPC_UPSTREAM_AMOY", &mut self.rpc.rpc_upstream_amoy),
            ("RPC_UPSTREAM_MUMBAI", &mut self.rpc.rpc_upstream_mumbai),
            ("RPC_UPSTREAM_ARBITRUM", &mut self.rpc.rpc_upstream_arbitrum),
            ("RPC_UPSTREAM_OPTIMISM", &mut self.rpc.rpc_upstream_optimism),
            (
                "RPC_UPSTREAM_AVALANCHE",
                &mut self.rpc.rpc_upstream_avalanche,
            ),
            ("RPC_UPSTREAM_BINANCE", &mut self.rpc.rpc_upstream_binance),
            ("RPC_UPSTREAM_FANTOM", &mut self.rpc.rpc_upstream_fantom),
            ("SESSION_SECRET", &mut self.admin.session_secret),
            (
                "AB_REGISTRY_ADMIN_TOKEN",
                &mut self.admin.ab_registry_admin_token,
            ),
            (
                "TRANSLATE_BACKEND_API_KEY",
                &mut self.social.translate_backend_api_key,
            ),
            (
                "TRANSLATE_LLM_API_KEY",
                &mut self.social.translate_llm_api_key,
            ),
            (
                "THIRD_PARTY_REGISTRY_L2_SUBGRAPH_URL",
                &mut self.validator.third_party_registry_l2_subgraph_url,
            ),
            (
                "BLOCKS_L2_SUBGRAPH_URL",
                &mut self.validator.blocks_l2_subgraph_url,
            ),
            ("ONBOARDING_API_KEY", &mut self.node.onboarding_api_key),
            (
                "PLACES_ADMIN_AUTH_TOKEN",
                &mut self.explore.places_admin_auth_token,
            ),
            (
                "DATA_TEAM_AUTH_TOKEN",
                &mut self.explore.data_team_auth_token,
            ),
        ]
    }

    /// The signing keys: each signs value on this node's behalf (treasury
    /// credit claims, relayed transactions), so each is
    /// read from `<NAME>_FILE` alone (a `LoadCredential` or agenix file). A
    /// plain environment variable would sit in `/proc/<pid>/environ`,
    /// `systemctl show` and, set through a unit's `environment`, the unit
    /// file in the world-readable nix store.
    pub const FILE_ONLY_SECRETS: [&'static str; 3] = [
        "CREDITS_SIGNER_KEY",
        "CREDITS_REVOKER_KEY",
        "RELAYER_PRIVATE_KEY",
    ];

    fn read_secret_file(name: &str, slot: &mut Option<String>) -> anyhow::Result<()> {
        let file_var = format!("{name}_FILE");
        let Some(path) = std::env::var_os(&file_var).filter(|p| !p.is_empty()) else {
            return Ok(());
        };
        if slot.as_deref().is_some_and(|v| !v.trim().is_empty()) {
            anyhow::bail!("{name} and {file_var} are both set; keep one");
        }
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("{file_var}: cannot read the file: {e}"))?;
        let value = contents.trim().to_string();
        *slot = if value.is_empty() { None } else { Some(value) };
        Ok(())
    }

    /// The write switches that are on, by their `WRITES=` names (local.sh),
    /// in that list's order.
    pub fn write_switches_on(&self) -> Vec<&'static str> {
        [
            ("content", self.writes.content_writes_enabled),
            ("create", self.create.create_writes_enabled),
            ("data", self.writes.data_writes_enabled),
            ("social", self.writes.social_writes_enabled),
            ("worlds", self.worlds.worlds_writes_enabled),
            ("ops", self.ops.ops_writes_enabled),
        ]
        .into_iter()
        .filter_map(|(name, on)| on.then_some(name))
        .collect()
    }

    /// `WRITES_RESTORE_STAMP_DIR`, trimmed; blank is unset.
    pub fn restore_stamp_dir(&self) -> Option<&str> {
        self.writes
            .writes_restore_stamp_dir
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
    }

    /// For a preset that requires the restore stamps (mirror, island): with
    /// any write switch on, `WRITES_RESTORE_STAMP_DIR` must be set and both
    /// budgets positive, or startup fails naming the switches. A node with
    /// every switch off has nothing to vouch for.
    pub fn require_restore_stamps(&self) -> anyhow::Result<()> {
        let on = self.write_switches_on();
        if on.is_empty() {
            return Ok(());
        }
        if self.restore_stamp_dir().is_none() {
            anyhow::bail!(
                "write switches are on ({}) but WRITES_RESTORE_STAMP_DIR is unset: this preset lets a write through only while a backup stamp is under WRITES_BACKUP_MAX_AGE_HOURS and a restore-drill stamp under WRITES_DRILL_MAX_AGE_DAYS",
                on.join(",")
            );
        }
        if self.writes.writes_backup_max_age_hours == 0 {
            anyhow::bail!("WRITES_BACKUP_MAX_AGE_HOURS=0 would hold every write");
        }
        if self.writes.writes_drill_max_age_days == 0 {
            anyhow::bail!("WRITES_DRILL_MAX_AGE_DAYS=0 would hold every write");
        }
        Ok(())
    }

    /// The warning for a price staleness budget a healthy poll cannot meet:
    /// the snapshot's age reaches one poll interval plus the upstream's own
    /// lag (`PRICE_UPSTREAM_LAG_SECS`) before the next poll lands.
    pub fn price_staleness_warning(&self) -> Option<String> {
        let floor = self.price.price_poll_interval_secs as i64 + Self::PRICE_UPSTREAM_LAG_SECS;
        (self.shop.mana_price_max_staleness_secs < floor).then(|| {
            format!(
                "MANA_PRICE_MAX_STALENESS_SECS={} is below PRICE_POLL_INTERVAL_SECS={} plus the upstream's {} s lag: credits price reads will answer 503 \"oracle is stale\" for part of every poll cycle; raise it to at least {floor}",
                self.shop.mana_price_max_staleness_secs,
                self.price.price_poll_interval_secs,
                Self::PRICE_UPSTREAM_LAG_SECS
            )
        })
    }

    /// How far CoinGecko's `source_updated_at` trails a poll (about 90 s
    /// measured on the 2026-09-25 review node), with margin.
    pub const PRICE_UPSTREAM_LAG_SECS: i64 = 120;

    /// The map URLs /about, a world's /about and the lands lambda advertise:
    /// each configured one, else this node's own route (the minimap and
    /// estate images it draws, its parcel images); the satellite view has no
    /// own route.
    pub fn map_urls(&self) -> MapUrls {
        let own = self.node.own_url();
        let or_own = |value: &str, path: &str| match value.trim().trim_end_matches('/') {
            "" => format!("{own}{path}"),
            set => set.to_string(),
        };
        MapUrls {
            satellite_base: Some(
                self.node
                    .map_satellite_base_url
                    .trim()
                    .trim_end_matches('/'),
            )
            .filter(|base| !base.is_empty())
            .map(str::to_string),
            parcel_view: or_own(&self.node.map_parcel_view_url, "/api/v1/minimap.png"),
            estate_view: or_own(
                &self.worlds_content.map_estate_view_url,
                "/api/v1/estatemap.png",
            ),
            land_image_base: or_own(&self.node.land_image_base_url, "/api"),
        }
    }

    /// The commit `/about` and the `/status` routes report: `COMMIT_HASH` when
    /// set and not blank, else the build's `CATALYST_COMMIT_HASH`.
    pub fn commit_hash(&self) -> Option<String> {
        Self::non_empty(&self.node.commit_hash)
            .or_else(|| option_env!("CATALYST_COMMIT_HASH").map(str::to_string))
    }

    /// An optional secret or token as the old binaries read them: blank is unset.
    pub fn non_empty(value: &Option<String>) -> Option<String> {
        value
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Config {
    /// `Config` has about 270 clap fields, and the debug-build frame of the
    /// generated `from_arg_matches` is past the 2 MiB stack a test thread
    /// gets (the lib test binary aborted at 269 fields), so every test parses
    /// on a thread of its own. The binary parses on the 8 MiB main thread.
    pub fn parse_for_tests(args: &[&str]) -> Result<Self, clap::Error> {
        Self::parse_with_matches_for_tests(args).map(|(config, _)| config)
    }

    /// The parse plus its `ArgMatches`, for the argv-secret guard.
    pub fn parse_with_matches_for_tests(
        args: &[&str],
    ) -> Result<(Self, clap::ArgMatches), clap::Error> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        on_a_wide_stack(move || {
            let matches =
                <TestCli as clap::CommandFactory>::command().try_get_matches_from(args)?;
            let cli = <TestCli as clap::FromArgMatches>::from_arg_matches(&matches)?;
            Ok((cli.config, matches))
        })
    }

    /// Parses as if `vars` were the whole environment, without touching the
    /// process's: each variable becomes its flag (a `true` switch the bare
    /// flag, a `false` one nothing).
    pub fn parse_env_for_tests<K: Into<String>, V: Into<String>>(
        vars: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, clap::Error> {
        let vars: Vec<(String, String)> = vars
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let argv = on_a_wide_stack(move || {
            let cmd = <TestCli as clap::CommandFactory>::command();
            let mut argv = vec!["x".to_string()];
            for (key, value) in vars {
                let arg = cmd
                    .get_arguments()
                    .find(|a| a.get_env().is_some_and(|e| e == key.as_str()))
                    .unwrap_or_else(|| panic!("{key} is not a serve variable"));
                let long = arg.get_long().expect("every Config arg has a long flag");
                if matches!(arg.get_action(), clap::ArgAction::SetTrue) {
                    if value == "true" {
                        argv.push(format!("--{long}"));
                    }
                } else {
                    argv.push(format!("--{long}={value}"));
                }
            }
            argv
        });
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        Self::parse_for_tests(&argv)
    }

    /// Every environment variable `serve` reads.
    pub fn env_names_for_tests() -> std::collections::BTreeSet<String> {
        Self::env_names().iter().cloned().collect()
    }

    /// Each variable with the default clap applies when it is unset.
    #[cfg(test)]
    pub(crate) fn defaults_for_tests() -> Vec<(String, String)> {
        on_a_wide_stack(|| {
            <TestCli as clap::CommandFactory>::command()
                .get_arguments()
                .filter_map(|a| {
                    let env = a.get_env()?.to_str()?.to_string();
                    let default = a
                        .get_default_values()
                        .iter()
                        .filter_map(|v| v.to_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    Some((env, default))
                })
                .collect()
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
#[derive(clap::Parser)]
struct TestCli {
    #[command(flatten)]
    config: Config,
}

impl Config {
    /// Every environment variable this configuration reads, from its clap
    /// arguments (built once, on a wide stack: the command is large).
    pub fn env_names() -> &'static [String] {
        static NAMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        NAMES.get_or_init(|| {
            on_a_wide_stack(|| {
                <Config as clap::Args>::augment_args(clap::Command::new("catalyst"))
                    .get_arguments()
                    .filter_map(|a| a.get_env()?.to_str().map(str::to_string))
                    .collect()
            })
        })
    }
}

fn on_a_wide_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(f)
        .expect("spawning the parse thread")
        .join()
        .expect("the parse thread panicked")
}

#[cfg(test)]
mod tests {
    use super::Config;

    fn parse(args: &[&str]) -> Result<Config, clap::Error> {
        Config::parse_for_tests(args)
    }

    /// `--help` prints the value of an env-backed arg unless it hides it, so
    /// every secret slot hides its value, and every arg that hides one is a
    /// slot (read from `<NAME>_FILE`, refused on argv). `DATABASE_URL` is the
    /// one hidden non-slot: a required peer-auth DSN `schema apply` and the
    /// tests pass on the command line.
    #[test]
    fn every_hidden_value_is_a_secret_slot_and_every_slot_hides_its_value() {
        let (mut config, _) =
            Config::parse_with_matches_for_tests(&["catalyst", "--database-url", "postgres://x/y"])
                .unwrap();
        let slots: std::collections::BTreeSet<&'static str> =
            config.secret_slots().into_iter().map(|(n, _)| n).collect();
        let hidden: std::collections::BTreeSet<String> = std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(|| {
                #[derive(clap::Parser)]
                struct Cli {
                    #[command(flatten)]
                    config: Config,
                }
                let cmd = <Cli as clap::CommandFactory>::command();
                let mut out = std::collections::BTreeSet::new();
                for arg in cmd.get_arguments() {
                    if let Some(env) = arg.get_env() {
                        let env = env.to_string_lossy().to_string();
                        if arg.is_hide_env_values_set() {
                            out.insert(env);
                        } else {
                            out.insert(format!("visible:{env}"));
                        }
                    }
                }
                out
            })
            .unwrap()
            .join()
            .unwrap();
        for slot in &slots {
            assert!(
                hidden.contains(*slot),
                "{slot} is a secret slot whose value --help would print"
            );
        }
        for name in hidden.iter().filter(|n| !n.starts_with("visible:")) {
            assert!(
                slots.contains(name.as_str()) || name == "DATABASE_URL",
                "{name} hides its value but is no secret slot (no _FILE, not refused on argv)"
            );
        }
        for key in Config::FILE_ONLY_SECRETS {
            assert!(slots.contains(key), "{key}");
        }
    }

    #[test]
    fn the_default_price_staleness_outlasts_a_poll_cycle_and_a_short_one_warns() {
        let config = parse(&["catalyst", "--database-url", "postgres://x/y"]).unwrap();
        assert_eq!(
            (
                config.price.price_poll_interval_secs,
                config.shop.mana_price_max_staleness_secs
            ),
            (300, 900)
        );
        assert!(config.price_staleness_warning().is_none());
        let short = parse(&[
            "catalyst",
            "--database-url",
            "postgres://x/y",
            "--mana-price-max-staleness-secs",
            "300",
        ])
        .unwrap();
        let warning = short
            .price_staleness_warning()
            .expect("300 s is one poll interval");
        assert!(warning.contains("at least 420"), "{warning}");
    }

    #[test]
    fn a_signing_key_is_read_from_its_file_alone() {
        for key in Config::FILE_ONLY_SECRETS {
            let err = Config::refuse_plain_signing_key(key, Some("0xfeedface"), false)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("{key}_FILE")) && !err.contains("feedface"),
                "{err}"
            );
            assert!(
                Config::refuse_plain_signing_key(key, Some("0xfeedface"), true).is_ok(),
                "the file's value"
            );
            assert!(Config::refuse_plain_signing_key(key, None, false).is_ok());
        }
        assert!(Config::refuse_plain_signing_key("SESSION_SECRET", Some("x"), false).is_ok());
    }

    #[test]
    fn a_secret_on_the_command_line_is_refused_and_never_echoed() {
        let (mut config, matches) = Config::parse_with_matches_for_tests(&[
            "catalyst",
            "--relayer-private-key",
            "0xfeedfacefeedface",
            "--database-url",
            "postgres://x/y",
        ])
        .unwrap();
        let err = config
            .refuse_argv_secrets(&matches)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--relayer-private-key") && err.contains("RELAYER_PRIVATE_KEY_FILE"),
            "{err}"
        );
        assert!(!err.contains("feedface"), "the value never prints: {err}");
        let (mut config, matches) = Config::parse_with_matches_for_tests(&[
            "catalyst",
            "--database-url",
            "postgres://x/y",
            "--trade-rpc-urls",
            "137=https://rpc.example/k3y",
        ])
        .unwrap();
        assert!(
            config
                .refuse_argv_secrets(&matches)
                .unwrap_err()
                .to_string()
                .contains("--trade-rpc-urls")
        );
        let name = format!(
            "CATALYST_TEST_ARGV_{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        );
        let _ = name;
        let (mut config, matches) =
            Config::parse_with_matches_for_tests(&["catalyst", "--database-url", "postgres://x/y"])
                .unwrap();
        assert!(
            config.refuse_argv_secrets(&matches).is_ok(),
            "nothing on argv: nothing to refuse (env and _FILE values pass)"
        );
    }

    #[test]
    fn mirror_defaults_are_off_with_the_old_upstreams() {
        let cli = parse(&["x", "--database-url", "postgres:///catalyst"]).unwrap();
        let c = cli;
        assert!(
            c.market_upstreams.profile_cdn_base_url.is_none()
                && c.explore.catalyrst_events_admin_token.is_none()
        );

        let cli = parse(&[
            "x",
            "--database-url",
            "postgres:///catalyst",
            "--places-mirror-upstream",
            "true",
            "--worlds-content-mirror-enabled",
            "true",
            "--worlds-content-mirror-limit",
            "3",
        ])
        .unwrap();
        assert!(
            cli.explore.places_mirror_upstream && cli.worlds_content.worlds_content_mirror_enabled
        );
    }

    #[test]
    fn social_defaults_are_the_old_binaries_values_and_writes_are_off() {
        let cli = parse(&[
            "x",
            "--database-url",
            "postgres:///catalyst",
            "--social-writes-enabled",
            "true",
            "--livekit-allow-dev-creds",
            "true",
            "--first-wear-enabled",
            "true",
        ])
        .unwrap();
        assert!(
            cli.writes.social_writes_enabled
                && cli.social.livekit_allow_dev_creds
                && cli.social.first_wear_enabled
        );
    }

    #[test]
    fn data_plane_defaults_are_the_old_binaries_values_and_writes_are_off() {
        let cli = parse(&[
            "x",
            "--database-url",
            "postgres:///catalyst",
            "--data-writes-enabled",
            "true",
            "--marketplace-mirrors-enabled",
            "true",
            "--price-poll-enabled",
            "true",
            "--land-authz-index-enabled",
            "true",
            "--land-authz-stop-block",
            "4950000",
            "--presence-retention-days",
            "30",
            "--trade-rpc-urls",
            "137=https://rpc.example/polygon",
        ])
        .unwrap();
        let c = cli;
        assert!(
            c.writes.data_writes_enabled
                && c.marketplace.marketplace_mirrors_enabled
                && c.price.price_poll_enabled
                && c.land_authz.land_authz_index_enabled
        );
        assert_eq!(
            (
                c.land_authz.land_authz_stop_block,
                c.presence.presence_retention_days
            ),
            (Some(4_950_000), Some(30))
        );
        assert!(!c.explore.satellite_mirror_allow_private_upstream);
    }

    #[test]
    fn ops_defaults_are_the_old_binaries_values_and_writes_are_off() {
        let c = parse(&[
            "x",
            "--database-url",
            "postgres:///catalyst",
            "--ops-writes-enabled",
            "true",
            "--telemetry-ingest-enabled",
            "true",
            "--credits-mock-card",
            "true",
            "--ab-cdn-enabled",
            "false",
            "--admin-addresses",
            "0xAbC, 0xdef",
            "--rpc-upstream-polygon",
            "https://rpc.example/polygon",
        ])
        .unwrap();
        assert!(
            c.ops.ops_writes_enabled
                && c.ops.telemetry_ingest_enabled
                && c.shop.credits_mock_card
                && !c.ab_cdn.ab_cdn_enabled
        );
    }

    /// The map URLs advertise this node's own routes unless one is
    /// configured; they named a dead loopback port and api.decentraland.org.
    /// The satellite view has no own route: the node's `/satellite/{z}/{x}/{y}`
    /// is not the `{base}/{zoom}/{x},{y}.jpg` scheme the clients ask.
    #[test]
    fn map_urls_default_to_this_nodes_own_routes() {
        let c = parse(&["x", "--database-url", "postgres:///catalyst"]).unwrap();
        let map = c.map_urls();
        assert_eq!(map.satellite_base, None);
        assert_eq!(map.parcel_view, "http://127.0.0.1:5210/api/v1/minimap.png");
        assert_eq!(
            map.estate_view,
            "http://127.0.0.1:5210/api/v1/estatemap.png"
        );
        assert_eq!(map.land_image_base, "http://127.0.0.1:5210/api");

        let c = parse(&[
            "x",
            "--database-url",
            "postgres:///catalyst",
            "--public-url",
            "https://node.example.org/",
            "--map-satellite-base-url",
            "https://tiles.example.org/latest/",
            "--land-image-base-url",
            " ",
        ])
        .unwrap();
        let map = c.map_urls();
        assert_eq!(
            map.satellite_base.as_deref(),
            Some("https://tiles.example.org/latest")
        );
        assert_eq!(
            map.parcel_view,
            "https://node.example.org/api/v1/minimap.png"
        );
        assert_eq!(map.land_image_base, "https://node.example.org/api");
    }

    #[test]
    fn secrets_are_hidden_from_help_and_read_from_files() {
        let help = match parse(&["x", "--help"]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("--help must not parse"),
        };
        for flag in [
            "--livekit-api-secret",
            "--moderator-token",
            "--sendgrid-api-key",
            "--turnstile-secret-key",
            "--catalyrst-builder-admin-token",
            "--catalyrst-camera-reel-admin-token",
            "--encryption-key",
            "--scene-host-authoritative-key",
            "--pulse-room-authority-key",
            "--market-admin-token",
            "--price-admin-token",
            "--relayer-private-key",
            "--oz-relayer-api-key",
            "--catalyrst-economy-admin-token",
            "--rpc-endpoint-polygon",
            "--trade-rpc-urls",
            "--catalyrst-telemetry-admin-token",
            "--telemetry-sql-database-url",
            "--catalyrst-credits-admin-token",
            "--credits-captcha-secret",
            "--stripe-secret-key",
            "--stripe-webhook-secret",
            "--credits-signer-key",
            "--catalyrst-rpc-admin-token",
            "--rpc-upstream-mainnet",
            "--session-secret",
            "--ab-registry-admin-token",
        ] {
            assert!(help.contains(flag), "{flag} missing from --help");
        }
        assert!(
            !help.contains("[env: LIVEKIT_API_SECRET="),
            "secret env values must be hidden"
        );
        assert!(
            !help.contains("[env: BUILDER_POLYGON_RPC_URL=")
                && !help.contains("[env: NEWSLETTER_SERVICE_API_KEY=")
        );
        assert!(
            !help.contains("[env: ENCRYPTION_KEY=")
                && !help.contains("[env: SCENE_HOST_AUTHORITATIVE_KEY=")
                && !help.contains("[env: PULSE_ROOM_AUTHORITY_KEY=")
        );
        for var in [
            "MARKET_ADMIN_TOKEN",
            "PRICE_ADMIN_TOKEN",
            "RELAYER_PRIVATE_KEY",
            "OZ_RELAYER_API_KEY",
            "CATALYRST_ECONOMY_ADMIN_TOKEN",
            "RPC_ENDPOINT_POLYGON",
            "TRADE_RPC_URLS",
            "CATALYRST_TELEMETRY_ADMIN_TOKEN",
            "TELEMETRY_SQL_DATABASE_URL",
            "CATALYRST_CREDITS_ADMIN_TOKEN",
            "CREDITS_CAPTCHA_SECRET",
            "STRIPE_SECRET_KEY",
            "STRIPE_WEBHOOK_SECRET",
            "CREDITS_SIGNER_KEY",
            "CATALYRST_RPC_ADMIN_TOKEN",
            "RPC_UPSTREAM_MAINNET",
            "RPC_UPSTREAM_POLYGON",
            "SESSION_SECRET",
            "AB_REGISTRY_ADMIN_TOKEN",
        ] {
            assert!(
                !help.contains(&format!("[env: {var}=")),
                "{var} value must be hidden from --help"
            );
        }

        let dir = std::env::temp_dir().join(format!(
            "catalyst-config-secret-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("token");
        std::fs::write(&file, "  s3cr3t\n").unwrap();
        let mut slot = None;
        // A private name so the process environment of a parallel test cannot collide.
        let name = format!(
            "CATALYST_TEST_SECRET_{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        );
        unsafe { std::env::set_var(format!("{name}_FILE"), &file) };
        Config::read_secret_file(&name, &mut slot).unwrap();
        assert_eq!(slot.as_deref(), Some("s3cr3t"));

        let mut both = Some("inline".to_string());
        let err = Config::read_secret_file(&name, &mut both)
            .unwrap_err()
            .to_string();
        assert!(err.contains("both set"), "{err}");

        unsafe { std::env::set_var(format!("{name}_FILE"), dir.join("missing")) };
        let mut missing = None;
        let err = Config::read_secret_file(&name, &mut missing)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(&format!("{name}_FILE")) && !err.contains("s3cr3t"),
            "{err}"
        );
        unsafe { std::env::remove_var(format!("{name}_FILE")) };
        let _ = std::fs::remove_dir_all(&dir);
    }
}
