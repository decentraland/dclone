//! The social RPC: the Decentraland-RPC WebSocket unity-explorer opens for
//! friends, friendship requests and their events, presence, blocks, the
//! social settings, private calls and community voice rooms (the old
//! `catalyrst-social-rpc` executable, a port of decentraland/social-service-ea
//! over `dcl-rpc`). The socket is [`ws::ws_upgrade`]: a signed handshake
//! frame (`handshake`), then protobuf-over-WebSocket framed by `drpc` with
//! the one module `service` registers; every procedure is answered from
//! `social.*` over the ONE pool (`db`) and fans its events out in-process
//! (`pubsub`; local delivery). Online status is reconstructed independently on
//! every replica from the complete Pulse topology over the existing comms NATS link.
//!
//! Voice needs LiveKit: call persistence and media operations use the gatekeeper's
//! `VoiceRooms` seam (`comms::gatekeeper::voice::rooms`), which its state
//! implements in `catalyst serve`; its LiveKit configuration controls call
//! availability. Presence questions are answered from the
//! shared presence tables.
//!
//! Wiring (main.rs): `let social_rpc = Arc::new(social::rpc::State::new(pool,
//! settings, eip1654, gatekeeper.clone()))`; `social_rpc.spawn_jobs(&jobs)`
//! (the RPC server loop and the private-voice expiry sweep);
//! `.merge(social::rpc::router(social_rpc))`. Routes: `GET /social-rpc`
//! (the upgrade; the handshake accepts a signature over `/` or `/social-rpc`,
//! the only `x-original-path` a proxy may forward), `GET /social-rpc/info`,
//! `GET /social-rpc/health` (local), and the upstream uWS host's HTTP routes
//! (`src/controllers/routes/uws.routes.ts`): `GET /social-rpc/status`, any
//! method on `/social-rpc/health/live`,
//! `GET /social-rpc/v1/users/{address}/privacy-settings` and a 404
//! `{"error":"Not Found"}` for every other `/social-rpc/*` path or method,
//! all with `Access-Control-Allow-Origin: *`; then the bearer-gated
//! `/admin/social/*` (`admin`).

pub mod admin;
pub mod context;
pub mod db;
pub use crate::shared::rpc::{drpc, handshake};
pub mod limits;
pub mod profiles;
pub mod proto;
pub mod pubsub;
pub mod service;
mod topology;
pub mod voice;
pub mod ws;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State as AxumState};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::auth::eip1654::Eip1654Validator;
use crate::comms::gatekeeper::voice::rooms::VoiceRooms;
use crate::jobs::{Jobs, PeriodicCfg};
use crate::rpc::context::{Context, VoiceSettings};
use crate::rpc::db::Db;
use crate::rpc::drpc::server::{RpcServer, ServerEventsSender};
use crate::rpc::profiles::Profiles;
use crate::shared::rpc::ws::AxumWsTransport;

/// The socket's settings (the old `catalyrst-social-rpc` env, minus its
/// database and gatekeeper URLs, which the ONE pool and the voice seam
/// replace).
#[derive(Clone, Debug)]
pub struct Settings {
    /// `AUTH_WINDOW_SECS`: how long the first frame may take to arrive and how
    /// far its timestamp may sit from the server clock.
    pub auth_window_secs: i64,
    /// `PRIVATE_VOICE_CHAT_EXPIRATION_TIME` (ms): a call not accepted within it expires.
    pub private_voice_chat_expiration_ms: i64,
    /// `PRIVATE_VOICE_CHAT_JOB_INTERVAL` (ms): the expiry sweep's tick.
    pub private_voice_chat_job_interval_ms: u64,
    /// `PRIVATE_VOICE_CHAT_EXPIRATION_BATCH_SIZE`: expired calls reclaimed per DELETE.
    pub private_voice_chat_expiration_batch_size: i64,
    /// `WS_MAX_CONCURRENT_CONNECTIONS`: unset = unlimited.
    pub ws_max_concurrent_connections: Option<usize>,
    /// `WS_MAX_PAYLOAD_LENGTH`: the largest inbound frame.
    pub ws_max_payload_bytes: usize,
    /// `CATALYRST_SOCIAL_RPC_ADMIN_TOKEN`: the `/admin/social` bearer; unset refuses.
    pub admin_token: Option<String>,
    /// Where profile pictures are served.
    pub faces: crate::communities::profiles::FaceUrls,
    /// `SOCIAL_WRITES_ENABLED`: false answers every mutating procedure with a
    /// 503 `WritesDisabled` remote error (`social::writes`).
    pub writes_enabled: bool,
    /// `COMMIT_HASH` (or the build's `CATALYST_COMMIT_HASH`) for `/social-rpc/status`.
    pub commit_hash: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            auth_window_secs: 300,
            private_voice_chat_expiration_ms: 60_000,
            private_voice_chat_job_interval_ms: 1_000,
            private_voice_chat_expiration_batch_size: 20,
            ws_max_concurrent_connections: None,
            ws_max_payload_bytes: 1024 * 1024,
            admin_token: None,
            faces: Default::default(),
            writes_enabled: true,
            commit_hash: None,
        }
    }
}

pub struct State {
    pub settings: Settings,
    pub ctx: Context,
    pub eip1654: Option<Arc<dyn Eip1654Validator>>,
    rpc_events: OnceCell<ServerEventsSender<AxumWsTransport>>,
    /// The server, until `spawn_jobs` takes it into its job.
    server: Mutex<Option<RpcServer<Context, AxumWsTransport>>>,
}

impl State {
    pub fn new(
        pool: PgPool,
        settings: Settings,
        eip1654: Option<Arc<dyn Eip1654Validator>>,
        voice: Arc<dyn VoiceRooms>,
    ) -> Self {
        let profiles = Profiles::new(Some(pool.clone()), settings.faces.clone());
        let ctx = Context::new(
            VoiceSettings {
                private_voice_chat_expiration_ms: settings.private_voice_chat_expiration_ms,
                private_voice_chat_expiration_batch_size: settings
                    .private_voice_chat_expiration_batch_size,
            },
            Db::new(pool),
            profiles,
            voice,
        );
        ctx.set_writes_enabled(settings.writes_enabled);
        Self::with_context(settings, ctx, eip1654)
    }

    /// A state over an already built context (the tests hand in a fake voice
    /// runtime or a pool-less profile reader).
    pub fn with_context(
        settings: Settings,
        ctx: Context,
        eip1654: Option<Arc<dyn Eip1654Validator>>,
    ) -> Self {
        let (sender, server) = build_rpc_server(ctx.clone());
        let rpc_events = OnceCell::new();
        rpc_events.set(sender).ok();
        Self {
            settings,
            ctx,
            eip1654,
            rpc_events,
            server: Mutex::new(Some(server)),
        }
    }

    /// The handle the socket attaches transports through; `None` before the
    /// server job took the server (never, after `spawn_jobs`).
    pub fn rpc_events(&self) -> Option<&ServerEventsSender<AxumWsTransport>> {
        self.rpc_events.get()
    }

    /// The RPC server loop (one job; it ends at shutdown) and the private-
    /// voice expiry sweep (a periodic job on the configured tick).
    pub fn spawn_jobs(self: &Arc<Self>, jobs: &Jobs) {
        if let Some(mut server) = self.server.lock().take() {
            jobs.spawn("social-rpc-server", move |ctx| async move {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {}
                    _ = server.run() => {}
                }
                Ok(())
            });
        }
        let ctx = self.ctx.clone();
        let expiration_ms = self.settings.private_voice_chat_expiration_ms;
        let batch_size = self.settings.private_voice_chat_expiration_batch_size;
        let interval =
            Duration::from_millis(self.settings.private_voice_chat_job_interval_ms.max(1));
        jobs.spawn_periodic_leased(
            "private-voice-chat-expiry",
            PeriodicCfg::every(interval),
            move |_job| {
                let ctx = ctx.clone();
                async move {
                    ctx.sweep_private_voice_chats_if_due(expiration_ms, batch_size)
                        .await;
                    Ok(())
                }
            },
        );
    }
}

/// The server with its transport handlers: a connected socket registers its
/// identity and announces the address online (once per address, across its
/// sockets); a closed one forgets it, announces offline and ends the call it
/// was on (#479).
fn build_rpc_server(
    ctx: Context,
) -> (
    ServerEventsSender<AxumWsTransport>,
    RpcServer<Context, AxumWsTransport>,
) {
    let mut server: RpcServer<Context, AxumWsTransport> = RpcServer::create(ctx.clone());
    let sender = server.get_server_events_sender();

    let bind_ctx = ctx.clone();
    server.set_on_transport_connected_handler(move |transport, transport_id| {
        bind_ctx.connection_opened();
        let address = transport.address().to_string();
        bind_ctx.register_identity(transport_id, address.clone());
        bind_ctx.register_kill_handle(transport_id, transport.kill_handle());
        bind_ctx.mark_online(&address);
    });
    let forget_ctx = ctx.clone();
    server.set_on_transport_closes_handler(move |transport, transport_id| {
        forget_ctx.connection_closed();
        forget_ctx.forget_identity(transport_id);
        let address = transport.address().to_string();
        if forget_ctx.mark_offline(&address) {
            let voice_ctx = forget_ctx.clone();
            tokio::spawn(async move {
                voice_ctx
                    .end_private_voice_chat_on_disconnect(&address)
                    .await;
            });
        }
    });
    server.set_module_registrator_handler(service::register_service);
    (sender, server)
}

/// The socket at `/social-rpc`, the upstream host's HTTP routes and the local
/// info/health beside it (one `Access-Control-Allow-Origin` layer over them,
/// as upstream's `wrap`), and the admin routes.
pub fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/social-rpc", get(ws::ws_upgrade).fallback(not_found))
        .route("/social-rpc/info", get(info))
        .route("/social-rpc/health", get(health))
        .route("/social-rpc/health/live", any(health_live))
        // upstream's `app.get` takes GET alone; HEAD falls to its `ANY /*`
        .route(
            "/social-rpc/status",
            get(status).head(not_found).fallback(not_found),
        )
        .route(
            "/social-rpc/v1/users/{address}/privacy-settings",
            get(privacy_settings).head(not_found).fallback(not_found),
        )
        .route("/social-rpc/{*rest}", any(not_found))
        .layer(axum::middleware::map_response(allow_any_origin))
        .nest("/admin/social", admin::router())
        .with_state(state)
}

/// Upstream's `wrap`: `*` unless the handler set an origin itself.
async fn allow_any_origin(mut response: Response) -> Response {
    response
        .headers_mut()
        .entry(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .or_insert(HeaderValue::from_static("*"));
    response
}

/// Upstream's `ANY /*`.
pub(super) async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "Not Found" }))).into_response()
}

/// `handlers/uws/status-handler.ts`.
async fn status(AxumState(state): AxumState<Arc<State>>) -> Json<serde_json::Value> {
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "currentTime": current_time,
        "commitHash": state.settings.commit_hash.as_deref().unwrap_or(""),
    }))
}

/// `handlers/uws/privacy-handler.ts`: the address lowercased (never trimmed),
/// then the stored privacy or the absent-row `all`.
async fn privacy_settings(
    AxumState(state): AxumState<Arc<State>>,
    Path(address): Path<String>,
) -> Response {
    let Ok(address) = crate::shared::identity::Address::parse_untrimmed(&address) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid address" })),
        )
            .into_response();
    };
    match state.ctx.db().get_social_settings(address.as_str()).await {
        Ok(row) => Json(json!({
            "private_messages_privacy": row.unwrap_or_default().private_messages_privacy,
        }))
        .into_response(),
        // upstream echoes the error's message; the shape is kept, the
        // database detail stays in the log
        Err(e) => {
            tracing::warn!(error = %e, "privacy-settings read failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Internal Server Error" })),
            )
                .into_response()
        }
    }
}

async fn info() -> Json<serde_json::Value> {
    Json(
        json!({ "service": "catalyrst-social-rpc", "version": env!("CARGO_PKG_VERSION"), "ws": "/social-rpc" }),
    )
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "healthy": true }))
}

async fn health_live() -> &'static str {
    "alive"
}
