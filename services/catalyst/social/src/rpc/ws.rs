//! The socket: an axum WebSocket upgrade whose first frame must be the signed
//! handshake (`handshake.rs`) within the auth window, closed with `3003
//! Unauthorized` otherwise and `1013` when the connection cap is reached;
//! after it the socket is a binary [`Transport`](crate::shared::rpc::drpc::transport::Transport) attached to the RPC server,
//! which answers `SERVER_READY` and serves the ports the client creates.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use axum::extract::State as AxumState;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Uri};
use axum::response::{IntoResponse, Response};
use tokio::time::timeout;

use crate::rpc::State;
use crate::shared::identity::Address;
use crate::shared::rpc::handshake::{candidate_paths, validator_ref, verify_handshake};
use crate::shared::rpc::ws::AxumWsTransport;

pub async fn ws_upgrade(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
    uri: Uri,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    // upstream's `app.ws('/')` takes upgrades only; a request that is not one
    // (no `Sec-WebSocket-Key`) falls to its `ANY /*`. A broken upgrade keeps
    // axum's answer (400, 405 or 426): what uWS answers it is not in the
    // reference's source.
    let ws = match ws {
        Ok(ws) => ws,
        Err(_) if !headers.contains_key(axum::http::header::SEC_WEBSOCKET_KEY) => {
            return super::not_found().await;
        }
        Err(rejection) => return rejection.into_response(),
    };
    let paths = candidate_paths(&headers, uri.path());
    ws.max_message_size(state.settings.ws_max_payload_bytes)
        .on_upgrade(move |socket| async move {
            if let Err(err) = handle_connection(state, paths, socket).await {
                tracing::warn!(error = %err, "social-rpc ws connection ended");
            }
        })
        .into_response()
}

async fn handle_connection(
    state: Arc<State>,
    paths: Vec<String>,
    mut socket: WebSocket,
) -> Result<()> {
    if let Some(max) = state.settings.ws_max_concurrent_connections
        && state.ctx.live_connections() >= max
    {
        tracing::warn!(max, "rejecting ws connection: pool is full");
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: 1013,
                reason: "Server is at capacity; try again later".into(),
            })))
            .await;
        return Ok(());
    }
    let address = match auth_handshake(&state, &paths, &mut socket).await {
        Ok(addr) => addr,
        Err(err) => {
            tracing::info!(%err, "auth handshake failed");
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: 3003,
                    reason: "Unauthorized".into(),
                })))
                .await;
            return Ok(());
        }
    };
    tracing::info!(address = %address, "social-rpc client authenticated");

    let Some(events) = state.rpc_events() else {
        return Err(anyhow!(
            "rpc server not running; State::spawn_jobs was not called"
        ));
    };
    let transport = Arc::new(AxumWsTransport::spawn(socket, address.into_string()));
    events
        .send_attach_transport(transport)
        .map_err(|e| anyhow!("attach transport: {e:?}"))?;
    Ok(())
}

async fn auth_handshake(
    state: &State,
    paths: &[String],
    socket: &mut WebSocket,
) -> Result<Address> {
    let window_secs = state.settings.auth_window_secs.max(1);
    let first = timeout(Duration::from_secs(window_secs as u64), socket.recv())
        .await
        .map_err(|_| anyhow!("auth timeout after {window_secs}s"))?
        .ok_or_else(|| anyhow!("socket closed before auth"))?
        .map_err(|e| anyhow!("ws recv error: {e}"))?;
    let text: String = match first {
        Message::Text(s) => s.to_string(),
        Message::Binary(b) => String::from_utf8(b.to_vec())
            .map_err(|_| anyhow!("auth payload was binary but not utf-8 json"))?,
        _ => return Err(anyhow!("expected text/binary frame for auth")),
    };
    let now_secs = chrono::Utc::now().timestamp();
    verify_handshake(
        &text,
        "get",
        paths,
        state.settings.auth_window_secs,
        now_secs,
        validator_ref(&state.eip1654),
    )
    .await
    .map_err(|e| anyhow!("handshake: {e}"))
}
