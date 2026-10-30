//! The socket's handshake and lifetime as upstream's `ws-handler.spec.ts`
//! (`decentraland/social-service-ea` at `fb3dcb8`) drives them, over a real
//! WebSocket: the frames the explorer could send first, what closes and
//! with which code, and what an authenticated socket keeps.

use super::*;

async fn first_answer(server: &Server, frame: &str) -> WsMessage {
    let mut socket = connect(server, "/social-rpc").await;
    socket
        .send(WsMessage::Text(frame.to_string().into()))
        .await
        .unwrap();
    next_message(&mut socket).await
}

fn is_server_ready(msg: &WsMessage) -> bool {
    let WsMessage::Binary(b) = msg else {
        return false;
    };
    matches!(
        super::super::drpc::rpc_protocol::parse::parse_header(b),
        Some((
            super::super::drpc::rpc_protocol::RpcMessageTypes::ServerReady,
            _
        ))
    )
}

/// A signed frame with one field replaced by `value` (or added).
async fn valid_frame_with(key: &str, value: serde_json::Value) -> String {
    let (_, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    let mut frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
    frame[key] = value;
    frame.to_string()
}

/// Upstream's `describe.each` of malformed first frames: each closes 3003
/// before any verification, never an internal error. An `x-identity-*`
/// value that is neither a string nor an array of strings is one of them,
/// metadata included (`ws-handler.ts:139-146`).
#[tokio::test]
async fn a_malformed_first_frame_closes_unauthorized() {
    let server = spawn_server(None, 5).await;
    for frame in ["", "null", "123", "\"x\"", "[]", "{}"] {
        assert_eq!(
            close_code(&first_answer(&server, frame).await),
            Some(3003),
            "{frame:?}"
        );
    }
    for (key, value) in [
        (
            "x-identity-timestamp",
            serde_json::json!(1_700_000_000_000_i64),
        ),
        (
            "x-identity-auth-chain-0",
            serde_json::json!({ "type": "SIGNER" }),
        ),
        ("x-identity-metadata", serde_json::json!({})),
        ("x-identity-metadata", serde_json::Value::Null),
        ("x-identity-auth-chain-1", serde_json::json!(["{}", 1])),
        ("X-Identity-Foo", serde_json::json!(1)),
        ("X-IDENTITY-METADATA", serde_json::json!({})),
    ] {
        let frame = valid_frame_with(key, value).await;
        assert_eq!(
            close_code(&first_answer(&server, &frame).await),
            Some(3003),
            "{key}"
        );
    }
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// `verify()` reads only the exact lowercase keys (`@dcl/crypto-middleware`
/// 6.3.0 `dist/verify.js:72`, `:321-322`): a re-cased duplicate next to the
/// lowercase header passes, and a timestamp sent only as
/// `X-Identity-Timestamp` is absent, so the socket closes 3003.
#[tokio::test]
async fn a_re_cased_identity_header_is_never_read_on_the_socket() {
    let server = spawn_server(None, 5).await;
    for (key, value) in [
        ("X-Identity-Timestamp", serde_json::json!([])),
        ("X-Identity-Timestamp", serde_json::json!("0")),
    ] {
        let frame = valid_frame_with(key, value.clone()).await;
        let answer = first_answer(&server, &frame).await;
        assert!(is_server_ready(&answer), "{key} {value}: {answer:?}");
    }
    let (_, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    let mut frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
    let ts = frame
        .as_object_mut()
        .unwrap()
        .remove("x-identity-timestamp")
        .unwrap();
    frame["X-Identity-Timestamp"] = ts;
    assert_eq!(
        close_code(&first_answer(&server, &frame.to_string()).await),
        Some(3003)
    );
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// A field outside `x-identity-*` is left alone: the chain still verifies.
#[tokio::test]
async fn a_field_outside_the_identity_headers_does_not_stop_the_handshake() {
    let server = spawn_server(None, 5).await;
    for value in [
        serde_json::json!(2),
        serde_json::json!({ "nested": [1] }),
        serde_json::Value::Null,
    ] {
        let frame = valid_frame_with("version", value.clone()).await;
        let answer = first_answer(&server, &frame).await;
        assert!(is_server_ready(&answer), "{value}: {answer:?}");
    }
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// A contract wallet's chain whose validation cannot reach the chain (upstream:
/// the catalyst's 503) closes 3003 like any other refusal.
#[tokio::test]
async fn an_unreachable_contract_validator_closes_unauthorized() {
    static ASKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    struct Unreachable;
    #[async_trait::async_trait]
    impl crate::auth::eip1654::Eip1654Validator for Unreachable {
        async fn validate_signature(
            &self,
            _contract: &str,
            _hash: &[u8],
            _signature: &[u8],
        ) -> Result<bool, crate::auth::AuthError> {
            ASKED.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(crate::auth::AuthError::Eip1654ValidationFailed(
                "unreachable".into(),
            ))
        }
    }
    let settings = Settings {
        auth_window_secs: 5,
        ..Settings::default()
    };
    let server = spawn_server_with(None, settings, Some(Arc::new(Unreachable))).await;
    let ts = chrono::Utc::now().timestamp_millis().to_string();
    let link = |kind: &str, payload: &str, signature: &str| {
        serde_json::json!({ "type": kind, "payload": payload, "signature": signature }).to_string()
    };
    let frame = serde_json::json!({
        "x-identity-auth-chain-0": link("SIGNER", &format!("0x{}", "ab".repeat(20)), ""),
        "x-identity-auth-chain-1": link(
            "ECDSA_EIP_1654_EPHEMERAL",
            &format!("Decentraland Login\nEphemeral address: 0x{}\nExpiration: 2099-01-01T00:00:00.000Z", "cd".repeat(20)),
            &format!("0x{}", "11".repeat(65)),
        ),
        "x-identity-auth-chain-2": link(
            "ECDSA_SIGNED_ENTITY",
            &crate::shared::signed_fetch::build_payload_v6("get", "/", &ts, "{}"),
            &format!("0x{}", "22".repeat(65)),
        ),
        "x-identity-timestamp": ts,
        "x-identity-metadata": "{}",
    });
    assert_eq!(
        close_code(&first_answer(&server, &frame.to_string()).await),
        Some(3003)
    );
    assert!(
        ASKED.load(std::sync::atomic::Ordering::SeqCst),
        "the refusal came before the validator"
    );
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// Each field as an array of strings authenticates: an array reads as its
/// first element (`firstOf`, `@dcl/crypto-middleware` 6.3.0 `dist/verify.js`).
#[tokio::test]
async fn an_array_of_strings_authenticates() {
    let server = spawn_server(None, 5).await;
    let (_, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    let mut frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
    for value in frame.as_object_mut().unwrap().values_mut() {
        *value = serde_json::json!([value.clone()]);
    }
    let answer = first_answer(&server, &frame.to_string()).await;
    assert!(is_server_ready(&answer), "{answer:?}");
    // The metadata alone as an array passes the pre-check; its first element
    // is the signed `{}`, whatever follows it.
    for metadata in [
        serde_json::json!(["{}"]),
        serde_json::json!(["{}", "ignored"]),
    ] {
        let frame = valid_frame_with("x-identity-metadata", metadata.clone()).await;
        let answer = first_answer(&server, &frame).await;
        assert!(is_server_ready(&answer), "{metadata}: {answer:?}");
    }
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// A chain signed outside the freshness window closes 3003 on the socket.
#[tokio::test]
async fn an_expired_chain_closes_unauthorized() {
    let server = spawn_server(None, 5).await;
    let stale = chrono::Utc::now().timestamp_millis() - 10 * 60 * 1000;
    let (_, frame) = frame("/", stale).await;
    assert_eq!(close_code(&first_answer(&server, &frame).await), Some(3003));
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// The handshake timeout bounds only the wait for the first frame: an
/// authenticated socket idle past it stays open.
#[tokio::test]
async fn an_authenticated_socket_outlives_the_handshake_timeout() {
    let server = spawn_server(None, 1).await;
    let mut socket = connect(&server, "/social-rpc").await;
    let (_, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    assert!(is_server_ready(&next_message(&mut socket).await));
    match tokio::time::timeout(Duration::from_millis(2500), socket.next()).await {
        Err(_) => {}
        Ok(Some(Ok(msg))) => assert!(close_code(&msg).is_none() && !msg.is_close(), "{msg:?}"),
        Ok(other) => panic!("the authenticated socket ended: {other:?}"),
    }
    assert_eq!(server.state.ctx.live_connections(), 1);
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// Closing an authenticated socket gives its slot back.
#[tokio::test]
async fn closing_an_authenticated_socket_releases_its_connection() {
    let server = spawn_server(None, 5).await;
    let mut socket = connect(&server, "/social-rpc").await;
    let (_, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    assert!(is_server_ready(&next_message(&mut socket).await));
    assert_eq!(server.state.ctx.live_connections(), 1);
    socket.close(None).await.unwrap();
    assert!(
        crate::shared::test_wait::until(async || server.state.ctx.live_connections() == 0).await,
        "the closed socket still counts"
    );
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// Opens a socket and authenticates it as a wallet made for the call.
async fn authenticated(server: &Server) -> WsStream {
    let mut socket = connect(server, "/social-rpc").await;
    let ts = chrono::Utc::now().timestamp_millis();
    let payload = crate::shared::signed_fetch::build_payload_v6("get", "/", &ts.to_string(), "{}");
    let (_, frame) = frame_signed(
        &PrivateKeySigner::random(),
        &PrivateKeySigner::random(),
        ts,
        &payload,
        "{}",
    )
    .await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    assert!(is_server_ready(&next_message(&mut socket).await));
    socket
}

/// Upstream's `ws-pool-component.spec.ts` "registering multiple connections":
/// each socket counts, and closing one frees only its own slot.
#[tokio::test]
async fn each_socket_counts_and_closing_one_frees_its_slot() {
    let server = spawn_server(None, 5).await;
    let mut first = authenticated(&server).await;
    let _second = authenticated(&server).await;
    assert!(
        crate::shared::test_wait::until(async || server.state.ctx.live_connections() == 2).await,
        "two sockets, two slots"
    );
    first.close(None).await.unwrap();
    assert!(
        crate::shared::test_wait::until(async || server.state.ctx.live_connections() == 1).await,
        "closing one frees one"
    );
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// Upstream's pool cap (`src/logic/ws-pool/component.ts:26-29`,
/// `src/controllers/handlers/uws/ws-handler.ts:407`): with the pool full a new
/// socket closes 1013, and a freed slot is taken again. Recorded difference
/// (RPC.md, ws-pool-component.spec.ts, "A full pool refuses a new socket"):
/// the reason reads "Server is at capacity; try again later", not "Unable to
/// acquire connection", and only authenticated sockets count.
#[tokio::test]
async fn a_full_pool_closes_a_new_socket_until_a_slot_frees() {
    let settings = Settings {
        auth_window_secs: 5,
        ws_max_concurrent_connections: Some(1),
        ..Settings::default()
    };
    let server = spawn_server_with(None, settings, None).await;
    let mut first = authenticated(&server).await;
    assert!(
        crate::shared::test_wait::until(async || server.state.ctx.live_connections() == 1).await
    );
    let mut refused = connect(&server, "/social-rpc").await;
    match next_message(&mut refused).await {
        WsMessage::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), 1013);
            assert_eq!(
                frame.reason.as_str(),
                "Server is at capacity; try again later"
            );
        }
        other => panic!("expected a close, got {other:?}"),
    }
    first.close(None).await.unwrap();
    assert!(
        crate::shared::test_wait::until(async || server.state.ctx.live_connections() == 0).await
    );
    let _again = authenticated(&server).await;
    server.jobs.shutdown(Duration::from_secs(2)).await;
}
