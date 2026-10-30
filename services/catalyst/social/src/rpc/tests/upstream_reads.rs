//! The read-only procedures against a dated capture of the production
//! service (`wss://rpc-social-service-ea.decentraland.org`). The capture is an
//! ignored test run by hand with `CATALYST_SOCIAL_RPC_CAPTURE=1`: it signs in
//! with a key generated for the run and held only in memory, calls the eight
//! read procedures below (ten calls: `GetFriends` and `GetFriendshipStatus`
//! twice each; no subscription, no write), and writes the request
//! and response bytes to `tests/fixtures/conformance/social/social-rpc/_socket/reads.json`
//! with the chain's signatures redacted (the HTTP replay skips `_` folders).
//! The status reads name the zero address or a second address generated for
//! the run, never a real user. The replay sends the recorded requests to the
//! local `/social-rpc` as a wallet without data and expects the same bytes
//! back; it first clears the stored settings of the two named addresses, the
//! only rows of the shared database those answers read (the caller is new, so
//! no friendship or block can involve it).

use alloy::signers::local::PrivateKeySigner;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::{Buf, BufMut};
use prost::encoding::{DecodeContext, WireType};
use serde_json::{Value, json};

use super::*;
use crate::shared::rpc::handshake::test_support::frame_signed;

const UPSTREAM: &str = "wss://rpc-social-service-ea.decentraland.org";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const PAUSE: Duration = Duration::from_millis(500);

fn fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/fixtures/conformance/social/social-rpc/_socket/reads.json")
}

/// A message's bytes as they travel: what is sent is sent unchanged and what
/// is received is kept whole.
#[derive(Debug, Default, Clone, PartialEq)]
struct Raw(Vec<u8>);

impl prost::Message for Raw {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        buf.put_slice(&self.0);
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        unreachable!("merge keeps the whole buffer")
    }

    fn merge(&mut self, mut buf: impl Buf) -> Result<(), prost::DecodeError> {
        while buf.has_remaining() {
            let chunk = buf.chunk();
            self.0.extend_from_slice(chunk);
            let n = chunk.len();
            buf.advance(n);
        }
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        self.0.len()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

struct RawClient {
    module: RpcClientModule<TungsteniteTransport>,
}

impl ServiceClient<TungsteniteTransport> for RawClient {
    fn set_client_module(module: RpcClientModule<TungsteniteTransport>) -> Self {
        Self { module }
    }
}

impl RawClient {
    async fn call(&self, procedure: &str, request: &[u8]) -> Vec<u8> {
        let Raw(bytes) = self
            .module
            .call_unary_procedure(procedure, Raw(request.to_vec()))
            .await
            .unwrap_or_else(|e| panic!("{procedure}: {e:?}"));
        bytes
    }
}

fn show<T: prost::Message + Default + std::fmt::Debug>(bytes: &[u8]) -> String {
    format!("{:?}", T::decode(bytes).expect("decodes"))
}

/// One recorded read: its procedure and how to print its request and answer.
struct Read {
    name: &'static str,
    procedure: &'static str,
    request: fn(&[u8]) -> String,
    response: fn(&[u8]) -> String,
}

const PAGE: Pagination = Pagination {
    limit: 50,
    offset: 0,
};

fn reads() -> [Read; 10] {
    [
        Read {
            name: "get-friends",
            procedure: "GetFriends",
            request: show::<GetFriendsPayload>,
            response: show::<PaginatedFriendsProfilesResponse>,
        },
        Read {
            name: "get-friends-no-pagination",
            procedure: "GetFriends",
            request: show::<GetFriendsPayload>,
            response: show::<PaginatedFriendsProfilesResponse>,
        },
        Read {
            name: "get-pending-friendship-requests",
            procedure: "GetPendingFriendshipRequests",
            request: show::<GetFriendshipRequestsPayload>,
            response: show::<PaginatedFriendshipRequestsResponse>,
        },
        Read {
            name: "get-sent-friendship-requests",
            procedure: "GetSentFriendshipRequests",
            request: show::<GetFriendshipRequestsPayload>,
            response: show::<PaginatedFriendshipRequestsResponse>,
        },
        Read {
            name: "get-blocked-users",
            procedure: "GetBlockedUsers",
            request: show::<GetBlockedUsersPayload>,
            response: show::<GetBlockedUsersResponse>,
        },
        Read {
            name: "get-social-settings",
            procedure: "GetSocialSettings",
            request: show::<()>,
            response: show::<GetSocialSettingsResponse>,
        },
        Read {
            name: "get-private-messages-settings",
            procedure: "GetPrivateMessagesSettings",
            request: show::<GetPrivateMessagesSettingsPayload>,
            response: show::<GetPrivateMessagesSettingsResponse>,
        },
        Read {
            name: "get-friendship-status-zero",
            procedure: "GetFriendshipStatus",
            request: show::<GetFriendshipStatusPayload>,
            response: show::<GetFriendshipStatusResponse>,
        },
        Read {
            name: "get-friendship-status-throwaway",
            procedure: "GetFriendshipStatus",
            request: show::<GetFriendshipStatusPayload>,
            response: show::<GetFriendshipStatusResponse>,
        },
        Read {
            name: "get-blocking-status",
            procedure: "GetBlockingStatus",
            request: show::<()>,
            response: show::<GetBlockingStatusResponse>,
        },
    ]
}

/// The requests of [`reads`], in order; `other` is the run's second address.
fn requests(other: &str) -> [Vec<u8>; 10] {
    let user = |address: &str| {
        Some(User {
            address: address.to_string(),
        })
    };
    [
        GetFriendsPayload {
            pagination: Some(PAGE),
        }
        .encode_to_vec(),
        GetFriendsPayload { pagination: None }.encode_to_vec(),
        GetFriendshipRequestsPayload {
            pagination: Some(PAGE),
        }
        .encode_to_vec(),
        GetFriendshipRequestsPayload {
            pagination: Some(PAGE),
        }
        .encode_to_vec(),
        GetBlockedUsersPayload {
            pagination: Some(PAGE),
        }
        .encode_to_vec(),
        Vec::new(),
        GetPrivateMessagesSettingsPayload {
            user: vec![user(ZERO).unwrap(), user(other).unwrap()],
        }
        .encode_to_vec(),
        GetFriendshipStatusPayload { user: user(ZERO) }.encode_to_vec(),
        GetFriendshipStatusPayload { user: user(other) }.encode_to_vec(),
        Vec::new(),
    ]
}

/// Signs in as a key generated here, delegated for ten minutes, and loads the
/// module over `socket`.
async fn sign_in(
    mut socket: WsStream,
    path: &str,
) -> (String, Value, RpcClient<TungsteniteTransport>, RawClient) {
    let root = PrivateKeySigner::random();
    let ephemeral = PrivateKeySigner::random();
    let now = chrono::Utc::now();
    let expiration =
        (now + chrono::Duration::minutes(10)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let ts = now.timestamp_millis();
    let payload = crate::shared::signed_fetch::build_payload_v6("get", path, &ts.to_string(), "{}");
    let (signer, frame) = frame_signed(&root, &ephemeral, &expiration, ts, &payload, "{}").await;
    drop((root, ephemeral));
    let chain = redacted_chain(&frame);
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    let mut client = RpcClient::new(TungsteniteTransport::new(socket))
        .await
        .expect("SERVER_READY");
    let port = client.create_port("social").await.expect("port");
    let raw: RawClient = port.load_module(MODULE_NAME).await.expect("module");
    (signer, chain, client, raw)
}

/// The frame's links with every signature replaced, as the fixture keeps them.
fn redacted_chain(frame: &str) -> Value {
    let frame: Value = serde_json::from_str(frame).unwrap();
    let links: Vec<Value> = (0..)
        .map_while(|i| frame.get(format!("x-identity-auth-chain-{i}")))
        .map(|link| {
            let mut link: Value = serde_json::from_str(link.as_str().unwrap()).unwrap();
            if link["signature"].as_str().is_some_and(|s| !s.is_empty()) {
                link["signature"] = json!("<<REDACTED:signature>>");
            }
            link
        })
        .collect();
    json!({
        "links": links,
        "timestamp": frame["x-identity-timestamp"],
        "metadata": frame["x-identity-metadata"],
    })
}

/// Records the production answers. Run by hand, never in CI:
/// `CATALYST_SOCIAL_RPC_CAPTURE=1 cargo test -p catalyst-social --lib
/// upstream_reads::capture -- --ignored`.
#[tokio::test]
#[ignore = "reads the production service; needs CATALYST_SOCIAL_RPC_CAPTURE=1"]
async fn capture() {
    assert_eq!(
        std::env::var("CATALYST_SOCIAL_RPC_CAPTURE").as_deref(),
        Ok("1"),
        "the capture runs only with CATALYST_SOCIAL_RPC_CAPTURE=1"
    );
    let other = format!("{:#x}", PrivateKeySigner::random().address());
    let started = chrono::Utc::now();
    // The test build links both of rustls' providers, so it cannot pick one itself.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (socket, _) = tokio_tungstenite::connect_async(UPSTREAM)
        .await
        .expect("connects");
    let (signer, chain, _client, raw) = sign_in(socket, "/").await;
    let mut calls = Vec::new();
    for (read, request) in reads().iter().zip(requests(&other)) {
        tokio::time::sleep(PAUSE).await;
        let at = chrono::Utc::now();
        let response = raw.call(read.procedure, &request).await;
        println!("{}: {}", read.name, (read.response)(&response));
        calls.push(json!({
            "name": read.name,
            "procedure": read.procedure,
            "captured_at": at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "request": (read.request)(&request),
            "request_b64": B64.encode(&request),
            "response": (read.response)(&response),
            "response_b64": B64.encode(&response),
        }));
    }
    let fixture = json!({
        "description": "The eight read procedures of the social RPC, in ten calls (GetFriends and GetFriendshipStatus twice each) by a wallet created for the run (no data behind it). The status reads name the zero address and a second address created for the run. Signatures in the chain are redacted; the keys were held in memory only.",
        "captured_from": UPSTREAM,
        "started_at": started.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "signer": signer,
        "other": other,
        "auth_chain": chain,
        "calls": calls,
    });
    std::fs::write(
        fixture_path(),
        serde_json::to_string_pretty(&fixture).unwrap() + "\n",
    )
    .unwrap();
}

/// The recorded requests sent to the local `/social-rpc` as a wallet without
/// data: each answer is byte for byte what production answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reads_answer_as_production_does() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let fixture: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture_path()).unwrap()).unwrap();
    for named in [ZERO, fixture["other"].as_str().unwrap()] {
        sqlx::query("DELETE FROM social.user_settings WHERE address = $1")
            .bind(named)
            .execute(&pool)
            .await
            .unwrap();
    }
    // The server runs the private-voice expiry sweep against the scratch pool.
    let _turn = VOICE_TABLE.lock().await;
    let server = spawn_server(Some(pool), 5).await;
    let (_, _, _client, raw) = sign_in(connect(&server, "/social-rpc").await, "/").await;
    let calls = fixture["calls"].as_array().unwrap();
    let reads = reads();
    assert_eq!(calls.len(), reads.len());
    let mut differ = Vec::new();
    for (read, call) in reads.iter().zip(calls) {
        assert_eq!(call["name"], read.name);
        let request = B64.decode(call["request_b64"].as_str().unwrap()).unwrap();
        let recorded = B64.decode(call["response_b64"].as_str().unwrap()).unwrap();
        let local = raw.call(read.procedure, &request).await;
        if recorded != local {
            differ.push(format!(
                "{}:\n  production {}\n  local      {}",
                read.name,
                (read.response)(&recorded),
                (read.response)(&local)
            ));
        }
    }
    assert!(differ.is_empty(), "{}", differ.join("\n"));
    server.jobs.shutdown(Duration::from_secs(2)).await;
}
