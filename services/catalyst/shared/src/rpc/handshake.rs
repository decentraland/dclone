//! The WebSocket handshake: the first frame the explorer sends is the
//! signed-fetch header bag encoded as one JSON object (`x-identity-auth-chain-N`,
//! `x-identity-timestamp`, `x-identity-metadata`), signed over
//! `get:<path>:<timestamp>:<metadata>` in the crypto-middleware 6.x shape. The
//! verified SIGNER address becomes the connection's identity; nothing else
//! authenticates a procedure call.
//!
//! 6.x only: the pre-6.0.0 folded payload is never accepted on this socket,
//! so a re-cased metadata key cannot fold back into the scene metadata that
//! was actually signed (the explorer signs `{}` here, which folds to itself).
//! A chain the explorer signed on a scene's behalf (`signer:
//! decentraland-kernel-scene`, ADR-44) is refused before the signature is
//! checked, and a non-canonical spelling of the `signer` key or value is a
//! refusal rather than an absence.
//!
//! EIP-1654 (contract wallet) SIGNER links verify through the validator the
//! caller passes; with `None` they fail closed.

use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::auth::eip1654::Eip1654Validator;
use crate::auth::signed_fetch::{
    ChainShapeError, LegacyPayload, SignatureRefusal, SignedRequest, SkewError, UnsignedLinks,
    check_symmetric_skew, extract_auth_chain_with, verify_signature,
};
use crate::auth::{AuthChain, AuthError, MAX_AUTH_CHAIN_LINKS};
use crate::identity::Address;
use crate::signed_fetch::{AUTH_METADATA_HEADER, AUTH_TIMESTAMP_HEADER, ORIGINAL_PATH_HEADER};

pub use crate::auth::signed_fetch::is_refused_signer;

const DETAIL_MAX_CHARS: usize = 64;

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("invalid auth-chain envelope: not a JSON object")]
    EnvelopeNotObject,
    #[error("invalid auth-chain envelope: header values must be strings")]
    HeaderNotString,
    #[error("invalid auth-chain link {index}: {detail}")]
    MalformedChain { index: usize, detail: String },
    #[error("auth-chain shorter than 2 links")]
    InsufficientLinks,
    #[error("missing {0}")]
    MissingHeader(&'static str),
    #[error("signature older than {window_secs}s window: signed_at={signed_at} now={now}")]
    Expired {
        signed_at: i64,
        now: i64,
        window_secs: i64,
    },
    #[error("signature did not verify: {0}")]
    InvalidSignature(String),
    #[error("EIP-1654 chains need an RPC validator")]
    EipNotImplemented,
    #[error("catalyst unavailable: {0}")]
    CatalystUnavailable(String),
    #[error("requests from scenes are not allowed")]
    RefusedSigner,
}

fn truncate_detail(value: &str) -> String {
    if value.chars().count() > DETAIL_MAX_CHARS {
        let head: String = value.chars().take(DETAIL_MAX_CHARS).collect();
        format!("{head}...")
    } else {
        value.to_string()
    }
}

/// A header read under its exact lowercase name, as `verify()` in
/// `@dcl/crypto-middleware` 6.3.0 reads the frame (`dist/verify.js:72`,
/// `:321-322`): a re-cased key is never read, so it cannot shadow the
/// lowercase one.
fn obj_str<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Value::as_str)
}

fn frame_object(frame_json: &str) -> Result<Map<String, Value>, HandshakeError> {
    let value: Value =
        serde_json::from_str(frame_json).map_err(|e| HandshakeError::MalformedChain {
            index: 0,
            detail: format!("frame not JSON: {e}"),
        })?;
    match value {
        Value::Object(obj) => identity_headers(obj),
        _ => Err(HandshakeError::EnvelopeNotObject),
    }
}

const IDENTITY_PREFIX: &str = "x-identity-";

/// Every `x-identity-*` field, in any case, must be a string or an array of
/// strings before anything is verified (upstream's `parseAuthChainHeaders` in
/// `ws-handler.ts`); other fields are left alone. An array reads as its first
/// element and an empty one as absent, as `firstOf` in `@dcl/crypto-middleware`
/// 6.3.0 (`dist/verify.js`) reads a repeated header.
fn identity_headers(mut obj: Map<String, Value>) -> Result<Map<String, Value>, HandshakeError> {
    for (key, value) in obj.iter_mut() {
        if !key
            .get(..IDENTITY_PREFIX.len())
            .is_some_and(|p| p.eq_ignore_ascii_case(IDENTITY_PREFIX))
        {
            continue;
        }
        match value {
            Value::String(_) => {}
            Value::Array(items) if items.iter().all(Value::is_string) => {
                *value = items.first().cloned().unwrap_or(Value::Null);
            }
            _ => return Err(HandshakeError::HeaderNotString),
        }
    }
    Ok(obj)
}

pub fn extract_from_object(obj: &Map<String, Value>) -> Result<AuthChain, HandshakeError> {
    extract_auth_chain_with(|name| obj_str(obj, name), UnsignedLinks::Malformed).map_err(|e| {
        let (index, detail) = match e {
            ChainShapeError::InsufficientLinks => return HandshakeError::InsufficientLinks,
            ChainShapeError::Unparseable { index, detail } => (index, truncate_detail(&detail)),
            ChainShapeError::SignerNotFirst(index) => {
                (index, "SIGNER link at non-zero index".into())
            }
            ChainShapeError::FirstNotSigner => (0, "first link must be SIGNER".into()),
            ChainShapeError::MissingSignature(index) => (index, "missing signature".into()),
            ChainShapeError::TooLong => (
                MAX_AUTH_CHAIN_LINKS,
                format!("exceeds max length {MAX_AUTH_CHAIN_LINKS}"),
            ),
        };
        HandshakeError::MalformedChain { index, detail }
    })
}

/// Unparseable or non-object metadata is refused before anything is verified;
/// an explicit JSON `null` reads as an empty object.
fn parse_metadata(raw: &str) -> Result<Value, HandshakeError> {
    crate::auth::signed_fetch::parse_metadata(raw).ok_or_else(|| HandshakeError::MalformedChain {
        index: 0,
        detail: format!("invalid chain metadata: \"{}\"", truncate_detail(raw)),
    })
}

fn map_auth_error(err: AuthError) -> HandshakeError {
    match err {
        AuthError::MalformedChain(d) => HandshakeError::MalformedChain {
            index: 0,
            detail: d,
        },
        AuthError::MissingSignature { .. } => HandshakeError::MalformedChain {
            index: 0,
            detail: err.to_string(),
        },
        AuthError::RecoveryFailed(d) => HandshakeError::InvalidSignature(d),
        AuthError::SignerMismatch { .. } | AuthError::FinalAuthorityMismatch { .. } => {
            HandshakeError::InvalidSignature(err.to_string())
        }
        AuthError::EphemeralExpired {
            expiration_ms,
            now_ms,
        } => HandshakeError::Expired {
            signed_at: expiration_ms / 1000,
            now: now_ms / 1000,
            window_secs: 0,
        },
        AuthError::InvalidEphemeralPayload(d) => HandshakeError::MalformedChain {
            index: 0,
            detail: d,
        },
        AuthError::Eip1654NotImplemented => HandshakeError::EipNotImplemented,
        AuthError::Eip1654Rejected { .. } => HandshakeError::InvalidSignature(err.to_string()),
        AuthError::Eip1654ValidationFailed(d) => HandshakeError::CatalystUnavailable(d),
    }
}

/// The paths the frame may be signed over, in the order they are tried: the
/// first path that verifies settles it, and only an invalid-signature verdict
/// tries the next one. Upstream verifies `/` (the explorer connects to the
/// service's root); the old gateway forwarded the stripped public path as
/// `x-original-path`, and this node serves the socket under `/social-rpc`,
/// so a signature over any of the three is the same authority.
pub fn candidate_paths(headers: &HeaderMap, route: &str) -> Vec<String> {
    let mut paths = vec![handshake_path(headers, "/")];
    if !paths.contains(&route.to_string()) {
        paths.push(route.to_string());
    }
    paths
}

/// Verifies the first frame against every candidate path; the identity is the
/// lowercased SIGNER address.
pub async fn verify_handshake(
    frame_json: &str,
    method: &str,
    paths: &[String],
    expiration_secs: i64,
    now_secs: i64,
    validator: Option<&dyn Eip1654Validator>,
) -> Result<Address, HandshakeError> {
    let obj = frame_object(frame_json)?;
    let chain = extract_from_object(&obj)?;
    let timestamp = obj_str(&obj, AUTH_TIMESTAMP_HEADER)
        .ok_or(HandshakeError::MissingHeader(AUTH_TIMESTAMP_HEADER))?;
    check_symmetric_skew(timestamp, now_secs, expiration_secs).map_err(|e| match e {
        SkewError::InvalidTimestamp(timestamp) => HandshakeError::MalformedChain {
            index: 0,
            detail: format!("invalid timestamp: {}", truncate_detail(&timestamp)),
        },
        SkewError::Expired {
            signed_at,
            now,
            window_secs,
        } => HandshakeError::Expired {
            signed_at,
            now,
            window_secs,
        },
    })?;
    let metadata_raw = obj_str(&obj, AUTH_METADATA_HEADER).unwrap_or("{}");
    let metadata = parse_metadata(metadata_raw)?;
    if is_refused_signer(&metadata) {
        return Err(HandshakeError::RefusedSigner);
    }
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    let request = SignedRequest {
        method,
        paths: &paths,
        timestamp,
        metadata: metadata_raw,
    };
    verify_signature(
        &chain,
        &request,
        now_secs,
        LegacyPayload::Refused,
        validator,
    )
    .await
    .map_err(|refusal| match refusal {
        SignatureRefusal::Invalid(Some(e)) | SignatureRefusal::Settled(e) => map_auth_error(e),
        // `Refused` never reaches the legacy guard.
        SignatureRefusal::Invalid(None) | SignatureRefusal::LegacyKeysNotCanonical => {
            HandshakeError::InvalidSignature("no payload".into())
        }
    })?;
    Address::parse(&chain[0].payload).map_err(|e| HandshakeError::MalformedChain {
        index: 0,
        detail: format!("SIGNER payload is not an address: {e}"),
    })
}

/// The socket's own path; the only `x-original-path` the root fallback accepts.
pub const SOCKET_PATH: &str = "/social-rpc";

/// The public path a prefix-stripping proxy forwards as `x-original-path`,
/// accepted only when it is the socket path itself, with or without a
/// trailing slash (against the root fallback), or ends with the route
/// (against a nested fallback); anything else is the fallback itself. The
/// header is client-controlled, so any other single segment would let a
/// chain signed for another root-level path open the socket.
pub fn handshake_path(headers: &HeaderMap, fallback: &str) -> String {
    match headers
        .get(ORIGINAL_PATH_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(raw) => {
            let stripped = raw.split('?').next().unwrap_or(raw);
            let accept = if fallback == "/" {
                stripped.trim_end_matches('/') == SOCKET_PATH
            } else {
                stripped.ends_with(fallback)
            };
            if accept {
                stripped.to_string()
            } else {
                fallback.to_string()
            }
        }
        None => fallback.to_string(),
    }
}

/// The validator handle the state carries, as the `Option<&dyn>` the verifier takes.
pub fn validator_ref(
    validator: &Option<Arc<dyn Eip1654Validator>>,
) -> Option<&dyn Eip1654Validator> {
    validator.as_deref()
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use alloy::signers::{Signer as _, local::PrivateKeySigner};

    use crate::signed_fetch::build_payload_v6;

    pub const ROOT_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    pub const EPHEMERAL_KEY: &str =
        "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    /// A three-link chain (SIGNER, ECDSA_EPHEMERAL, ECDSA_SIGNED_ENTITY) as the
    /// explorer sends it: `signed_payload` is what the ephemeral key signs,
    /// `delivered_metadata` what the frame carries (they differ only in the
    /// re-cased-key attack shape). Returns `(lowercase signer, frame json)`.
    pub async fn frame_with(
        root_key: &str,
        ts_ms: i64,
        signed_payload: &str,
        delivered_metadata: &str,
    ) -> (String, String) {
        let root: PrivateKeySigner = root_key.parse().unwrap();
        let ephemeral: PrivateKeySigner = EPHEMERAL_KEY.parse().unwrap();
        frame_signed(&root, &ephemeral, ts_ms, signed_payload, delivered_metadata).await
    }

    /// [`frame_with`] over signers the caller holds, so a key need never be
    /// written out as text.
    pub async fn frame_signed(
        root: &PrivateKeySigner,
        ephemeral: &PrivateKeySigner,
        ts_ms: i64,
        signed_payload: &str,
        delivered_metadata: &str,
    ) -> (String, String) {
        let root_address = format!("{:#x}", root.address());
        let ephemeral_address = format!("{:#x}", ephemeral.address());
        let ephemeral_payload = format!(
            "Decentraland Login\nEphemeral address: {ephemeral_address}\nExpiration: 2099-01-01T00:00:00.000Z"
        );
        let ephemeral_sig = root
            .sign_message(ephemeral_payload.as_bytes())
            .await
            .unwrap();
        let entity_sig = ephemeral
            .sign_message(signed_payload.as_bytes())
            .await
            .unwrap();
        let frame = serde_json::json!({
            "x-identity-auth-chain-0": serde_json::json!({"type": "SIGNER", "payload": root_address, "signature": ""}).to_string(),
            "x-identity-auth-chain-1": serde_json::json!({"type": "ECDSA_EPHEMERAL", "payload": ephemeral_payload, "signature": ephemeral_sig.to_string()}).to_string(),
            "x-identity-auth-chain-2": serde_json::json!({"type": "ECDSA_SIGNED_ENTITY", "payload": signed_payload, "signature": entity_sig.to_string()}).to_string(),
            "x-identity-timestamp": ts_ms.to_string(),
            "x-identity-metadata": delivered_metadata
        });
        (root_address.to_lowercase(), frame.to_string())
    }

    /// A frame signed by `root_key` for `get <path>` at `ts_ms` with `{}` metadata.
    pub async fn frame_for(root_key: &str, path: &str, ts_ms: i64) -> (String, String) {
        let payload = build_payload_v6("get", path, &ts_ms.to_string(), "{}");
        frame_with(root_key, ts_ms, &payload, "{}").await
    }

    pub async fn frame(path: &str, ts_ms: i64) -> (String, String) {
        frame_for(ROOT_KEY, path, ts_ms).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::signed_fetch::{FIVE_MINUTES, SCENE_SIGNER, build_payload_v6};
    use axum::http::HeaderValue;
    use serde_json::json;

    fn root() -> Vec<String> {
        vec!["/".to_string()]
    }

    async fn verify(
        frame: &str,
        method: &str,
        paths: &[String],
        now_secs: i64,
    ) -> Result<Address, HandshakeError> {
        verify_handshake(frame, method, paths, FIVE_MINUTES, now_secs, None).await
    }

    #[tokio::test]
    async fn verify_handshake_accepts_valid_chain() {
        let now_secs = 1_700_000_000;
        let (expected_signer, frame) = frame("/", now_secs * 1000).await;
        let signer = verify(&frame, "get", &root(), now_secs)
            .await
            .expect("valid chain must verify");
        assert_eq!(signer.as_str(), expected_signer);
    }

    #[tokio::test]
    async fn verify_handshake_rejects_method_path_mismatch() {
        let now_secs = 1_700_000_000;
        let (_, frame) = frame("/", now_secs * 1000).await;
        let err = verify(&frame, "post", &root(), now_secs)
            .await
            .expect_err("wrong method must be rejected");
        assert!(
            matches!(err, HandshakeError::InvalidSignature(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn verify_handshake_rejects_expired() {
        let signed_secs = 1_700_000_000;
        let now_secs = signed_secs + 10 * 60;
        let (_, frame) = frame("/", signed_secs * 1000).await;
        let err = verify(&frame, "get", &root(), now_secs)
            .await
            .expect_err("expired chain must be rejected");
        assert!(matches!(err, HandshakeError::Expired { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn a_prefixed_path_verifies_against_its_own_candidate_only() {
        let now_secs = 1_700_000_000;
        let (expected_signer, frame) = frame("/social-rpc", now_secs * 1000).await;
        let signer = verify(&frame, "get", &["/social-rpc".to_string()], now_secs)
            .await
            .expect("matching path");
        assert_eq!(signer.as_str(), expected_signer);
        let err = verify(&frame, "get", &root(), now_secs)
            .await
            .expect_err("must not verify against /");
        assert!(
            matches!(err, HandshakeError::InvalidSignature(_)),
            "{err:?}"
        );
        // The route's candidates: the root first, then the route; a frame
        // signed over either verifies.
        let paths = candidate_paths(&HeaderMap::new(), "/social-rpc");
        assert_eq!(paths, vec!["/".to_string(), "/social-rpc".to_string()]);
        assert!(verify(&frame, "get", &paths, now_secs).await.is_ok());
        let (_, over_root) = super::test_support::frame("/", now_secs * 1000).await;
        assert!(verify(&over_root, "get", &paths, now_secs).await.is_ok());
    }

    #[test]
    fn handshake_path_accepts_stripped_prefix_and_defaults_to_root() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ORIGINAL_PATH_HEADER,
            HeaderValue::from_static("/social-rpc"),
        );
        assert_eq!(handshake_path(&headers, "/"), "/social-rpc");
        assert_eq!(
            candidate_paths(&headers, "/social-rpc"),
            vec!["/social-rpc".to_string()]
        );
        headers.insert(
            ORIGINAL_PATH_HEADER,
            HeaderValue::from_static("/social-rpc/?ts=1"),
        );
        assert_eq!(handshake_path(&headers, "/"), "/social-rpc/");
        assert_eq!(handshake_path(&HeaderMap::new(), "/"), "/");
    }

    #[test]
    fn handshake_path_rejects_any_original_but_the_socket_path_against_root() {
        let mut headers = HeaderMap::new();
        for forged in [
            "/v1/friends",
            "/social-rpc/deep",
            "/status",
            "/about",
            "/v1",
            "//social-rpc",
            "social-rpc",
        ] {
            headers.insert(ORIGINAL_PATH_HEADER, HeaderValue::from_static(forged));
            assert_eq!(handshake_path(&headers, "/"), "/", "{forged}");
        }
        headers.insert(
            ORIGINAL_PATH_HEADER,
            HeaderValue::from_static("/social-rpc//?x=1"),
        );
        assert_eq!(handshake_path(&headers, "/"), "/social-rpc//");
    }

    #[test]
    fn handshake_path_rejects_non_suffix_original() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ORIGINAL_PATH_HEADER,
            HeaderValue::from_static("/v1/friends"),
        );
        assert_eq!(
            handshake_path(&headers, "/v1/communities"),
            "/v1/communities"
        );
        headers.insert(
            ORIGINAL_PATH_HEADER,
            HeaderValue::from_static("/edge/v1/lists"),
        );
        assert_eq!(handshake_path(&headers, "/v1/lists"), "/edge/v1/lists");
    }

    #[tokio::test]
    async fn verify_handshake_rejects_malformed_envelope() {
        let err = verify("not json", "get", &root(), 0).await.unwrap_err();
        assert!(
            matches!(err, HandshakeError::MalformedChain { .. }),
            "{err:?}"
        );
        let err2 = verify("[]", "get", &root(), 0).await.unwrap_err();
        assert!(
            matches!(err2, HandshakeError::EnvelopeNotObject),
            "{err2:?}"
        );
    }

    #[tokio::test]
    async fn verify_handshake_rejects_short_chain_and_missing_timestamp() {
        let frame = json!({
            "x-identity-auth-chain-0": "{\"type\":\"SIGNER\",\"payload\":\"0xabc\",\"signature\":\"\"}",
            "x-identity-timestamp": "0",
            "x-identity-metadata": "{}"
        })
        .to_string();
        let err = verify(&frame, "get", &root(), 0).await.unwrap_err();
        assert!(matches!(err, HandshakeError::InsufficientLinks), "{err:?}");

        let now_secs = 1_700_000_000;
        let (_, signed) = super::test_support::frame("/", now_secs * 1000).await;
        let mut obj: Map<String, Value> = serde_json::from_str(&signed).unwrap();
        obj.remove("x-identity-timestamp");
        let err = verify(&Value::Object(obj).to_string(), "get", &root(), now_secs)
            .await
            .unwrap_err();
        assert!(matches!(err, HandshakeError::MissingHeader(_)), "{err:?}");
    }

    /// A signed frame at `now_secs` with `edit` applied to its object.
    async fn edited(now_secs: i64, edit: impl FnOnce(&mut Map<String, Value>)) -> String {
        let (_, signed) = super::test_support::frame("/", now_secs * 1000).await;
        let mut obj: Map<String, Value> = serde_json::from_str(&signed).unwrap();
        edit(&mut obj);
        Value::Object(obj).to_string()
    }

    /// Upstream's `parseAuthChainHeaders` (`ws-handler.ts:139-146`): an
    /// `x-identity-*` field in any case that is neither a string nor an array
    /// of strings is refused before anything is verified; metadata included,
    /// so a non-string one is never read as `{}`.
    #[tokio::test]
    async fn an_identity_field_that_is_not_a_string_is_refused_up_front() {
        let now_secs = 1_700_000_000;
        for (key, value) in [
            ("x-identity-timestamp", json!(now_secs * 1000)),
            ("x-identity-metadata", json!({})),
            ("x-identity-metadata", Value::Null),
            ("x-identity-metadata", json!(true)),
            ("x-identity-auth-chain-0", json!({ "type": "SIGNER" })),
            ("x-identity-auth-chain-1", json!(["{}", 1])),
            ("X-Identity-Anything", json!(1)),
        ] {
            let frame = edited(now_secs, |obj| {
                obj.insert(key.to_string(), value.clone());
            })
            .await;
            let err = verify(&frame, "get", &root(), now_secs).await.unwrap_err();
            assert!(
                matches!(err, HandshakeError::HeaderNotString),
                "{key} {value}: {err:?}"
            );
        }
        let other = edited(now_secs, |obj| {
            obj.insert("version".into(), json!({ "nested": [1] }));
        })
        .await;
        assert!(verify(&other, "get", &root(), now_secs).await.is_ok());
    }

    /// `firstOf` in `@dcl/crypto-middleware` 6.3.0 (`dist/verify.js`): an
    /// array of strings reads as its first element, whatever follows it, and
    /// an empty one as absent.
    #[tokio::test]
    async fn an_array_of_strings_reads_as_its_first_element() {
        let now_secs = 1_700_000_000;
        let (signer, _) = super::test_support::frame("/", now_secs * 1000).await;
        let wrapped = edited(now_secs, |obj| {
            for value in obj.values_mut() {
                *value = json!([value.clone(), "ignored"]);
            }
        })
        .await;
        let got = verify(&wrapped, "get", &root(), now_secs)
            .await
            .expect("every field as an array");
        assert_eq!(got.as_str(), signer);

        let no_timestamp = edited(now_secs, |obj| {
            obj.insert("x-identity-timestamp".into(), json!([]));
        })
        .await;
        let err = verify(&no_timestamp, "get", &root(), now_secs)
            .await
            .unwrap_err();
        assert!(matches!(err, HandshakeError::MissingHeader(_)), "{err:?}");

        let empty_metadata = edited(now_secs, |obj| {
            obj.insert("x-identity-metadata".into(), json!([]));
        })
        .await;
        let got = verify(&empty_metadata, "get", &root(), now_secs)
            .await
            .expect("an empty metadata array reads as absent");
        assert_eq!(got.as_str(), signer);
    }

    /// `verify()` reads the exact lowercase keys (`dist/verify.js:72`,
    /// `:321-322`): a re-cased duplicate passes the up-front type check and is
    /// then never read, so it cannot shadow the lowercase header, and a
    /// re-cased header alone is absent.
    #[tokio::test]
    async fn a_re_cased_identity_header_is_never_read() {
        let now_secs = 1_700_000_000;
        let (signer, _) = super::test_support::frame("/", now_secs * 1000).await;
        for (key, value) in [
            ("X-Identity-Timestamp", json!([])),
            ("X-Identity-Timestamp", json!("0")),
            ("X-Identity-Metadata", json!([])),
            (
                "X-Identity-Metadata",
                json!(r#"{"signer":"decentraland-kernel-scene"}"#),
            ),
            ("X-Identity-Auth-Chain-0", json!("not a link")),
        ] {
            let frame = edited(now_secs, |obj| {
                obj.insert(key.to_string(), value.clone());
            })
            .await;
            let got = verify(&frame, "get", &root(), now_secs)
                .await
                .unwrap_or_else(|e| panic!("{key} {value}: {e:?}"));
            assert_eq!(got.as_str(), signer);
        }
        let re_cased = edited(now_secs, |obj| {
            let ts = obj.remove("x-identity-timestamp").unwrap();
            obj.insert("X-Identity-Timestamp".into(), ts);
        })
        .await;
        let err = verify(&re_cased, "get", &root(), now_secs)
            .await
            .unwrap_err();
        assert!(matches!(err, HandshakeError::MissingHeader(_)), "{err:?}");
    }

    #[test]
    fn scene_signer_gate_matches_upstream_reject_if_signer() {
        assert!(is_refused_signer(&json!({ "signer": SCENE_SIGNER })));
        assert!(is_refused_signer(
            &json!({ "signer": "Decentraland-Kernel-Scene" })
        ));
        assert!(is_refused_signer(
            &json!({ "signer": "  decentraland-kernel-scene\t" })
        ));
        assert!(is_refused_signer(&json!({ "signer": "DCL:Explorer" })));
        assert!(is_refused_signer(
            &json!({ "signer": format!("\u{FEFF}{SCENE_SIGNER}") })
        ));
        assert!(is_refused_signer(&json!({ "signer": 42 })));
        assert!(is_refused_signer(&json!({ "signer": null })));
        assert!(is_refused_signer(&json!({ "signer": [SCENE_SIGNER] })));
        assert!(is_refused_signer(&json!({ "Signer": SCENE_SIGNER })));
        assert!(is_refused_signer(
            &json!({ "signer": "dcl:explorer", "Signer": SCENE_SIGNER })
        ));

        assert!(!is_refused_signer(&json!({ "signer": "dcl:explorer" })));
        assert!(!is_refused_signer(&json!({ "signer": "" })));
        assert!(!is_refused_signer(&json!({})));
        assert!(!is_refused_signer(&Value::Null));
        assert!(!is_refused_signer(&json!({ "sceneId": "BafkreiAbcDef" })));
    }

    const SCENE_METADATA: &str = r#"{"signer":"decentraland-kernel-scene"}"#;
    const RECASED_KEY: &str = r#"{"Signer":"decentraland-kernel-scene"}"#;

    #[tokio::test]
    async fn a_scene_signed_frame_is_refused_before_the_signature_is_checked() {
        let now_secs = 1_700_000_000;
        let ts_ms = now_secs * 1000;
        for delivered in [SCENE_METADATA, RECASED_KEY] {
            // Signed as delivered: the signature alone would accept it.
            let signed = build_payload_v6("get", "/", &ts_ms.to_string(), delivered);
            let (_, frame) = frame_with(ROOT_KEY, ts_ms, &signed, delivered).await;
            let err = verify(&frame, "get", &root(), now_secs).await.unwrap_err();
            assert!(
                matches!(err, HandshakeError::RefusedSigner),
                "{delivered}: {err:?}"
            );
        }
        // The re-cased-key attack shape: signed over the scene metadata,
        // delivered under the re-spelled key.
        let signed = build_payload_v6("get", "/", &ts_ms.to_string(), SCENE_METADATA);
        let (_, frame) = frame_with(ROOT_KEY, ts_ms, &signed, RECASED_KEY).await;
        assert!(matches!(
            verify(&frame, "get", &root(), now_secs).await.unwrap_err(),
            HandshakeError::RefusedSigner
        ));
    }

    #[tokio::test]
    async fn the_socket_is_6x_only_for_metadata_that_does_not_fold_to_itself() {
        let now_secs = 1_700_000_000;
        let ts_ms = now_secs * 1000;
        let cased = r#"{"signer":"dcl:explorer","sceneId":"bafkreiAbC123"}"#;
        let v6 = build_payload_v6("get", "/", &ts_ms.to_string(), cased);
        let (_, frame) = frame_with(ROOT_KEY, ts_ms, &v6, cased).await;
        assert!(verify(&frame, "get", &root(), now_secs).await.is_ok());
        let legacy =
            crate::signed_fetch::build_legacy_payload("get", "/", &ts_ms.to_string(), cased);
        let (_, frame) = frame_with(ROOT_KEY, ts_ms, &legacy, cased).await;
        let err = verify(&frame, "get", &root(), now_secs).await.unwrap_err();
        assert!(
            matches!(err, HandshakeError::InvalidSignature(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_metadata_field_this_service_never_reads_is_still_bound_to_the_signature() {
        let now_secs = 1_700_000_000;
        let ts_ms = now_secs * 1000;
        let signed = build_payload_v6(
            "get",
            "/",
            &ts_ms.to_string(),
            r#"{"sceneId":"bafkreiabcdef"}"#,
        );
        let (_, frame) =
            frame_with(ROOT_KEY, ts_ms, &signed, r#"{"sceneId":"BAFKREIABCDEF"}"#).await;
        let err = verify(&frame, "get", &root(), now_secs).await.unwrap_err();
        assert!(
            matches!(err, HandshakeError::InvalidSignature(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn unparseable_metadata_is_refused_as_malformed() {
        let now_secs = 1_700_000_000;
        let ts_ms = now_secs * 1000;
        for raw in ["not json", "[]", "42"] {
            let signed = build_payload_v6("get", "/", &ts_ms.to_string(), raw);
            let (_, frame) = frame_with(ROOT_KEY, ts_ms, &signed, raw).await;
            let err = verify(&frame, "get", &root(), now_secs).await.unwrap_err();
            assert!(
                matches!(err, HandshakeError::MalformedChain { .. }),
                "{raw}: {err:?}"
            );
        }
    }
}
