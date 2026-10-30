//! The rpc host's HTTP routes against the upstream uWS server
//! (`uws.routes.ts` at `fb3dcb8`) and its captures
//! (`tests/fixtures/conformance/social/social-rpc/`).

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;
use crate::test_fixtures;

const CAPTURES: [&str; 5] = [
    "status",
    "health-live",
    "privacy-settings-unknown",
    "privacy-settings-bad-address",
    "unknown-path-404",
];

fn app(pool: PgPool) -> Router {
    let settings = Settings {
        commit_hash: Some("cafe".into()),
        ..Default::default()
    };
    super::super::router(Arc::new(State::new(
        pool,
        settings,
        None,
        Arc::new(crate::comms::gatekeeper::voice::rooms::NoVoiceRuntime),
    )))
}

async fn call(app: &Router, method: Method, path: &str) -> (StatusCode, Option<String>, String) {
    call_with(app, Request::builder().method(method).uri(path)).await
}

/// One request as `serve` sees it: the node trims a trailing slash first.
async fn call_with(
    app: &Router,
    request: axum::http::request::Builder,
) -> (StatusCode, Option<String>, String) {
    let response = tower_http::normalize_path::NormalizePath::trim_trailing_slash(app.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let origin = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .map(|v| v.to_str().unwrap().to_string());
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, origin, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn the_rpc_host_answers_as_its_captures() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let app = app(pool);
    for name in CAPTURES {
        let fx = test_fixtures::fixture("social-rpc", name);
        let notes = test_fixtures::replay(&app, "/social-rpc", name, &fx).await;
        assert!(notes.is_empty(), "{name}: {notes:#?}");
    }
}

#[tokio::test]
async fn the_rpc_host_routes_follow_upstream_and_keep_the_local_ones() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let app = app(pool.clone());

    let (status, origin, body) = call(&app, Method::GET, "/social-rpc/status").await;
    assert_eq!((status, origin.as_deref()), (StatusCode::OK, Some("*")));
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["commitHash"], "cafe");
    assert!(
        v["currentTime"].as_u64().unwrap() > 1_700_000_000_000,
        "{v}"
    );

    // any method on the liveness check; every other method or path is the JSON 404
    for method in [
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::PATCH,
    ] {
        let (status, origin, body) = call(&app, method.clone(), "/social-rpc/health/live").await;
        assert_eq!(
            (status, origin.as_deref(), body.as_str()),
            (StatusCode::OK, Some("*"), "alive"),
            "{method}"
        );
    }
    let (status, origin, _) = call(&app, Method::HEAD, "/social-rpc/health/live").await;
    assert_eq!(
        (status, origin.as_deref()),
        (StatusCode::OK, Some("*")),
        "HEAD"
    );
    let unknown = format!("/social-rpc/v1/users/{}/privacy-settings", addr("pr01"));
    for (method, path) in [
        (Method::POST, "/social-rpc/status"),
        (Method::DELETE, unknown.as_str()),
        (Method::GET, "/social-rpc/v1/does-not-exist"),
        (Method::POST, "/social-rpc/v1/does-not-exist"),
        (Method::GET, "/social-rpc/"),
        // upstream's `app.get` is GET alone and `app.ws('/')` upgrades alone
        (Method::HEAD, "/social-rpc/status"),
        (Method::HEAD, unknown.as_str()),
        (Method::GET, "/social-rpc"),
        (Method::POST, "/social-rpc"),
    ] {
        let (status, origin, body) = call(&app, method.clone(), path).await;
        assert_eq!(
            (status, origin.as_deref()),
            (StatusCode::NOT_FOUND, Some("*")),
            "{method} {path}"
        );
        if method != Method::HEAD {
            assert_eq!(
                serde_json::from_str::<Value>(&body).unwrap(),
                json!({ "error": "Not Found" }),
                "{method} {path}"
            );
        }
    }

    // the explicit routes keep precedence over the catch-all
    let (status, _, body) = call(&app, Method::GET, "/social-rpc/info").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["ws"],
        "/social-rpc"
    );
    let (status, _, body) = call(&app, Method::GET, "/social-rpc/health").await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::OK, r#"{"healthy":true}"#)
    );
    let (status, origin, _) = call(&app, Method::GET, "/admin/social/presence").await;
    assert_ne!(
        status,
        StatusCode::NOT_FOUND,
        "the admin nest is not the catch-all"
    );
    assert_eq!(
        origin, None,
        "the admin nest is outside the rpc host's layer"
    );

    // privacy-settings: stored or `all`, the address lowercased and never trimmed
    let wallet = addr("pr02");
    let path = |a: &str| format!("/social-rpc/v1/users/{a}/privacy-settings");
    let read = |a: String| {
        let app = app.clone();
        async move {
            let (status, origin, body) = call(&app, Method::GET, &a).await;
            assert_eq!(origin.as_deref(), Some("*"), "{a}");
            (status, serde_json::from_str::<Value>(&body).unwrap())
        }
    };
    assert_eq!(
        read(path(&wallet)).await,
        (StatusCode::OK, json!({ "private_messages_privacy": "all" }))
    );
    Db::new(pool)
        .upsert_social_settings(&wallet, Some("only_friends"), None, None)
        .await
        .unwrap();
    assert_eq!(
        read(path(&wallet.to_uppercase().replacen("0X", "0x", 1))).await,
        (
            StatusCode::OK,
            json!({ "private_messages_privacy": "only_friends" })
        )
    );
    for bad in [
        format!("%20{wallet}"),
        "0x1234".into(),
        "not-an-address".into(),
    ] {
        assert_eq!(
            read(path(&bad)).await,
            (
                StatusCode::BAD_REQUEST,
                json!({ "error": "Invalid address" })
            ),
            "{bad}"
        );
    }
}

/// `writes::guard` wraps the rpc router: with writes off it answers a non-GET
/// before any rpc route, the liveness check and the catch-all included (its
/// 503, without the host's `Access-Control-Allow-Origin`); reads are untouched.
#[tokio::test]
async fn with_writes_off_the_guard_answers_non_gets_first() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let app = crate::writes::guard(app(pool), false);
    for path in ["/social-rpc/health/live", "/social-rpc/v1/does-not-exist"] {
        let (status, origin, body) = call(&app, Method::POST, path).await;
        assert_eq!(
            (status, origin),
            (StatusCode::SERVICE_UNAVAILABLE, None),
            "{path}"
        );
        assert!(
            body.contains("SOCIAL_WRITES_ENABLED=false"),
            "{path}: {body}"
        );
    }
    let (status, origin, body) = call(&app, Method::GET, "/social-rpc/health/live").await;
    assert_eq!(
        (status, origin.as_deref(), body.as_str()),
        (StatusCode::OK, Some("*"), "alive")
    );
}

/// A failed read answers upstream's `{error}` 500 without the database's detail.
#[tokio::test]
async fn a_failed_privacy_read_names_no_database_detail() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://nobody@127.0.0.1:1/nowhere")
        .unwrap();
    let path = format!("/social-rpc/v1/users/{}/privacy-settings", addr("pr03"));
    let (status, origin, body) = call(&app(pool), Method::GET, &path).await;
    assert_eq!(
        (status, origin.as_deref()),
        (StatusCode::INTERNAL_SERVER_ERROR, Some("*"))
    );
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "error": "Internal Server Error" })
    );
}

/// Upstream's `app.ws('/')` serves upgrades alone and its `ANY /*` answers the
/// rest. A request with no `Sec-WebSocket-Key` is not an upgrade and gets the
/// JSON 404; a broken upgrade (here version 12) keeps axum's own 400, since
/// what uWS answers it is not in the reference's source (`docs/compatibility.md`).
#[tokio::test]
async fn only_a_request_that_is_not_an_upgrade_gets_the_json_404() {
    let app = app(sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://nobody@127.0.0.1:1/nowhere")
        .unwrap());
    let (status, origin, body) = call_with(
        &app,
        Request::builder()
            .uri("/social-rpc")
            .header(header::CONNECTION, "upgrade")
            .header(header::UPGRADE, "websocket"),
    )
    .await;
    assert_eq!(
        (status, origin.as_deref()),
        (StatusCode::NOT_FOUND, Some("*"))
    );
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "error": "Not Found" })
    );

    let (status, _, body) = call_with(
        &app,
        Request::builder()
            .uri("/social-rpc")
            .header(header::CONNECTION, "upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "12")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ=="),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(!body.contains("Not Found"), "{body}");
}
