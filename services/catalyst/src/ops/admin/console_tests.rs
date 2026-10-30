//! The console's module calls (`console`): for each route family, no
//! session is the old 403, a session reaches the module, the call lands one
//! `ops.admin_audit` row naming the wallet, and the domain's write switch
//! refuses its writes with the domain's own 503 and no row. DB-backed on
//! the process's scratch database (`db::scratch`; skipped without
//! `CATALYST_TEST_DATABASE_URL`), every row scoped by a random wallet.

use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use axum::Router;
use axum::http::{StatusCode, header};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::console::{CONSOLE_ROUTES, Domains};
use super::tests::{SECRET, audit_rows, cleanup, lazy_pool, send, session_cookie, wallet};
use super::{RawSettings, Settings, State, router};
use crate::content::{ContentState, Settings as ContentSettings};
use crate::http::Outbound;
use crate::jobs::Jobs;
use crate::storage::Storage;

const TOKEN: &str = "console-test-bearer";

async fn storage() -> Arc<Storage> {
    let tmp = std::env::temp_dir().join(format!(
        "catalyst-console-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    Arc::new(Storage::from_config(&tmp).await.unwrap())
}

async fn app(pool: PgPool, ops_writes: bool, domains: Domains, admin: &str) -> Router {
    let content = Arc::new(ContentState::new(
        pool.clone(),
        ContentSettings::for_tests(),
        storage().await,
    ));
    let settings = Settings::parse(RawSettings {
        session_secret: Some(SECRET.to_string()),
        admin_addresses: admin.to_string(),
        session_ttl_secs: 0,
        cookie_insecure: true,
        eth_network: "mainnet".to_string(),
        writes_enabled: ops_writes,
    });
    router(Arc::new(
        State::new(pool, settings, content, Arc::new(Jobs::new()), None).with_domains(domains),
    ))
}

/// A signed-in console over `domains`, its wallet and cookie.
async fn console(pool: &PgPool, ops_writes: bool, domains: Domains) -> (Router, String, String) {
    let admin = PrivateKeySigner::random();
    let actor = wallet(&admin);
    let app = app(pool.clone(), ops_writes, domains, &actor).await;
    let cookie = session_cookie(&app, &admin).await;
    (app, actor, cookie)
}

async fn post(app: &Router, cookie: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let (status, headers, body) = send(app, "POST", path, &[("cookie", cookie)], Some(body)).await;
    let refusal = body["body"]["message"]
        .as_str()
        .is_some_and(|m| m.contains("_WRITES_ENABLED=false"));
    if status == StatusCode::SERVICE_UNAVAILABLE && refusal {
        assert!(
            headers.get(header::RETRY_AFTER).is_some(),
            "a refused write carries Retry-After: {body}"
        );
    }
    (status, body)
}

/// The console's own rows for `actor` (domain, action, target, result), newest first.
async fn console_rows(pool: &PgPool, actor: &str) -> Vec<(String, String, Option<String>, String)> {
    audit_rows(pool, actor)
        .await
        .into_iter()
        .map(|r| (r.domain, r.action, r.target, r.result))
        .collect()
}

fn random_address() -> String {
    wallet(&PrivateKeySigner::random())
}

fn assert_refused(status: StatusCode, body: &Value, switch: &str) {
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["ok"], false, "{body}");
    assert_eq!(body["status"], 503, "{body}");
    let message = body["body"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&format!("{switch}=false")),
        "the domain's own 503 names {switch}: {body}"
    );
}

#[tokio::test]
async fn every_console_route_is_403_without_a_session_and_502_without_its_module() {
    let admin = PrivateKeySigner::random();
    let app = app(lazy_pool(), true, Domains::default(), &wallet(&admin)).await;
    assert_eq!(CONSOLE_ROUTES.len(), 91);
    for path in CONSOLE_ROUTES {
        let (status, _, body) = send(&app, "POST", path, &[], Some(json!({}))).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{path} without a cookie: {body}"
        );
    }
    let cookie = session_cookie(&app, &admin).await;
    let (status, _, _) = send(
        &app,
        "POST",
        CONSOLE_ROUTES[0],
        &[("cookie", &cookie), ("origin", "https://evil.test")],
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "cross-origin");
    // A session over a node that runs none of the modules: the old
    // unconfigured-service answer, or the input check that comes first.
    let full = json!({
        "address": random_address(), "wallet": random_address(), "id": "x", "badge_id": "b", "world_name": "w.dcl.eth",
        "scene": "s", "name": "s", "image_id": "i", "collection_id": "c", "item_id": "i", "entity_id": "e", "network": "n",
        "token": "mana", "vs": "usd", "fingerprint": "f", "sql": "select 1", "position": "1,1", "enabled": "true",
        "place_id": "p", "world_id": "w", "event_id": "e", "highlighted": "true", "content_rating": "T"
    });
    let create =
        json!({ "name": "n", "start_at": "2030-01-01T00:00:00Z", "duration": 1, "x": 0, "y": 0 });
    for path in CONSOLE_ROUTES {
        let body = if *path == "/admin/api/events/create" {
            create.clone()
        } else {
            full.clone()
        };
        let (status, _, body) = send(&app, "POST", path, &[("cookie", &cookie)], Some(body)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{path}: {body}");
        assert_eq!(body["body"]["error"], "not-configured", "{path}: {body}");
    }
}

#[tokio::test]
async fn telemetry_calls_run_the_modules_handlers_behind_the_ops_switch() {
    let _telemetry_tables = crate::ops::telemetry::tests::LOCK.lock().await;
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let telemetry = |writes: bool, token: Option<&str>| {
        let pool = pool.clone();
        let token = token.map(str::to_string);
        async move {
            let settings = crate::ops::telemetry::Settings {
                writes_enabled: writes,
                admin_token: token,
                ..crate::ops::telemetry::Settings::for_tests()
            };
            Arc::new(
                crate::ops::telemetry::State::new(pool, Outbound::for_tests(), settings)
                    .await
                    .unwrap(),
            )
        }
    };
    let fingerprint = format!("console-{}", uuid::Uuid::new_v4().simple());

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            telemetry: Some(telemetry(false, Some(TOKEN)).await),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/telemetry/issue-state",
        json!({ "fingerprint": fingerprint, "status": "resolve" }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");
    assert!(
        console_rows(&pool, &actor).await.is_empty(),
        "a refused write is not an operator action"
    );
    // The SQL console is a read: the gate lets it through, the module answers
    // its own 503 without a reader role, and the call is recorded.
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/telemetry/sql",
        json!({ "sql": "select 1" }),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::SERVICE_UNAVAILABLE,
        "the module's answer (503 without a reader role): {body}"
    );
    let result = if status == StatusCode::OK {
        "ok"
    } else {
        "error"
    };
    assert_eq!(
        console_rows(&pool, &actor).await,
        vec![(
            "telemetry".into(),
            "telemetry.sql".into(),
            None,
            result.into()
        )]
    );
    cleanup(&pool, &actor).await;

    let (app, _, cookie) = console(
        &pool,
        true,
        Domains {
            telemetry: Some(telemetry(true, None).await),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/telemetry/issue-state",
        json!({ "fingerprint": fingerprint }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("telemetry-admin-token-not-configured")
        ),
        "{body}"
    );

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            telemetry: Some(telemetry(true, Some(TOKEN)).await),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/telemetry/issue-state",
        json!({ "fingerprint": fingerprint, "status": "resolve", "note": "" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (body["ok"].as_bool(), body["body"]["status"].as_str()),
        (Some(true), Some("resolved")),
        "the old form's verb maps to the stored state"
    );
    let stored: String =
        sqlx::query_scalar("SELECT status FROM telemetry.issue_state WHERE fingerprint = $1")
            .bind(&fingerprint)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, "resolved");
    assert_eq!(
        console_rows(&pool, &actor).await,
        vec![(
            "telemetry".into(),
            "telemetry.issue-state".into(),
            Some(fingerprint.clone()),
            "ok".into()
        )]
    );
    let own: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.admin_audit WHERE domain = 'telemetry' AND action = 'issue.state' AND target = $1")
        .bind(&fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        own, 1,
        "the module's own row, in the statement that wrote the state"
    );

    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/telemetry/audit",
        json!({ "fingerprint": fingerprint, "limit": "5" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = post(&app, &cookie, "/admin/api/telemetry/ingest", json!({})).await;
    assert_eq!(status, StatusCode::OK, "the ingest report: {body}");
    let rows = console_rows(&pool, &actor).await;
    assert_eq!(
        rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
        vec![
            "telemetry.ingest",
            "telemetry.audit",
            "telemetry.issue-state"
        ]
    );

    sqlx::query("DELETE FROM telemetry.issue_state WHERE fingerprint = $1")
        .bind(&fingerprint)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM ops.admin_audit WHERE domain = 'telemetry' AND target = $1")
        .bind(&fingerprint)
        .execute(&pool)
        .await
        .unwrap();
    cleanup(&pool, &actor).await;
}

#[tokio::test]
async fn credits_calls_record_the_wallet_in_the_ledgers_own_audit_row() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let credits = |writes: bool| {
        let settings = crate::shop::credits::Settings {
            writes_enabled: writes,
            admin_token: Some("credits-tok".into()),
            ..crate::shop::credits::Settings::for_tests()
        };
        Arc::new(crate::shop::credits::State::new(
            pool.clone(),
            Outbound::for_tests(),
            settings,
            crate::shop::credits::Sources {
                market: None,
                price: None,
                economy: None,
            },
            None,
        ))
    };
    let user = random_address();

    // no credits operator credential: the old console's 403, whatever the switches say
    let tokenless = Arc::new(crate::shop::credits::State::new(
        pool.clone(),
        Outbound::for_tests(),
        crate::shop::credits::Settings {
            writes_enabled: true,
            admin_token: None,
            ..crate::shop::credits::Settings::for_tests()
        },
        crate::shop::credits::Sources {
            market: None,
            price: None,
            economy: None,
        },
        None,
    ));
    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            credits: Some(tokenless),
            ..Default::default()
        },
    )
    .await;
    for path in [
        "/admin/api/credits/grant",
        "/admin/api/credits/revoke",
        "/admin/api/credits/user-block",
    ] {
        let (status, body) = post(
            &app,
            &cookie,
            path,
            json!({ "address": user, "amount": "5", "blocked": "true" }),
        )
        .await;
        assert_eq!(
            (status, body["body"]["error"].as_str()),
            (
                StatusCode::FORBIDDEN,
                Some("credits-admin-token-not-configured")
            ),
            "{path}: {body}"
        );
    }
    let minted: i64 = sqlx::query_scalar("SELECT count(*) FROM credits.ledger WHERE address = $1")
        .bind(&user)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(minted, 0);
    cleanup(&pool, &actor).await;

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            credits: Some(credits(false)),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/credits/grant",
        json!({ "address": user, "amount": "5" }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");
    assert!(console_rows(&pool, &actor).await.is_empty());

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            credits: Some(credits(true)),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/credits/grant",
        json!({ "address": user, "amount": "5", "reason": "console test" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (
            body["body"]["address"].as_str(),
            body["body"]["applied"].as_str()
        ),
        (Some(user.as_str()), Some("5")),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/credits/user-block",
        json!({ "address": user, "blocked": "true" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["blocked"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/credits/revoke",
        json!({ "address": user, "amount": "0" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the credits validator refuses a zero amount: {body}"
    );

    let rows = console_rows(&pool, &actor).await;
    let console_only: Vec<_> = rows
        .iter()
        .filter(|r| r.1.starts_with("credits."))
        .map(|r| (r.1.as_str(), r.3.as_str()))
        .collect();
    assert_eq!(
        console_only,
        vec![
            ("credits.revoke", "error"),
            ("credits.user.block", "ok"),
            ("credits.grant", "ok")
        ]
    );
    assert!(
        rows.iter()
            .all(|r| r.0 == "credits" && r.2.as_deref() == Some(user.as_str())),
        "{rows:?}"
    );
    let linked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credits.ledger l JOIN ops.admin_audit a ON a.id = l.admin_audit_id WHERE l.address = $1 AND a.actor = $2",
    )
    .bind(&user)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        linked >= 1,
        "the grant's ledger row links an audit row naming the console wallet"
    );
    cleanup(&pool, &actor).await;
}

#[tokio::test]
async fn price_overrides_take_the_price_bearer_and_the_data_switch() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let price = |writes: bool, token: Option<&str>| {
        let settings = crate::price::Settings {
            writes_enabled: writes,
            admin_token: token.map(str::to_string),
            ..crate::price::Settings::for_tests()
        };
        Arc::new(crate::price::State::new(
            pool.clone(),
            Outbound::for_tests(),
            settings,
        ))
    };
    let token = format!("ctest{}", &uuid::Uuid::new_v4().simple().to_string()[..10]);
    let set = json!({ "token": token, "vs": "usd", "value": "0.25", "note": "console" });

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            price: Some(price(false, Some(TOKEN))),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(&app, &cookie, "/admin/api/price/override-set", set.clone()).await;
    assert_refused(status, &body, "DATA_WRITES_ENABLED");
    let (app, _, cookie) = console(
        &pool,
        true,
        Domains {
            price: Some(price(true, None)),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(&app, &cookie, "/admin/api/price/override-set", set.clone()).await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("price-admin-token-not-configured")
        )
    );
    assert!(console_rows(&pool, &actor).await.is_empty());

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            price: Some(price(true, Some(TOKEN))),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(&app, &cookie, "/admin/api/price/override-set", set).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let recorded: Option<String> =
        sqlx::query_scalar("SELECT updated_by FROM economy.price_overrides WHERE token_id = $1")
            .bind(&token)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        recorded.as_deref(),
        Some(actor.as_str()),
        "the override names the console wallet (x-catalyrst-admin) as its operator"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/price/override-delete",
        json!({ "token": token, "vs": "usd" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["removed"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/price/override-delete",
        json!({ "token": "../x", "vs": "usd" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let target = format!("{token}/usd");
    assert_eq!(
        console_rows(&pool, &actor).await,
        vec![
            (
                "economy".into(),
                "price.override.delete".into(),
                Some(target.clone()),
                "ok".into()
            ),
            (
                "economy".into(),
                "price.override.set".into(),
                Some(target),
                "ok".into()
            ),
        ]
    );
    cleanup(&pool, &actor).await;
}

async fn social_domains(pool: &PgPool, writes: bool, token: Option<&str>) -> Domains {
    let token = token.map(str::to_string);
    let comms = crate::comms::gatekeeper::Settings {
        moderator_token: token.clone(),
        ..crate::comms::gatekeeper::Settings::for_tests()
    };
    let comms = Arc::new(crate::comms::gatekeeper::CommsState::new(
        pool.clone(),
        Outbound::for_tests(),
        comms,
        None,
        None,
        Arc::new(crate::worlds::policy::Policy::new(
            pool.clone(),
            crate::worlds::policy::PersonalWorldsPolicy::disabled(),
            None,
        )),
        Arc::new(crate::worlds::rooms::SceneResolver::new(pool.clone())),
        Arc::new(crate::comms::presence::Presence::new()),
        Arc::new(crate::comms::interfaces::test_support::StubDirectory::default()),
    ));
    let rpc = crate::social::rpc::Settings {
        admin_token: token.clone(),
        ..Default::default()
    };
    let rpc = Arc::new(crate::social::rpc::State::new(
        pool.clone(),
        rpc,
        None,
        Arc::new(crate::comms::gatekeeper::voice::rooms::NoVoiceRuntime),
    ));
    let communities = crate::social::communities::Settings {
        admin_token: token.clone(),
        ..crate::social::communities::Settings::for_tests()
    };
    let communities = Arc::new(crate::social::communities::State::new(
        pool.clone(),
        storage().await,
        communities,
        None,
        &Outbound::for_tests(),
        crate::social::communities::State::explore_for_tests(&pool),
    ));
    let notifications = crate::social::notifications::Settings {
        admin_token: token.clone(),
        ..Default::default()
    };
    let notifications = Arc::new(crate::social::notifications::State::new(
        pool.clone(),
        notifications,
        Outbound::for_tests(),
        None,
        None,
    ));
    let badges = crate::social::badges::Settings {
        admin_token: token,
        ..Default::default()
    };
    let badges = Arc::new(crate::social::badges::State::new(pool.clone(), badges));
    Domains {
        comms: Some(comms),
        social_rpc: Some(rpc),
        communities: Some(communities),
        notifications: Some(notifications),
        badges: Some(badges),
        social_writes_enabled: writes,
        ..Default::default()
    }
}

#[tokio::test]
async fn social_calls_follow_the_social_gate_and_name_the_wallet_as_moderator() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let user = random_address();

    // SOCIAL_WRITES_ENABLED off: every durable write refused, the two
    // socket-level RPC controls and the reads still answer.
    let (app, actor, cookie) =
        console(&pool, true, social_domains(&pool, false, Some(TOKEN)).await).await;
    for (path, body) in [
        (
            "/admin/api/social/user-ban",
            json!({ "address": user, "reason": "spam" }),
        ),
        ("/admin/api/social/user-unban", json!({ "address": user })),
        (
            "/admin/api/social/user-warning",
            json!({ "address": user, "reason": "spam" }),
        ),
        (
            "/admin/api/social-rpc/reset-settings",
            json!({ "address": user }),
        ),
        (
            "/admin/api/communities/suspend",
            json!({ "id": uuid::Uuid::new_v4().to_string() }),
        ),
        (
            "/admin/api/communities/unsuspend",
            json!({ "id": uuid::Uuid::new_v4().to_string() }),
        ),
        (
            "/admin/api/notifications/broadcast",
            json!({ "type": "badge_granted", "addresses": user }),
        ),
        (
            "/admin/api/badges/grant",
            json!({ "address": user, "badge_id": "none" }),
        ),
        (
            "/admin/api/badges/revoke",
            json!({ "address": user, "badge_id": "none" }),
        ),
    ] {
        let (status, body) = post(&app, &cookie, path, body).await;
        assert_refused(status, &body, "SOCIAL_WRITES_ENABLED");
    }
    assert!(console_rows(&pool, &actor).await.is_empty());
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social-rpc/disconnect",
        json!({ "address": user }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["disconnected"].as_u64()),
        (StatusCode::OK, Some(0)),
        "{body}"
    );
    let (status, body) = post(&app, &cookie, "/admin/api/social-rpc/presence", json!({})).await;
    assert_eq!(
        (status, body["body"]["online"].as_u64()),
        (StatusCode::OK, Some(0)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/communities/list",
        json!({ "limit": "5" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        console_rows(&pool, &actor)
            .await
            .iter()
            .map(|r| r.1.as_str())
            .collect::<Vec<_>>(),
        vec![
            "communities.list",
            "social-rpc.presence",
            "social-rpc.disconnect"
        ]
    );
    cleanup(&pool, &actor).await;

    // No bearer configured for the module: the old 403, nothing called.
    let (app, _, cookie) = console(&pool, true, social_domains(&pool, true, None).await).await;
    for (path, key) in [
        (
            "/admin/api/social/user-ban",
            "comms-moderator-token-not-configured",
        ),
        (
            "/admin/api/social-rpc/presence",
            "social-rpc-admin-token-not-configured",
        ),
        (
            "/admin/api/communities/list",
            "communities-admin-token-not-configured",
        ),
        (
            "/admin/api/notifications/broadcast",
            "notifications-admin-token-not-configured",
        ),
        (
            "/admin/api/badges/grant",
            "badges-admin-token-not-configured",
        ),
    ] {
        let (status, body) = post(
            &app,
            &cookie,
            path,
            json!({ "address": user, "reason": "r", "badge_id": "b", "type": "badge_granted" }),
        )
        .await;
        assert_eq!(
            (status, body["body"]["error"].as_str()),
            (StatusCode::FORBIDDEN, Some(key)),
            "{path}"
        );
    }

    let (app, actor, cookie) =
        console(&pool, true, social_domains(&pool, true, Some(TOKEN)).await).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social/user-ban",
        json!({ "address": user, "reason": "spam" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let banned_by: String = sqlx::query_scalar(
        "SELECT banned_by FROM social.user_bans WHERE banned_address = $1 AND lifted_at IS NULL",
    )
    .bind(&user)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        banned_by, actor,
        "the console wallet is the moderator of record"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social/user-unban",
        json!({ "address": user }),
    )
    .await;
    assert_eq!(
        (status, body["ok"].as_bool(), body["status"].as_u64()),
        (StatusCode::OK, Some(true), Some(204)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social/user-warning",
        json!({ "address": user, "reason": "be nice" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social-rpc/reset-settings",
        json!({ "address": user }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["reset"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/social-rpc/force-presence",
        json!({ "address": user, "presence": "online" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the old form's `presence` field: {body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/communities/suspend",
        json!({ "id": uuid::Uuid::new_v4().to_string(), "reason": "r" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/notifications/broadcast",
        json!({ "type": "badge_granted", "metadata": "{\"title\":\"t\"}", "addresses": user }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["recipients"].as_u64()),
        (StatusCode::OK, Some(1)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/badges/grant",
        json!({ "address": user, "badge_id": "no-such-badge" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/social/user-ban",
        json!({ "address": "0x123", "reason": "r" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an address is checked before the call"
    );

    let rows = console_rows(&pool, &actor).await;
    let summary: Vec<(&str, &str)> = rows.iter().map(|r| (r.1.as_str(), r.3.as_str())).collect();
    assert_eq!(
        summary,
        vec![
            ("badges.grant", "error"),
            ("notifications.broadcast", "ok"),
            ("communities.suspend", "error"),
            ("social-rpc.force-presence", "ok"),
            ("social-rpc.reset-settings", "ok"),
            ("social.user-warning", "ok"),
            ("social.user-unban", "ok"),
            ("social.user-ban", "ok"),
        ]
    );
    assert!(rows.iter().all(|r| r.0 == "social"));
    assert_eq!(rows.last().unwrap().2.as_deref(), Some(user.as_str()));

    sqlx::query("DELETE FROM social.user_warnings WHERE warned_address = $1")
        .bind(&user)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM social.user_bans WHERE banned_address = $1")
        .bind(&user)
        .execute(&pool)
        .await
        .ok();
    cleanup(&pool, &actor).await;
}

async fn worlds_domains(pool: &PgPool, writes: bool, token: Option<&str>) -> Domains {
    let policy = Arc::new(crate::worlds::policy::Policy::new(
        pool.clone(),
        crate::worlds::policy::PersonalWorldsPolicy::disabled(),
        None,
    ));
    let store = storage().await;
    let core = crate::worlds::core::CoreState::new(
        pool.clone(),
        store.clone(),
        policy.clone(),
        crate::worlds::core::Settings::for_tests(token),
        None,
    );
    let host_settings = crate::worlds::scene_state::Settings {
        admin_token: token.map(str::to_string),
        debugging_secret: token.map(str::to_string),
        ..crate::worlds::scene_state::Settings::for_tests()
    };
    let host = crate::worlds::scene_state::SceneHostState::new(
        pool.clone(),
        store,
        policy,
        host_settings,
        None,
    )
    .unwrap();
    Domains {
        worlds: Some(Arc::new(core)),
        scene_host: Some(Arc::new(host)),
        worlds_writes_enabled: writes,
        ..Default::default()
    }
}

#[tokio::test]
async fn worlds_and_scene_host_calls_follow_the_worlds_gate() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let blocked = random_address();

    let (app, actor, cookie) =
        console(&pool, true, worlds_domains(&pool, false, Some(TOKEN)).await).await;
    for (path, body) in [
        (
            "/admin/api/worlds/blocked-add",
            json!({ "wallet": blocked }),
        ),
        (
            "/admin/api/worlds/blocked-remove",
            json!({ "wallet": blocked }),
        ),
        (
            "/admin/api/worlds/enable",
            json!({ "world_name": "nobody.dcl.eth" }),
        ),
        (
            "/admin/api/worlds/disable",
            json!({ "world_name": "nobody.dcl.eth" }),
        ),
    ] {
        let (status, body) = post(&app, &cookie, path, body).await;
        assert_refused(status, &body, "WORLDS_WRITES_ENABLED");
    }
    assert!(console_rows(&pool, &actor).await.is_empty());
    // The scene host is not user authority: its controls answer while
    // worlds writes are off (nothing is loaded, so the host says so).
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/scene-state/kick-all",
        json!({ "scene": "nothing-here" }),
    )
    .await;
    assert_eq!(
        (status, body["body"].as_str()),
        (StatusCode::NOT_FOUND, Some("nothing-here is not loaded")),
        "{body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/worlds/list",
        json!({ "limit": "2" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    cleanup(&pool, &actor).await;

    let (app, _, cookie) = console(&pool, true, worlds_domains(&pool, true, None).await).await;
    let (status, body) = post(&app, &cookie, "/admin/api/worlds/list", json!({})).await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("worlds-admin-token-not-configured")
        )
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/scene/reload",
        json!({ "name": "x" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("debugging-secret-not-configured")
        )
    );

    let (app, actor, cookie) =
        console(&pool, true, worlds_domains(&pool, true, Some(TOKEN)).await).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/worlds/blocked-add",
        json!({ "wallet": blocked }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["blocked"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    let (status, body) = post(&app, &cookie, "/admin/api/worlds/blocked-list", json!({})).await;
    assert!(
        status == StatusCode::OK
            && body["body"]["blocked"]
                .as_array()
                .unwrap()
                .iter()
                .any(|b| b["wallet"] == blocked.as_str()),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/worlds/blocked-remove",
        json!({ "wallet": blocked }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["blocked"].as_bool()),
        (StatusCode::OK, Some(false)),
        "{body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/worlds/enable",
        json!({ "world_name": "nobody-console.dcl.eth" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/scene-state/crdt",
        json!({ "scene": "nothing-here" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/scene/reload",
        json!({ "name": "not a scene key" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the old name rule refuses whitespace before the host is asked: {body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/scene/reload",
        json!({ "sceneId": "bafy-not-a-key" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the host refuses a name that is no scene key: {body}"
    );

    let rows = console_rows(&pool, &actor).await;
    let summary: Vec<(&str, Option<&str>, &str)> = rows
        .iter()
        .map(|r| (r.1.as_str(), r.2.as_deref(), r.3.as_str()))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("scene.reload", Some("bafy-not-a-key"), "error"),
            ("scene-state.crdt", Some("nothing-here"), "error"),
            ("worlds.enable", Some("nobody-console.dcl.eth"), "error"),
            ("worlds.blocked.remove", Some(blocked.as_str()), "ok"),
            ("worlds.blocked.list", None, "ok"),
            ("worlds.blocked.add", Some(blocked.as_str()), "ok"),
        ]
    );
    assert!(rows.iter().all(|r| r.0 == "worlds"));
    let secret_leaked: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.admin_audit WHERE actor = $1 AND detail::text LIKE '%' || $2 || '%'")
        .bind(&actor)
        .bind(TOKEN)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        secret_leaked, 0,
        "the node's credentials never reach the audit detail"
    );
    cleanup(&pool, &actor).await;
}

async fn create_domains(pool: &PgPool, writes: bool, token: Option<&str>) -> Domains {
    let store = storage().await;
    let denylist = store.denylist().clone();
    let reel = crate::camera_reel::Settings {
        admin_token: token.map(str::to_string),
        writes_enabled: writes,
        ..crate::camera_reel::Settings::new("http://127.0.0.1:1/camera-reel-service")
    };
    let reel = Arc::new(crate::camera_reel::State::new(
        pool.clone(),
        store.clone(),
        reel,
        None,
    ));
    let builder = crate::builder::Settings::parse(crate::builder::RawSettings {
        writes_enabled: writes,
        admin_token: token.map(str::to_string),
        ..Default::default()
    })
    .unwrap();
    let builder = Arc::new(
        crate::builder::State::new(
            pool.clone(),
            store,
            denylist,
            Outbound::for_tests(),
            None,
            None,
            builder,
        )
        .unwrap(),
    );
    Domains {
        camera_reel: Some(reel),
        builder: Some(builder),
        ..Default::default()
    }
}

#[tokio::test]
async fn creator_calls_follow_the_create_gate() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let (image, collection, item) = (
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    );

    let (app, actor, cookie) =
        console(&pool, true, create_domains(&pool, false, Some(TOKEN)).await).await;
    for (path, body) in [
        (
            "/admin/api/camera-reel/image-delete",
            json!({ "image_id": image }),
        ),
        (
            "/admin/api/camera-reel/image-review",
            json!({ "image_id": image, "action": "reject" }),
        ),
        (
            "/admin/api/builder/item-status",
            json!({ "collection_id": collection, "item_id": item, "status": "approved" }),
        ),
        (
            "/admin/api/builder/items-status",
            json!({ "collection_id": collection, "item_ids": item, "status": "approved" }),
        ),
    ] {
        let (status, body) = post(&app, &cookie, path, body).await;
        assert_refused(status, &body, "CREATE_WRITES_ENABLED");
    }
    assert!(console_rows(&pool, &actor).await.is_empty());

    let (app, _, cookie) = console(&pool, true, create_domains(&pool, true, None).await).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/camera-reel/image-delete",
        json!({ "image_id": image }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("camera-reel-admin-token-not-configured")
        )
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/builder/item-status",
        json!({ "collection_id": collection, "item_id": item, "status": "approved" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("builder-admin-token-not-configured")
        )
    );

    let (app, actor, cookie) =
        console(&pool, true, create_domains(&pool, true, Some(TOKEN)).await).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/camera-reel/image-delete",
        json!({ "image_id": image }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the reel's own answer for an unknown photo: {body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/camera-reel/image-review",
        json!({ "image_id": image, "action": "approve" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/builder/item-status",
        json!({ "collection_id": collection, "item_id": item, "status": "sideways" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the builder's status rule: {body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/builder/items-status",
        json!({ "collection_id": collection, "itemIds": [item], "status": "approved" }),
    )
    .await;
    assert!(
        status.is_success() || status == StatusCode::NOT_FOUND,
        "{status} {body}"
    );
    let rows = console_rows(&pool, &actor).await;
    assert_eq!(
        rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
        vec![
            "builder.items.status",
            "builder.item.status",
            "camera-reel.image.review",
            "camera-reel.image.delete"
        ]
    );
    assert!(rows.iter().all(|r| r.0 == "create"));
    cleanup(&pool, &actor).await;
}

#[tokio::test]
async fn relay_calls_are_open_while_writes_are_off_and_audit_the_host_only() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let relay = |token: Option<&str>| {
        let settings = crate::ops::rpc::Settings {
            admin_token: token.map(str::to_string),
            ..crate::ops::rpc::Settings::for_tests(&[("mainnet", "https://rpc.example.org")])
        };
        crate::ops::rpc::State::new(settings, Outbound::for_tests(), pool.clone())
    };
    let (app, _, cookie) = console(
        &pool,
        false,
        Domains {
            rpc: Some(relay(None)),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(&app, &cookie, "/admin/api/rpc/config", json!({})).await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("rpc-admin-token-not-configured")
        )
    );

    let rs = relay(Some(TOKEN));
    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            rpc: Some(rs.clone()),
            ..Default::default()
        },
    )
    .await;
    let method = format!(
        "eth_getConsole{}",
        &uuid::Uuid::new_v4().simple().to_string()[..6]
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/rpc/methods-add",
        json!({ "method": method }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["added"].as_bool()),
        (StatusCode::OK, Some(true)),
        "OPS_WRITES_ENABLED does not gate the relay: {body}"
    );
    assert!(rs.methods_snapshot().contains(&method));
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/rpc/networks-set",
        json!({ "network": "Side", "url": "http://10.1.2.3/key-in-path" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the relay's own upstream rule: {body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/rpc/methods-remove",
        json!({ "method": method }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let rows = audit_rows(&pool, &actor).await;
    assert_eq!(
        rows.iter()
            .map(|r| (r.action.as_str(), r.result.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("rpc.methods.remove", "ok"),
            ("rpc.networks.set", "error"),
            ("rpc.methods.add", "ok"),
        ]
    );
    assert_eq!(
        rows[1].detail,
        json!({ "network": "Side", "host": "10.1.2.3", "status": 400 }),
        "the upstream's host only, never its path"
    );
    let own: i64 = sqlx::query_scalar("SELECT count(*) FROM ops.admin_audit WHERE domain = 'rpc' AND actor = 'token:CATALYRST_RPC_ADMIN_TOKEN' AND target = $1")
        .bind(&method)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(own, 2, "the relay's own rows for the add and the remove");
    sqlx::query("DELETE FROM ops.admin_audit WHERE domain = 'rpc' AND target = $1")
        .bind(&method)
        .execute(&pool)
        .await
        .unwrap();
    cleanup(&pool, &actor).await;
}

/// What reached the abgen stand-in: method, path, bearer, operator header.
type Seen = Arc<std::sync::Mutex<Vec<(String, String, Option<String>, Option<String>)>>>;

/// A stand-in for the abgen runtime on the loopback: it answers every
/// path and records what reached it.
async fn abgen_stub() -> (String, Seen) {
    use axum::extract::{Request, State as AxState};
    let seen: Seen = Arc::default();
    async fn record(AxState(seen): AxState<Seen>, req: Request) -> axum::Json<Value> {
        let h = |n: &str| {
            req.headers()
                .get(n)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        seen.lock().unwrap().push((
            req.method().to_string(),
            req.uri().path().to_string(),
            h("authorization"),
            h("x-catalyrst-admin"),
        ));
        axum::Json(json!({ "windowsPendingJobs": [], "path": req.uri().path() }))
    }
    let app = Router::new().fallback(record).with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), seen)
}

#[tokio::test]
async fn abgen_calls_go_to_the_loopback_runtime_behind_the_registry_gate() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let (base, seen) = abgen_stub().await;
    let http = Outbound::for_tests();
    let abgen = |token: Option<&str>| {
        super::create::Abgen::parse(&base, token.map(str::to_string), &http).unwrap()
    };
    let entity = format!("bafy-console-{}", uuid::Uuid::new_v4().simple());

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            abgen: Some(abgen(Some(TOKEN))),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/create/denylist-add",
        json!({ "entity_id": entity }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");
    let (status, body) = post(&app, &cookie, "/admin/api/create/queues-status", json!({})).await;
    assert_eq!(
        (status, body["body"]["path"].as_str()),
        (StatusCode::OK, Some("/queues/status")),
        "a read is open: {body}"
    );
    assert_eq!(
        console_rows(&pool, &actor).await,
        vec![(
            "registry".into(),
            "create.queues.status".into(),
            None,
            "ok".into()
        )]
    );
    cleanup(&pool, &actor).await;

    let (app, _, cookie) = console(
        &pool,
        true,
        Domains {
            abgen: Some(abgen(None)),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(&app, &cookie, "/admin/api/create/queues-pause", json!({})).await;
    assert_eq!(
        (status, body["body"]["error"].as_str()),
        (
            StatusCode::FORBIDDEN,
            Some("ab-registry-admin-token-not-configured")
        )
    );
    let (status, _) = post(&app, &cookie, "/admin/api/create/flush-ab-cache", json!({})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the cache flush and the re-ingest never required the bearer"
    );

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            abgen: Some(abgen(Some(TOKEN))),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/create/denylist-add",
        json!({ "entity_id": entity, "reason": "r" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let last = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        last,
        (
            "POST".into(),
            format!("/denylist/{entity}"),
            Some(format!("Bearer {TOKEN}")),
            Some(actor.clone())
        )
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/create/denylist-remove",
        json!({ "entityId": "../x" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The runtime is down: the old transport-failure envelope, audited.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let down = super::create::Abgen::parse(&format!("http://{closed}"), Some(TOKEN.into()), &http)
        .unwrap();
    let (app2, actor2, cookie2) = console(
        &pool,
        true,
        Domains {
            abgen: Some(down),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app2,
        &cookie2,
        "/admin/api/create/queues-retry",
        json!({ "entityId": entity }),
    )
    .await;
    assert_eq!(
        (
            status,
            body["status"].as_u64(),
            body["body"]["error"].as_str()
        ),
        (StatusCode::BAD_GATEWAY, Some(0), Some("request-failed")),
        "{body}"
    );
    assert_eq!(
        console_rows(&pool, &actor2).await,
        vec![(
            "registry".into(),
            "create.queues.retry".into(),
            None,
            "error".into()
        )]
    );

    assert_eq!(
        console_rows(&pool, &actor).await,
        vec![(
            "registry".into(),
            "create.denylist.add".into(),
            Some(entity.clone()),
            "ok".into()
        )]
    );
    cleanup(&pool, &actor).await;
    cleanup(&pool, &actor2).await;
}

#[tokio::test]
async fn the_console_pages_show_a_visitor_the_sign_in_card_and_an_operator_the_controls() {
    let admin = PrivateKeySigner::random();
    let app = app(lazy_pool(), false, Domains::default(), &wallet(&admin)).await;
    let get = |cookie: Option<String>, path: &'static str| {
        let app = app.clone();
        async move {
            let mut req = axum::http::Request::get(path).header(header::HOST, super::tests::HOST);
            if let Some(c) = cookie {
                req = req.header(header::COOKIE, c);
            }
            let resp =
                tower::ServiceExt::oneshot(app, req.body(axum::body::Body::empty()).unwrap())
                    .await
                    .unwrap();
            let (status, headers) = (resp.status(), resp.headers().clone());
            let body = String::from_utf8(
                axum::body::to_bytes(resp.into_body(), 1 << 22)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            (status, headers, body)
        }
    };
    let (status, headers, body) = get(None, "/admin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(headers.get(header::X_FRAME_OPTIONS).unwrap(), "DENY");
    assert!(
        body.contains("id=\"admin-signin\""),
        "a visitor gets the sign-in card"
    );
    assert!(
        !body.contains("data-admin-action=\"/admin/api") && !body.contains("Live activity"),
        "and neither controls nor status"
    );
    let (status, _, _) = get(None, "/admin/no-such-family").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let cookie = session_cookie(&app, &admin).await;
    // The status reads the backlog over the pool; the lazy pool answers
    // "unavailable" there, the page still renders.
    let (status, _, body) = get(Some(cookie.clone()), "/admin").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("id=\"admin-signout\"")
            && body.contains("Live activity")
            && body.contains("Write switches"),
        "the operator view"
    );
    for route in CONSOLE_ROUTES {
        assert!(
            body.contains(&format!("data-admin-action=\"{route}\"")),
            "{route} is on /admin"
        );
    }
    let (status, _, body) = get(Some(cookie), "/admin/social").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("/admin/api/social/user-ban") && !body.contains("/admin/api/credits/grant"),
        "one family's controls only"
    );
}

#[tokio::test]
async fn explorer_overlays_are_module_calls_behind_the_console_switch() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let content = Arc::new(ContentState::new(
        pool.clone(),
        ContentSettings::for_tests(),
        storage().await,
    ));
    let settings =
        crate::explore::api::Settings::for_tests(crate::explore::api::Sources::defaults());
    let api =
        crate::explore::api::State::new(settings, pool.clone(), Outbound::for_tests(), content)
            .await
            .unwrap();
    let flag = format!(
        "explorer-console-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let denied = random_address();
    let poi = format!(
        "{},{}",
        900 + (uuid::Uuid::new_v4().as_u128() % 100) as i32,
        -900
    );

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            explorer_api: Some(api.clone()),
            ..Default::default()
        },
    )
    .await;
    for (path, body) in [
        (
            "/admin/api/explorer-api/flags-toggle",
            json!({ "name": flag }),
        ),
        (
            "/admin/api/explorer-api/blocklist-add",
            json!({ "wallet": denied }),
        ),
        ("/admin/api/places/poi-create", json!({ "position": poi })),
    ] {
        let (status, body) = post(&app, &cookie, path, body).await;
        assert_refused(status, &body, "OPS_WRITES_ENABLED");
    }
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/explorer-api/flags-reload",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a reload is open");
    assert_eq!(
        console_rows(&pool, &actor)
            .await
            .iter()
            .map(|r| r.1.as_str())
            .collect::<Vec<_>>(),
        vec!["explorer-api.flags.reload"]
    );
    cleanup(&pool, &actor).await;

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            explorer_api: Some(api.clone()),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/explorer-api/flags-toggle",
        json!({ "name": flag, "value": "false" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["value"].as_bool()),
        (StatusCode::OK, Some(false)),
        "{body}"
    );
    assert_eq!(
        api.flags.document()["flags"][&flag],
        json!(false),
        "the served document carries the override at once"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/explorer-api/flags-clear",
        json!({ "name": flag }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        api.flags.document()["flags"].get(&flag).is_none(),
        "cleared: the upstream document again"
    );

    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/explorer-api/blocklist-add",
        json!({ "wallet": denied }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["added"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    assert!(api.denylist.contains(&denied));
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/explorer-api/blocklist-remove",
        json!({ "wallet": denied }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["removed"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    assert!(!api.denylist.contains(&denied));

    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/poi-create",
        json!({ "position": poi }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = post(&app, &cookie, "/admin/api/places/pois-list", json!({})).await;
    assert!(
        body["body"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p == poi.as_str()),
        "{body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/places/poi-delete",
        json!({ "position": poi }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !crate::explore::api::lists::values(&pool, "pois")
            .await
            .unwrap()
            .contains(&poi)
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/places/poi-create",
        json!({ "position": "north" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let rows = console_rows(&pool, &actor).await;
    assert_eq!(
        rows.iter()
            .map(|r| (r.1.as_str(), r.3.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("places.poi.delete", "ok"),
            ("places.pois.list", "ok"),
            ("places.poi.create", "ok"),
            ("explorer-api.blocklist.remove", "ok"),
            ("explorer-api.blocklist.add", "ok"),
            ("explorer-api.flags.clear", "ok"),
            ("explorer-api.flags.toggle", "ok"),
        ]
    );
    assert!(rows.iter().all(|r| r.0 == "explore"));
    sqlx::query("DELETE FROM explore_api.lists_local WHERE list = 'pois' AND value = $1")
        .bind(&poi)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM explore_api.denylist_users_local WHERE wallet = $1")
        .bind(&denied)
        .execute(&pool)
        .await
        .unwrap();
    cleanup(&pool, &actor).await;
}

#[tokio::test]
async fn place_world_and_event_moderation_record_the_operator_and_survive_the_mirror() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let explore = Arc::new(crate::explore::ExploreState::new(
        pool.clone(),
        crate::explore::Settings::for_tests(),
        None,
    ));
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let (place, world, event) = (
        format!("console-place-{tag}"),
        format!("console-world-{tag}"),
        format!("console-event-{tag}"),
    );
    let world_name = format!("console{}.dcl.eth", &tag[..10]);
    let mirrored = |id: &str, raw: Value| crate::explore::places::mirror::MirrorRow {
        id: id.to_string(),
        base_position: "3,4".into(),
        title: "t".into(),
        description: String::new(),
        creator_address: None,
        content_rating: Some("E".into()),
        categories: Vec::new(),
        likes: 0,
        dislikes: 0,
        favorites: 0,
        deployed_at: None,
        disabled: false,
        highlighted: false,
        raw,
    };
    let rows = vec![
        mirrored(&place, json!({ "id": place })),
        mirrored(
            &world,
            json!({ "id": world, "world": true, "world_name": world_name }),
        ),
    ];
    crate::explore::places::mirror::upsert_rows(&pool, &rows)
        .await
        .unwrap();
    sqlx::query("INSERT INTO explore.event (id, name, approved, raw) VALUES ($1, 'e', false, $2)")
        .bind(&event)
        .bind(json!({ "id": event, "name": "e", "approved": false }))
        .execute(&pool)
        .await
        .unwrap();

    let (app, actor, cookie) = console(
        &pool,
        false,
        Domains {
            explore: Some(explore.clone()),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/place-disable",
        json!({ "place_id": place }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/events/moderate",
        json!({ "event_id": event, "action": "approve" }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");
    assert!(console_rows(&pool, &actor).await.is_empty());

    let (app, actor, cookie) = console(
        &pool,
        true,
        Domains {
            explore: Some(explore.clone()),
            ..Default::default()
        },
    )
    .await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/place-disable",
        json!({ "place_id": place, "reason": "spam" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["data"]["disabled"].as_bool()),
        (StatusCode::OK, Some(true)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/place-rating",
        json!({ "place_id": place, "rating": "A" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["data"]["content_rating"].as_str()),
        (StatusCode::OK, Some("A")),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/world-highlight",
        json!({ "world_id": world_name, "highlighted": "true" }),
    )
    .await;
    assert_eq!(
        (status, body["body"]["data"]["highlighted"].as_bool()),
        (StatusCode::OK, Some(true)),
        "a world by its name: {body}"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/places/world-rating",
        json!({ "world_id": place, "content_rating": "T" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a place is not a world");
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/places/place-rating",
        json!({ "place_id": place, "content_rating": "X" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the old rating rule, before the module is asked (no row)"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/events/moderate",
        json!({ "event_id": event, "action": "approve", "trending": "true" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (
            body["body"]["data"]["approved"].as_bool(),
            body["body"]["data"]["trending"].as_bool(),
            body["body"]["data"]["approved_by"].as_str()
        ),
        (Some(true), Some(true), Some(actor.as_str()))
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/events/moderate",
        json!({ "event_id": event }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no moderation fields");
    let start = (chrono::Utc::now() + chrono::Duration::hours(3)).to_rfc3339();
    let (status, body) =
        post(&app, &cookie, "/admin/api/events/create", json!({ "name": "operator event", "start_at": start, "duration": "3600000", "x": "3", "y": "4" })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let created = body["body"]["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        (
            body["body"]["data"]["approved"].as_bool(),
            body["body"]["data"]["user"].as_str()
        ),
        (Some(false), Some("admin")),
        "pending, created by admin"
    );
    let (status, _) = post(
        &app,
        &cookie,
        "/admin/api/events/create",
        json!({ "name": "x", "start_at": start, "duration": 1, "x": 3, "y": 4, "admin": true }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown fields are refused before the module is asked"
    );

    // A mirror pass with upstream's values: the operator's decisions stay.
    crate::explore::places::mirror::upsert_rows(&pool, &rows)
        .await
        .unwrap();
    let place_row = explore
        .places
        .find_by_id_for(&place, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (place_row.disabled, place_row.content_rating.as_deref()),
        (true, Some("A"))
    );
    let world_row = explore
        .places
        .find_world_by_id_for(&world, None)
        .await
        .unwrap()
        .unwrap();
    assert!(world_row.highlighted);
    let overlay: String =
        sqlx::query_scalar("SELECT set_by FROM explore.place_moderation WHERE place_id = $1")
            .bind(&place)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(overlay, actor);
    assert!(
        crate::explore::events::mirror::upsert_event(
            &pool,
            &json!({ "id": event, "name": "e upstream", "approved": false, "trending": false }),
        )
        .await
        .unwrap()
    );
    let (name, approved, trending, set_by): (String, bool, bool, String) = sqlx::query_as(
        "SELECT e.name, e.approved, e.trending, m.set_by FROM explore.event e \
         JOIN explore.event_moderation m ON m.event_id = e.id WHERE e.id = $1",
    )
    .bind(&event)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (name.as_str(), approved, trending, set_by.as_str()),
        ("e upstream", true, true, actor.as_str()),
        "the mirrored event takes the upstream's name and keeps the operator's flags"
    );

    let rows = console_rows(&pool, &actor).await;
    assert_eq!(
        rows[0].2.as_deref(),
        Some(created.as_str()),
        "the created event is the row's target"
    );
    assert_eq!(
        rows.iter()
            .map(|r| (r.1.as_str(), r.3.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("events.create", "ok"),
            ("events.moderate", "error"),
            ("events.moderate", "ok"),
            ("places.world.rating", "error"),
            ("places.world.highlight", "ok"),
            ("places.place.rating", "ok"),
            ("places.place.disable", "ok"),
        ]
    );
    for sql in [
        "DELETE FROM explore.place_moderation WHERE place_id = ANY($1)",
        "DELETE FROM explore.place WHERE id = ANY($1)",
        "DELETE FROM explore.event_moderation WHERE event_id = ANY($1)",
        "DELETE FROM explore.events_local WHERE id = ANY($1)",
        "DELETE FROM explore.event WHERE id = ANY($1)",
    ] {
        sqlx::query(sql)
            .bind(vec![
                place.clone(),
                world.clone(),
                event.clone(),
                created.clone(),
            ])
            .execute(&pool)
            .await
            .unwrap();
    }
    cleanup(&pool, &actor).await;
}

/// `places/{reports,report-resolve}`: the old console's two report calls as
/// module calls, the list open with writes off, the resolve behind the
/// console switch.
#[tokio::test]
async fn place_reports_are_listed_and_settled_behind_the_console_switch() {
    let Some(pool) = crate::db::scratch::pool(4).await else {
        return;
    };
    let explore = Arc::new(crate::explore::ExploreState::new(
        pool.clone(),
        crate::explore::Settings::for_tests(),
        None,
    ));
    let entity = format!("console-report-{}", uuid::Uuid::new_v4().simple());
    let id: i64 = sqlx::query_scalar("INSERT INTO explore.place_reports (filename, entity_id, reporter, signed_url) VALUES ($1, $2, '0x00000000000000000000000000000000000000ab', 'https://x.test') RETURNING id")
        .bind(format!("{entity}.json"))
        .bind(&entity)
        .fetch_one(&pool)
        .await
        .unwrap();
    let domains = || Domains {
        explore: Some(explore.clone()),
        ..Default::default()
    };
    let (app, _, cookie) = console(&pool, false, domains()).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/reports",
        json!({ "entity_id": entity }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (
            body["body"]["data"][0]["id"].as_i64(),
            body["body"]["total"].as_i64()
        ),
        (Some(id), Some(1)),
        "{body}"
    );
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/report-resolve",
        json!({ "id": id.to_string(), "status": "resolved" }),
    )
    .await;
    assert_refused(status, &body, "OPS_WRITES_ENABLED");

    let (app, _, cookie) = console(&pool, true, domains()).await;
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/report-resolve",
        json!({ "id": id.to_string(), "status": "sideways" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = post(
        &app,
        &cookie,
        "/admin/api/places/report-resolve",
        json!({ "id": id.to_string(), "status": "resolved", "resolution": "removed" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (
            body["body"]["data"]["status"].as_str(),
            body["body"]["data"]["resolution"].as_str()
        ),
        (Some("resolved"), Some("removed")),
        "{body}"
    );
}
