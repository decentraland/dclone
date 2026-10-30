//! The comms contract tests: the authorization matrix of every credential,
//! the ban gates, the scene identity resolution and the voice presence
//! lifecycle, against the lib's scratch database (`CATALYST_TEST_DATABASE_URL`;
//! skipped without it) with uuid-scoped rows. Most tests use a closed
//! loopback media port; boundary conformance modules serve explicit local
//! Twirp fixtures. Neither establishes a live SFU result.

use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use crate::http::Outbound;
use crate::interfaces::test_support::{StubPolicy, StubScenes};
use crate::{Address, CommunityId, PermissionDecision};

use super::admission::handlers;
use super::admission::scene::{self, PlaceLookup, RoomContext};
use super::auth::test_support::{
    AUTHORITATIVE_KEY, KEY, OUTSIDER_KEY, address_of, random_key, signed,
};
use super::moderation::store::{
    CreateBan, PLATFORM_BANNED_MSG, SCENE_BANNED_MSG, UpsertPlayerConnection,
};
use super::voice::db::PrivateJoin;
use super::voice::{self, CommunityVoiceChatBody};
use super::{CommsState, Settings, SharedState};

#[path = "canonical_rooms_tests.rs"]
mod canonical_rooms;

#[path = "toolkit_moderation_tests.rs"]
mod toolkit_moderation;

fn wallet() -> Address {
    Address::parse(&format!("0x00000000{}", Uuid::new_v4().simple())).unwrap()
}

fn tag() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn scratch() -> Option<PgPool> {
    crate::db::scratch::pool(8).await
}

/// The `OriginalUri` of a handler called in process: the route at the root.
fn at(route: &str) -> axum::extract::OriginalUri {
    axum::extract::OriginalUri(route.parse().unwrap())
}

fn comms_state(pool: PgPool, settings: Settings) -> CommsState {
    CommsState::new(
        pool,
        Outbound::for_tests(),
        settings,
        None,
        None,
        Arc::new(StubPolicy::default()),
        Arc::new(StubScenes::default()),
        Arc::new(crate::presence::Presence::new()),
        Arc::new(crate::interfaces::test_support::StubDirectory::default()),
    )
}

fn state_with(pool: PgPool, settings: Settings) -> SharedState {
    Arc::new(comms_state(pool, settings))
}

fn lazy_pool() -> PgPool {
    PgPool::connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("a lazy pool never connects")
}

fn lazy_state(settings: Settings) -> SharedState {
    state_with(lazy_pool(), settings)
}

async fn webhook_voice_settings() -> (Settings, tokio::task::JoinHandle<()>) {
    let fixture = axum::Router::new().route(
        "/twirp/livekit.RoomService/DeleteRoom",
        axum::routing::post(|| async { Json(serde_json::json!({})) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut settings = Settings::for_tests();
    settings.livekit.api_url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
    (settings, task)
}

async fn body_json(resp: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

async fn seed_community(pool: &PgPool) -> CommunityId {
    let id = CommunityId::new();
    // Private and unlisted, so the rows stay out of every other module's
    // global listings while this process's tests share one scratch database.
    sqlx::query("INSERT INTO social.communities (id, name, owner_address, private, unlisted) VALUES ($1, $2, $3, TRUE, TRUE)")
        .bind(id)
        .bind(format!("community {id}"))
        .bind(wallet())
        .execute(pool)
        .await
        .unwrap();
    id
}

async fn seed_place(pool: &PgPool, id: &str, raw: serde_json::Value, base_position: &str) {
    sqlx::query("INSERT INTO explore.place (id, base_position, raw) VALUES ($1, $2, $3::jsonb)")
        .bind(id)
        .bind(base_position)
        .bind(raw)
        .execute(pool)
        .await
        .unwrap();
}

async fn seed_world_scene(pool: &PgPool, world: &str, entity_id: &str, parcels: &[&str]) {
    sqlx::query("INSERT INTO worlds.worlds (name) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(world)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO worlds.world_scenes (world_name, entity_id, deployment_auth_chain, entity, deployer, parcels, size, mirror_source, created_at) \
         VALUES ($1, $2, '[]'::json, '{}'::jsonb, $3, $4, 1, NULL, timezone('UTC', now()) + ($5 || ' seconds')::interval)",
    )
    .bind(world)
    .bind(entity_id)
    .bind(crate::shared::identity::ZERO_ADDRESS)
    .bind(parcels.iter().map(|p| p.to_string()).collect::<Vec<_>>())
    .bind(entity_id.len().to_string())
    .execute(pool)
    .await
    .unwrap();
}

fn with_authoritative(key: &str) -> Settings {
    Settings {
        authoritative_server_address: Some(Address::parse(&address_of(key)).unwrap()),
        ..Settings::for_tests()
    }
}

#[tokio::test]
async fn body_identity_without_auth_chain_cannot_mint_authoritative_token() {
    let state = lazy_state(with_authoritative(AUTHORITATIVE_KEY));
    let err = handlers::get_server_scene_adapter(State(state), at("/get-server-scene-adapter"), HeaderMap::new())
        .await
        .expect_err("an unauthenticated caller who merely knows the public authoritative address must not receive a token");
    assert_eq!(err.code, 401, "{}", err.message);
}

#[tokio::test]
async fn body_identity_cannot_override_a_non_authoritative_signature() {
    let state = lazy_state(with_authoritative(AUTHORITATIVE_KEY));
    let headers = signed(OUTSIDER_KEY, "post", "/get-server-scene-adapter", "{}").await;
    let err =
        handlers::get_server_scene_adapter(State(state), at("/get-server-scene-adapter"), headers)
            .await
            .expect_err("a signature from a non-authoritative wallet must be rejected");
    assert_eq!(err.code, 401);
}

#[tokio::test]
async fn authoritative_server_signature_still_mints_a_token() {
    let state = lazy_state(with_authoritative(AUTHORITATIVE_KEY));
    let headers = signed(
        AUTHORITATIVE_KEY,
        "post",
        "/get-server-scene-adapter",
        r#"{"realm":{"serverName":"main"},"sceneId":"bafkreiservertoken"}"#,
    )
    .await;
    let Json(value) =
        handlers::get_server_scene_adapter(State(state), at("/get-server-scene-adapter"), headers)
            .await
            .expect("the signed authoritative server gets an adapter");
    assert!(
        value
            .adapter
            .starts_with("livekit:wss://livekit.local?access_token=")
    );
    let jwt = value.adapter.split("access_token=").nth(1).unwrap();
    let claims = super::livekit::decode_jwt_payload(jwt);
    assert_eq!(claims["sub"], "authoritative-server");
    assert_eq!(claims["video"]["room"], "scene:main:bafkreiservertoken");
    assert_eq!(claims["video"]["canPublish"], true);
    assert_eq!(
        claims["metadata"].as_str().unwrap(),
        format!(
            r#"{{"catalyrstAuthoritativeServer":{{"wallet":"{}"}}}}"#,
            address_of(AUTHORITATIVE_KEY)
        )
    );
}

#[tokio::test]
async fn an_unconfigured_authoritative_address_refuses_every_caller() {
    let state = lazy_state(Settings::for_tests());
    let headers = signed(
        AUTHORITATIVE_KEY,
        "post",
        "/get-server-scene-adapter",
        r#"{"realm":{"serverName":"main"},"sceneId":"bafkreiservertoken"}"#,
    )
    .await;
    assert_eq!(
        handlers::get_server_scene_adapter(State(state), at("/get-server-scene-adapter"), headers,)
            .await
            .unwrap_err()
            .code,
        401
    );
}

const WORLD_BAN_STATUS: &str = "/worlds/someworld.dcl.eth/parcels/0,0/users/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ban-status";

fn router_with(token: Option<&str>) -> axum::Router {
    super::router(lazy_state(Settings {
        gatekeeper_auth_token: token.map(str::to_string),
        ..Settings::for_tests()
    }))
}

/// Retired APIs stay absent even for credentials that previously enabled reports.
/// Exercise the public mount as well as the standalone router, without PostgreSQL.
#[tokio::test]
async fn retired_report_routes_return_not_found() {
    let state = lazy_state(Settings {
        moderator_token: Some("moderator-token".into()),
        gatekeeper_auth_token: Some("service-token".into()),
        moderator_addresses: vec![Address::parse(&address_of(KEY)).unwrap()],
        ..Settings::for_tests()
    });
    let report_id = "6f1b4f8a-6a1e-4c2f-9b3d-0d9a2c7b1e55";
    let evidence_path = format!("/reports/players/{report_id}/evidence/0-shot.png");
    let detail_path = format!("/reports/{report_id}");
    let routes = [
        ("POST", "/reports/players/presign"),
        ("POST", "/reports/players"),
        ("GET", "/reports"),
        ("GET", detail_path.as_str()),
        ("PUT", evidence_path.as_str()),
        ("GET", evidence_path.as_str()),
    ];
    for prefix in ["", "/comms-gatekeeper"] {
        let router = super::router(state.clone());
        let app = if prefix.is_empty() {
            router
        } else {
            axum::Router::new().nest(prefix, router)
        };
        for (method, path) in routes {
            let uri = format!("{prefix}{path}");
            for credential in ["anonymous", "wallet", "moderator", "service"] {
                let mut request = Request::builder()
                    .method(method)
                    .uri(&uri)
                    .header("content-type", "application/json");
                match credential {
                    "wallet" => {
                        let headers = signed(KEY, &method.to_lowercase(), &uri, "{}").await;
                        *request.headers_mut().unwrap() = headers;
                    }
                    "moderator" => {
                        request = request.header("authorization", "Bearer moderator-token");
                    }
                    "service" => {
                        request = request.header("authorization", "Bearer service-token");
                    }
                    _ => {}
                }
                let response = app
                    .clone()
                    .oneshot(request.body(Body::from("{}")).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::NOT_FOUND,
                    "{method} {uri}, credential={credential}"
                );
            }
        }
    }
}

#[tokio::test]
async fn moderation_routes_still_require_wallet_authentication() {
    let app = router_with(Some("service-token"));
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (method, path, expected) in [
        ("GET", "/scene-admin".to_string(), StatusCode::BAD_REQUEST),
        ("GET", "/scene-bans".to_string(), StatusCode::BAD_REQUEST),
        (
            "POST",
            format!("/users/{address}/bans"),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            format!("/users/{address}/warnings"),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(&path)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{method} {path}");
    }
}

async fn post_json(
    app: &axum::Router,
    path: &str,
    bearer: Option<&str>,
    body: serde_json::Value,
) -> StatusCode {
    let mut req = Request::post(path).header("content-type", "application/json");
    if let Some(b) = bearer {
        req = req.header("authorization", format!("Bearer {b}"));
    }
    app.clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn unconfigured_service_token_refuses_the_bearer_gated_routes_with_503() {
    let app = router_with(None);
    let body = serde_json::json!({ "community_id": "", "user_address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" });
    assert_eq!(
        post_json(&app, "/community-voice-chat", None, body).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let resp = app
        .clone()
        .oneshot(Request::get(WORLD_BAN_STATUS).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let resp = app
        .oneshot(
            Request::patch(
                "/users/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/private-messages-privacy",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn configured_service_token_still_gates_on_the_bearer() {
    let app = router_with(Some("gatekeeper-service-token"));
    let body = || serde_json::json!({ "community_id": "", "user_address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" });
    assert_eq!(
        post_json(&app, "/community-voice-chat", None, body()).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_json(&app, "/community-voice-chat", Some("not-the-token"), body()).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_json(
            &app,
            "/community-voice-chat",
            Some("gatekeeper-service-token"),
            body()
        )
        .await,
        StatusCode::BAD_REQUEST,
        "the platform service must reach the handler"
    );
}

#[tokio::test]
async fn ungated_routes_are_untouched_by_the_fail_closed_gate() {
    let app = router_with(None);
    assert_eq!(
        post_json(&app, "/get-scene-adapter", None, serde_json::json!({})).await,
        StatusCode::UNAUTHORIZED,
        "an unsigned scene-adapter request is answered by its own auth check (upstream's 401)"
    );
    let resp = app
        .clone()
        .oneshot(Request::get("/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["currentTime"].is_number() && body["version"].is_string());
    let resp = app
        .oneshot(Request::get("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn world_ban_keys_on_resolved_scene_id_not_world_name() {
    let Some(pool) = scratch().await else { return };
    let m = super::Moderation::with_social(
        pool.clone(),
        Arc::new(crate::interfaces::test_support::StubDirectory::default()),
    );
    let world_name = format!("{}.eth", tag());
    let resolved_scene_id = format!("bafkrei{}", tag());
    let (user, moderator) = (wallet(), wallet());
    m.scene_ban(&resolved_scene_id, &user, &moderator, &[], "Scene")
        .await
        .unwrap();
    assert!(
        m.is_scene_banned(&resolved_scene_id, &user).await.unwrap(),
        "found when keyed on the resolved scene"
    );
    assert!(
        !m.is_scene_banned(&world_name, &user).await.unwrap(),
        "never found under the raw world name"
    );
    assert!(
        m.is_scene_banned(
            &resolved_scene_id,
            &Address::parse(&user.as_str().to_uppercase()).unwrap()
        )
        .await
        .unwrap()
    );
    assert_eq!(m.count_scene_bans(&resolved_scene_id).await.unwrap(), 1);
    let (page, total) = m
        .list_scene_ban_addresses(&resolved_scene_id, 100, 0)
        .await
        .unwrap();
    assert_eq!((page, total), (vec![user.as_str().to_string()], 1));
    assert_eq!(
        m.list_scene_ban_addresses(&world_name, 100, 0)
            .await
            .unwrap(),
        (vec![], 0)
    );
    let (page, total) = m
        .list_scene_ban_addresses(&resolved_scene_id, 100, 50)
        .await
        .unwrap();
    assert_eq!(
        (page.len(), total),
        (0, 1),
        "an empty page past the start keeps the exact total"
    );
    assert_eq!(
        m.scene_unban(&resolved_scene_id, &user, "Scene")
            .await
            .unwrap(),
        1
    );
    assert!(!m.is_scene_banned(&resolved_scene_id, &user).await.unwrap());
}

#[tokio::test]
async fn concurrent_create_ban_yields_single_active_ban() {
    let Some(pool) = scratch().await else { return };
    let victim = wallet();
    let moderator = wallet();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let m = super::Moderation::with_social(
            pool.clone(),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        );
        let (victim, moderator) = (victim.clone(), moderator.clone());
        handles.push(tokio::spawn(async move {
            m.create_ban(CreateBan {
                banned_address: victim,
                banned_by: moderator.into_string(),
                reason: "abuse".into(),
                custom_message: None,
                banned_device_id: None,
                duration_ms: None,
            })
            .await
        }));
    }
    let (mut created, mut already) = (0, 0);
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => created += 1,
            Err(super::moderation::store::BanWriteError::AlreadyBanned(addr)) => {
                assert_eq!(addr, victim.as_str());
                already += 1;
            }
            Err(super::moderation::store::BanWriteError::Db(e)) => {
                panic!("unexpected db error: {e}")
            }
        }
    }
    assert_eq!(
        (created, already),
        (1, 7),
        "exactly one concurrent ban must win"
    );
    let m = super::Moderation::with_social(
        pool.clone(),
        Arc::new(crate::interfaces::test_support::StubDirectory::default()),
    );
    let status = m.ban_status(&victim).await.unwrap();
    assert!(status.is_banned);
    let lifted = m.lift_ban(&victim, "0xMOD").await.unwrap();
    assert_eq!(lifted.lifted_by.as_deref(), Some("0xmod"));
    assert!(!m.ban_status(&victim).await.unwrap().is_banned);
    assert!(matches!(
        m.lift_ban(&victim, "0xmod").await,
        Err(super::moderation::store::LiftError::NotFound(_))
    ));
    let expired = m
        .create_ban(CreateBan {
            banned_address: victim.clone(),
            banned_by: "m".into(),
            reason: "r".into(),
            custom_message: None,
            banned_device_id: None,
            duration_ms: Some(1),
        })
        .await
        .unwrap();
    assert!(expired.expires_at.is_some());
    // Rather than wait out its millisecond, the ban moves an hour into the past.
    sqlx::query(
        "UPDATE social.user_bans SET banned_at = banned_at - interval '1 hour', \
         created_at = created_at - interval '1 hour', expires_at = expires_at - interval '1 hour' \
         WHERE id = $1::uuid",
    )
    .bind(&expired.id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        !m.ban_status(&victim).await.unwrap().is_banned,
        "an expired ban is not in force"
    );
    assert!(
        m.create_ban(CreateBan {
            banned_address: victim.clone(),
            banned_by: "m".into(),
            reason: "r".into(),
            custom_message: None,
            banned_device_id: None,
            duration_ms: None
        })
        .await
        .is_ok(),
        "a new ban may follow an expired one"
    );
    assert!(
        m.active_bans()
            .await
            .unwrap()
            .iter()
            .any(|b| b.banned_address == victim.as_str())
    );
}

struct Seeded {
    banned: Address,
    same_device: Address,
    clean: Address,
    moderator: Address,
}

/// A device ban on `banned`, and `same_device` recorded on that device by an
/// earlier scene-adapter connection.
async fn seed_device_ban(state: &CommsState) -> Seeded {
    let s = Seeded {
        banned: wallet(),
        same_device: wallet(),
        clean: wallet(),
        moderator: wallet(),
    };
    let device = format!("shared-device-{}", tag());
    state
        .moderation
        .create_ban(CreateBan {
            banned_address: s.banned.clone(),
            banned_by: s.moderator.as_str().into(),
            reason: "abuse".into(),
            custom_message: None,
            banned_device_id: Some(device.clone()),
            duration_ms: None,
        })
        .await
        .expect("create_ban");
    state
        .moderation
        .upsert_connection(UpsertPlayerConnection {
            address: s.same_device.clone(),
            ip_address: None,
            device_id: Some(device),
        })
        .await
        .expect("upsert connection");
    s
}

#[tokio::test]
async fn a_lookup_without_a_device_id_is_widened_to_the_recorded_device() {
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool, Settings::for_tests());
    let s = seed_device_ban(&state).await;
    let m = &state.moderation;
    assert!(
        m.is_banned_for_connection(&s.same_device, None)
            .await
            .unwrap(),
        "a route that sends no device id matches the device recorded for the wallet"
    );
    assert!(m.is_banned_for_connection(&s.banned, None).await.unwrap());
    assert!(!m.is_banned_for_connection(&s.clean, None).await.unwrap());
    assert!(
        !m.is_banned_for_connection(&s.same_device, Some("some-other-device"))
            .await
            .unwrap(),
        "a device id the caller does send is the one matched"
    );
    assert!(matches!(
        m.platform_ban_decision(&[s.clean.clone(), s.same_device.clone()])
            .await,
        PermissionDecision::Refused(_)
    ));
    assert!(
        m.platform_ban_decision(&[s.clean.clone(), s.moderator.clone()])
            .await
            .is_allowed()
    );
    assert!(matches!(
        m.connection_gate(&s.same_device, None, "scene-x")
            .await
            .into_result()
            .unwrap_err()
            .http_status(),
        403
    ));
    assert!(
        m.connection_gate(&s.clean, None, "scene-x")
            .await
            .is_allowed()
    );
    let public = m.ban_status(&s.banned).await.unwrap();
    let v = serde_json::to_value(public).unwrap();
    assert!(
        v["ban"].get("bannedDeviceId").is_none(),
        "the public status never carries the device id"
    );
}

#[tokio::test]
async fn private_voice_credentials_are_refused_when_a_participant_is_platform_banned() {
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let s = seed_device_ban(&state).await;
    let err = voice::get_private_voice_chat_room_credentials(
        &state,
        &Uuid::new_v4().to_string(),
        &[s.clean.clone(), s.same_device.clone()],
    )
    .await
    .expect_err("either side banned refuses the call");
    assert_eq!((err.code, err.message.as_str()), (403, PLATFORM_BANNED_MSG));
    let calls: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM social.private_voice_chats WHERE caller_address = $1 OR callee_address = $1")
        .bind(&s.clean)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(calls, 0, "the refused call must not open the room");
    let call_id = Uuid::new_v4();
    let out = voice::get_private_voice_chat_room_credentials(
        &state,
        &call_id.to_string(),
        &[s.clean.clone(), s.moderator.clone()],
    )
    .await
    .expect("two unbanned participants get credentials");
    assert_eq!(out.len(), 2);
    let url = out[s.clean.as_str()]["connection_url"].as_str().unwrap();
    let claims = super::livekit::decode_jwt_payload(url.split("access_token=").nth(1).unwrap());
    assert_eq!(
        claims["video"]["room"],
        format!("voice-chat-private-{call_id}")
    );
    assert_eq!(claims["video"]["canUpdateOwnMetadata"], false);
    assert!(
        state
            .voice_db
            .active_call_of(&s.clean)
            .await
            .unwrap()
            .is_some()
    );
    let err = voice::get_private_voice_chat_room_credentials(
        &state,
        "not-a-uuid",
        &[s.clean.clone(), s.moderator.clone()],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, 400);
    let err = voice::get_private_voice_chat_room_credentials(
        &state,
        &Uuid::new_v4().to_string(),
        std::slice::from_ref(&s.clean),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, 400);
}

#[tokio::test]
async fn community_voice_join_is_refused_when_the_named_wallet_is_platform_banned() {
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let s = seed_device_ban(&state).await;
    let community = seed_community(&pool).await;
    let body = |address: &Address, role: &str, action: &str| -> CommunityVoiceChatBody {
        serde_json::from_value(serde_json::json!({ "community_id": community.to_string(), "user_address": address.as_str(), "user_role": role, "action": action })).unwrap()
    };
    // A connected moderator keeps the room live: the expiry sweep, which a
    // sibling test runs over this process's database, deletes any room without one.
    let opened = voice::community_voice_chat_create_or_join(
        State(state.clone()),
        Json(body(&s.moderator, "moderator", "create")),
    )
    .await
    .expect("a moderator opens the room");
    assert!(opened.0["connection_url"].is_string());
    voice::handle_community_participant_joined(
        &state,
        s.moderator.as_str(),
        community,
        Some("PA_mod"),
    )
    .await
    .unwrap();
    let err = voice::community_voice_chat_create_or_join(
        State(state.clone()),
        Json(body(&s.same_device, "member", "join")),
    )
    .await
    .expect_err("a community role never outranks a platform ban");
    assert_eq!((err.code, err.message.as_str()), (403, PLATFORM_BANNED_MSG));
    assert!(
        !state
            .voice_db
            .community_users_in_room(community)
            .await
            .unwrap()
            .iter()
            .any(|u| u.address == s.same_device.as_str()),
        "the refused join must not reach the roster"
    );
    let joined = voice::community_voice_chat_create_or_join(
        State(state.clone()),
        Json(body(&s.clean, "member", "join")),
    )
    .await
    .expect("an unbanned wallet joins");
    assert!(joined.0["connection_url"].is_string());
    let roster = state
        .voice_db
        .community_users_in_room(community)
        .await
        .unwrap();
    assert_eq!(roster.len(), 2, "the moderator and the member");
    let member = roster
        .iter()
        .find(|u| u.address == s.clean.as_str())
        .expect("the unbanned wallet is on the roster");
    assert!(!member.is_moderator, "a claimed member is no moderator");
    let unknown: CommunityVoiceChatBody =
        serde_json::from_value(serde_json::json!({ "community_id": Uuid::new_v4().to_string(), "user_address": s.clean.as_str(), "user_role": "owner", "action": "create" })).unwrap();
    assert_eq!(
        voice::community_voice_chat_create_or_join(State(state.clone()), Json(unknown))
            .await
            .unwrap_err()
            .code,
        404,
        "a room only exists for a community this database holds"
    );
    let bad: CommunityVoiceChatBody = serde_json::from_value(
        serde_json::json!({ "community_id": "not-a-uuid", "user_address": s.clean.as_str() }),
    )
    .unwrap();
    assert_eq!(
        voice::community_voice_chat_create_or_join(State(state), Json(bad))
            .await
            .unwrap_err()
            .code,
        400
    );
}

fn meta(realm: &str, scene: &str, device: Option<&str>) -> String {
    let mut m =
        serde_json::json!({ "signer": "dcl:explorer", "realmName": realm, "sceneId": scene });
    if let Some(d) = device {
        m["deviceIdentifier"] = serde_json::json!(d);
    }
    m.to_string()
}

#[tokio::test]
async fn the_scene_adapter_mints_for_a_clean_wallet_and_refuses_platform_and_scene_bans() {
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let key = random_key();
    let me = Address::parse(&address_of(&key)).unwrap();
    let scene_id = format!("bafkrei{}", tag());
    let headers = signed(&key, "post", "/get-scene-adapter", &{
        let mut metadata: serde_json::Value =
            serde_json::from_str(&meta("main", &scene_id, Some("dev-1"))).unwrap();
        metadata["displayName"] = serde_json::json!("Signed Explorer");
        metadata.to_string()
    })
    .await;
    let Json(resp) = handlers::get_scene_adapter(
        State(state.clone()),
        at("/get-scene-adapter"),
        headers.clone(),
        writes_on(),
        axum::body::Bytes::new(),
    )
    .await
    .expect("a clean wallet gets an adapter");
    let claims =
        super::livekit::decode_jwt_payload(resp.adapter.split("access_token=").nth(1).unwrap());
    assert_eq!(claims["name"], "Signed Explorer");
    assert_eq!(claims["video"]["canUpdateOwnMetadata"], true);
    assert_eq!(claims["sub"], me.as_str());
    assert_eq!(claims["video"]["room"], format!("scene:main:{scene_id}"));
    assert_eq!(
        claims["video"]["canPublishSources"],
        serde_json::json!(["microphone"])
    );
    assert_eq!(claims["metadata"], r#"{"isGuest":true}"#);
    let recorded: Option<String> =
        sqlx::query_scalar("SELECT device_id FROM social.player_connections WHERE address = $1")
            .bind(&me)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        recorded.as_deref(),
        Some("dev-1"),
        "the token mint records the connection"
    );

    state
        .moderation
        .scene_ban(&scene_id, &me, &wallet(), &[], "Scene")
        .await
        .unwrap();
    let err = handlers::get_scene_adapter(
        State(state.clone()),
        at("/get-scene-adapter"),
        headers.clone(),
        writes_on(),
        axum::body::Bytes::new(),
    )
    .await
    .unwrap_err();
    assert_eq!((err.code, err.message.as_str()), (403, SCENE_BANNED_MSG));
    let other_scene = signed(
        &key,
        "post",
        "/get-scene-adapter",
        &meta("main", "bafkreiother", None),
    )
    .await;
    assert!(
        handlers::get_scene_adapter(
            State(state.clone()),
            at("/get-scene-adapter"),
            other_scene,
            writes_on(),
            axum::body::Bytes::new()
        )
        .await
        .is_ok(),
        "a scene ban is scoped to its scene"
    );

    state
        .moderation
        .create_ban(CreateBan {
            banned_address: me.clone(),
            banned_by: "m".into(),
            reason: "r".into(),
            custom_message: None,
            banned_device_id: None,
            duration_ms: None,
        })
        .await
        .unwrap();
    let err = handlers::get_scene_adapter(
        State(state.clone()),
        at("/get-scene-adapter"),
        headers,
        writes_on(),
        axum::body::Bytes::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (err.code, err.message.as_str()),
        (403, PLATFORM_BANNED_MSG),
        "the platform ban wins over the scene ban"
    );

    let other = random_key();
    let no_realm = signed(&other, "post", "/get-scene-adapter", r#"{"sceneId":"x"}"#).await;
    assert_eq!(
        handlers::get_scene_adapter(
            State(state.clone()),
            at("/get-scene-adapter"),
            no_realm,
            writes_on(),
            axum::body::Bytes::new()
        )
        .await
        .unwrap_err()
        .code,
        401
    );
    let no_scene = signed(
        &other,
        "post",
        "/get-scene-adapter",
        r#"{"realmName":"main"}"#,
    )
    .await;
    assert_eq!(
        handlers::get_scene_adapter(
            State(state.clone()),
            at("/get-scene-adapter"),
            no_scene,
            writes_on(),
            axum::body::Bytes::new()
        )
        .await
        .unwrap_err()
        .code,
        400
    );
    let scene_signed = signed(
        &other,
        "post",
        "/get-scene-adapter",
        r#"{"signer":"decentraland-kernel-scene","realmName":"main","sceneId":"x"}"#,
    )
    .await;
    assert!(
        handlers::get_scene_adapter(
            State(state),
            at("/get-scene-adapter"),
            scene_signed,
            writes_on(),
            axum::body::Bytes::new()
        )
        .await
        .is_ok(),
        "the adapter route declares no signer allow-list"
    );
}

/// A two-scene resolver that rejects any request other than a parcel of this world.
struct ParcelScenes {
    world: String,
}

#[async_trait::async_trait]
impl crate::interfaces::SceneIdentity for ParcelScenes {
    async fn classify_room(
        &self,
        room: &str,
    ) -> Result<crate::rooms::RoomClassification, crate::rooms::ResolveError> {
        crate::interfaces::test_support::classify_fixture(self, room).await
    }

    async fn resolve(
        &self,
        request: crate::rooms::RoomRequest<'_>,
    ) -> Result<crate::rooms::ResolvedRoom, crate::rooms::ResolveError> {
        use crate::rooms::{ResolvedRoom, RoomRequest};
        use crate::world_identity::{RealmId, SceneId};
        let RoomRequest::Parcel {
            realm: RealmId::World(world),
            parcel,
        } = request
        else {
            panic!("expected a world parcel request")
        };
        assert_eq!(world.display(), self.world);
        let scene = match parcel.to_string().as_str() {
            "0,0" => "bafkreiworldscene",
            "1,1" => "bafkreinewerworldscene",
            _ => {
                return Err(crate::rooms::ResolveError::NoSceneAtParcel {
                    realm: self.world.clone(),
                    parcel: parcel.to_string(),
                });
            }
        };
        Ok(ResolvedRoom {
            realm: RealmId::World(world.clone()),
            scene_id: Some(SceneId::parse(scene).unwrap()),
            deployment: None,
            room: String::new(),
        })
    }
    async fn world_scene(
        &self,
        _: &crate::world_identity::WorldName,
        _: &crate::world_identity::SceneId,
    ) -> Result<Option<crate::rooms::ActiveDeployment>, crate::rooms::ResolveError> {
        panic!("must resolve the requested parcel")
    }
    async fn world_active_scene(
        &self,
        _: &crate::world_identity::WorldName,
    ) -> Result<Option<crate::rooms::ActiveDeployment>, crate::rooms::ResolveError> {
        panic!("must not resolve the newest world scene")
    }
}

#[tokio::test]
async fn undetermined_scene_and_private_message_tokens_request_retry() {
    use axum::response::IntoResponse;
    let pool = lazy_pool();
    pool.close().await;
    let state = state_with(pool, Settings::for_tests());
    let key = random_key();
    let headers = signed(
        &key,
        "post",
        "/get-scene-adapter",
        &meta("main", "bafkreiscene", None),
    )
    .await;
    let scene_error = handlers::get_scene_adapter(
        State(state.clone()),
        at("/get-scene-adapter"),
        headers,
        None,
        axum::body::Bytes::new(),
    )
    .await
    .unwrap_err();
    let headers = signed(
        &key,
        "get",
        "/private-messages/token",
        r#"{"signer":"dcl:explorer"}"#,
    )
    .await;
    let message_error =
        voice::private_messages_token(State(state), at("/private-messages/token"), headers, None)
            .await
            .unwrap_err();
    for error in [scene_error, message_error] {
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        let (_, body) = body_json(response).await;
        assert!(body["error"].is_string());
        assert!(body.get("isBanned").is_none());
    }
}

#[tokio::test]
async fn undetermined_world_token_routes_request_retry() {
    let mut state = comms_state(lazy_pool(), Settings::for_tests());
    state.policy = Arc::new(StubPolicy {
        blocked: Err(crate::AuthorityNotEstablished::store_unavailable(
            "social.bans",
            &sqlx::Error::PoolTimedOut,
        )),
        ..StubPolicy::default()
    });
    let router = super::admission::world_mint::router(Arc::new(state));
    let key = random_key();
    for path in [
        "/worlds/foo.eth/comms",
        "/worlds/foo.eth/scenes/bafkreiscene/comms",
        "/get-comms-adapter/world-foo.eth",
    ] {
        let headers = signed(
            &key,
            "post",
            path,
            r#"{"signer":"dcl:explorer","intent":"dcl:explorer:comms-handshake"}"#,
        )
        .await;
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers;
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        let (_, body) = body_json(response).await;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"], body["message"]);
    }
}

#[tokio::test]
async fn comms_platform_ban_still_denies_world_token_routes_before_resolution() {
    let mut state = comms_state(lazy_pool(), Settings::for_tests());
    state.bans = Arc::new(super::moderation::test_support::StubBanStatus(Ok(Some(
        crate::access::Ban::Platform,
    ))));
    let router = super::admission::world_mint::router(Arc::new(state));
    let key = random_key();
    for path in [
        "/worlds/foo.eth/comms",
        "/worlds/foo.eth/scenes/bafkreiscene/comms",
        "/get-comms-adapter/world-foo.eth",
    ] {
        let headers = signed(
            &key,
            "post",
            path,
            r#"{"signer":"dcl:explorer","intent":"dcl:explorer:comms-handshake"}"#,
        )
        .await;
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers;
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn world_ban_status_invalid_or_unresolved_inputs_are_undetermined() {
    let state = lazy_state(Settings {
        gatekeeper_auth_token: Some("svc".into()),
        ..Settings::for_tests()
    });
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer svc"));
    for (world, parcel, address) in [
        ("foo.eth", "0,0", "invalid".to_string()),
        ("", "0,0", wallet().to_string()),
        ("foo.eth", "invalid", wallet().to_string()),
        ("foo.eth", "0,0", wallet().to_string()),
    ] {
        let error = super::moderation::handlers::world_ban_check(
            State(state.clone()),
            headers.clone(),
            Path((world.into(), parcel.into(), address)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, 503);
    }
}

#[tokio::test]
async fn world_ban_status_store_failure_is_undetermined() {
    let pool = lazy_pool();
    pool.close().await;
    let mut state = comms_state(
        pool,
        Settings {
            gatekeeper_auth_token: Some("svc".into()),
            ..Settings::for_tests()
        },
    );
    state.scenes = Arc::new(ParcelScenes {
        world: "foo.eth".into(),
    });
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer svc"));
    let error = super::moderation::handlers::world_ban_check(
        State(Arc::new(state)),
        headers,
        Path(("foo.eth".into(), "0,0".into(), wallet().to_string())),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, 503);
}

#[tokio::test]
async fn world_ban_status_uses_requested_parcel_and_completed_ban_result() {
    let Some(pool) = scratch().await else { return };
    let mut state = comms_state(
        pool.clone(),
        Settings {
            gatekeeper_auth_token: Some("svc".into()),
            ..Settings::for_tests()
        },
    );
    let world = format!("{}.dcl.eth", tag());
    state.scenes = Arc::new(ParcelScenes {
        world: world.clone(),
    });
    let state = Arc::new(state);
    let user = wallet();
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer svc"));
    let probe = |parcel: &str, address: String| {
        let (state, headers) = (state.clone(), headers.clone());
        let (world, parcel) = (world.clone(), parcel.to_string());
        async move {
            super::moderation::handlers::world_ban_check(
                State(state),
                headers,
                Path((world, parcel, address)),
            )
            .await
            .unwrap()
            .0
        }
    };
    seed_world_scene(&pool, &world, "bafkreiworldscene", &["0,0"]).await;
    seed_world_scene(&pool, &world, "bafkreinewerworldscene", &["1,1"]).await;
    assert_eq!(
        probe("0,0", user.as_str().into()).await,
        serde_json::json!({"isBanned": false})
    );
    state
        .moderation
        .scene_ban("bafkreiworldscene", &user, &wallet(), &[], "Scene")
        .await
        .unwrap();
    assert_eq!(
        probe("0,0", user.as_str().to_uppercase()).await,
        serde_json::json!({"isBanned": true})
    );
    assert_eq!(
        probe("1,1", user.as_str().into()).await,
        serde_json::json!({"isBanned": false})
    );
    let mut wrong = HeaderMap::new();
    wrong.insert("authorization", HeaderValue::from_static("Bearer nope"));
    assert_eq!(
        super::moderation::handlers::world_ban_check(
            State(state),
            wrong,
            Path((world, "0,0".into(), user.as_str().into()))
        )
        .await
        .unwrap_err()
        .code,
        401
    );
}

fn ctx(place_id: &str, realm: &str, scene_id: &str, parcel: Option<&str>) -> RoomContext {
    let mut metadata = serde_json::json!({ "realmName": realm, "sceneId": scene_id });
    if let Some(parcel) = parcel {
        metadata["parcel"] = serde_json::json!(parcel);
    }
    RoomContext::from_metadata(&metadata, place_id)
}

#[tokio::test]
async fn a_mutation_reaches_only_the_rooms_of_the_place_it_is_authorized_on() {
    let Some(pool) = scratch().await else { return };
    let mut state = state_with(pool.clone(), Settings::for_tests());
    let genesis = format!("place-genesis-{}", tag());
    let world_place = format!("place-world-{}", tag());
    let world = format!("{}.dcl.eth", tag());
    let realm = crate::world_identity::RealmId::parse(&world).unwrap();
    let parcel = crate::world_identity::Parcel { x: 0, y: 0 };
    Arc::get_mut(&mut state).unwrap().scenes = Arc::new(StubScenes {
        room: Ok(crate::rooms::ResolvedRoom {
            realm: realm.clone(),
            scene_id: Some(crate::world_identity::SceneId::parse("bafkreiworld").unwrap()),
            deployment: None,
            room: crate::rooms::worlds_service_scene_room(
                &crate::world_identity::WorldName::parse(&world).unwrap(),
                &crate::world_identity::SceneId::parse("bafkreiworld").unwrap(),
            ),
        }),
        scene: Ok(Some(crate::rooms::ActiveDeployment {
            title: None,
            scene_id: crate::world_identity::SceneId::parse("bafkreiworld").unwrap(),
            parcels: [parcel].into_iter().collect(),
            base: parcel,
            namespace: crate::world_identity::SceneNamespace::new(&realm, parcel, None),
            local: true,
        })),
        ..StubScenes::default()
    });
    seed_place(
        &pool,
        &genesis,
        serde_json::json!({ "world": false, "positions": ["10,20", "10,21"] }),
        "10,20",
    )
    .await;
    seed_place(
        &pool,
        &world_place,
        serde_json::json!({ "world": true, "world_name": world, "positions": ["0,0"] }),
        "0,0",
    )
    .await;
    seed_world_scene(&pool, &world, "bafkreiworld", &["0,0"]).await;

    let rooms = scene::resolve_rooms(
        &state,
        &ctx(&genesis, "main", "bafkreiabc", Some("10,21")),
        &PlaceLookup::new(&state, &genesis),
    )
    .await
    .unwrap();
    assert_eq!(rooms, vec!["scene:main:bafkreiabc".to_string()]);
    let err = scene::resolve_rooms(
        &state,
        &ctx(&genesis, "main", "bafkreiabc", Some("99,99")),
        &PlaceLookup::new(&state, &genesis),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code, 400,
        "a parcel outside the authorized place is a mismatch"
    );
    let err = scene::resolve_rooms(
        &state,
        &ctx(&genesis, &world, "bafkreiabc", None),
        &PlaceLookup::new(&state, &genesis),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code, 400,
        "a world context cannot aim at a Genesis place"
    );
    let rooms = scene::resolve_rooms(
        &state,
        &ctx(&world_place, &world, &world, None),
        &PlaceLookup::new(&state, &world_place),
    )
    .await
    .unwrap();
    assert_eq!(
        rooms,
        vec![
            format!("world-{world}"),
            crate::rooms::worlds_service_scene_room(
                &crate::world_identity::WorldName::parse(&world).unwrap(),
                &crate::world_identity::SceneId::parse("bafkreiworld").unwrap()
            )
        ],
        "a world-shaped scene id resolves to the active scene"
    );
    let rooms = scene::resolve_rooms(
        &state,
        &RoomContext::from_metadata(&serde_json::json!({}), &world_place),
        &PlaceLookup::new(&state, &world_place),
    )
    .await
    .unwrap();
    assert_eq!(
        rooms.len(),
        2,
        "no realm in the metadata falls back to the place record"
    );
    let err = scene::resolve_rooms(
        &state,
        &ctx("no-such-place", "main", "bafkreiabc", Some("10,20")),
        &PlaceLookup::new(&state, "no-such-place"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, 400, "an unknown place cannot be bound to a room");
    let unconfigured = state_with(
        pool,
        Settings {
            livekit: super::LiveKit {
                configured: false,
                ..Settings::for_tests().livekit
            },
            ..Settings::for_tests()
        },
    );
    assert!(
        scene::resolve_rooms(
            &unconfigured,
            &ctx("no-such-place", "main", "x", None),
            &PlaceLookup::new(&unconfigured, "no-such-place")
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn the_moderator_routes_need_the_token_or_a_listed_wallet() {
    let Some(pool) = scratch().await else { return };
    let listed = Address::parse(&address_of(KEY)).unwrap();
    let state = state_with(
        pool.clone(),
        Settings {
            moderator_token: Some("mod-token".into()),
            moderator_addresses: vec![listed.clone()],
            ..Settings::for_tests()
        },
    );
    let app = super::router(state.clone());
    let victim = wallet();
    let path = format!("/users/{}/bans", victim.as_str());
    let ban_body = serde_json::json!({ "reason": "spam", "customMessage": "bye" });
    assert_eq!(
        post_json(&app, &path, None, ban_body.clone()).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_json(&app, &path, Some("mod-token"), ban_body.clone()).await,
        StatusCode::BAD_REQUEST,
        "a token write must name its moderator"
    );
    let created = app
        .clone()
        .oneshot(
            Request::post(format!("{path}?moderator=alice.dcl"))
                .header("authorization", "Bearer mod-token")
                .header("content-type", "application/json")
                .body(Body::from(ban_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_json(created).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["data"]["bannedBy"], "alice.dcl");
    assert_eq!(body["data"]["customMessage"], "bye");
    let again = post_json(
        &app,
        &format!("{path}?moderator=alice.dcl"),
        Some("mod-token"),
        ban_body,
    )
    .await;
    assert_eq!(again, StatusCode::CONFLICT);

    let public = app
        .clone()
        .oneshot(Request::get(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, body) = body_json(public).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["isBanned"], true);
    assert!(body["data"]["ban"].get("bannedDeviceId").is_none());

    let signed_list = signed(KEY, "get", "/bans", "{}").await;
    let mut req = Request::get("/bans");
    for (k, v) in signed_list.iter() {
        req = req.header(k, v);
    }
    let (status, body) = body_json(
        app.clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b["bannedAddress"] == victim.as_str())
    );
    let outsider = signed(OUTSIDER_KEY, "get", "/bans", "{}").await;
    let mut req = Request::get("/bans");
    for (k, v) in outsider.iter() {
        req = req.header(k, v);
    }
    assert_eq!(
        app.clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let warn = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/users/{}/warnings?moderator=alice.dcl",
                victim.as_str()
            ))
            .header("authorization", "Bearer mod-token")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"reason":"be nice"}"#))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(warn.status(), StatusCode::CREATED);
    let warnings = app
        .clone()
        .oneshot(
            Request::get(format!("/users/{}/warnings", victim.as_str()))
                .header("authorization", "Bearer mod-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_json(warnings).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["reason"], "be nice");

    let lifted = app
        .clone()
        .oneshot(
            Request::delete(format!("{path}?moderator=alice.dcl"))
                .header("authorization", "Bearer mod-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lifted.status(), StatusCode::NO_CONTENT);
    let lifted_again = app
        .oneshot(
            Request::delete(format!("{path}?moderator=alice.dcl"))
                .header("authorization", "Bearer mod-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_json(lifted_again).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "Not Found");
}

#[tokio::test]
async fn the_private_messages_token_carries_the_merged_privacy_default_and_the_patch_moves_it() {
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let key = random_key();
    let me = Address::parse(&address_of(&key)).unwrap();
    let headers = signed(
        &key,
        "get",
        "/private-messages/token",
        r#"{"signer":"dcl:explorer"}"#,
    )
    .await;
    let Json(resp) = voice::private_messages_token(
        State(state.clone()),
        at("/private-messages/token"),
        headers.clone(),
        writes_on(),
    )
    .await
    .unwrap();
    let claims = super::livekit::decode_jwt_payload(
        resp["adapter"]
            .as_str()
            .unwrap()
            .split("access_token=")
            .nth(1)
            .unwrap(),
    );
    assert_eq!(claims["video"]["room"], "private-messages");
    assert_eq!(claims["video"]["canPublish"], false);
    assert_eq!(
        claims["metadata"], r#"{"private_messages_privacy":"all"}"#,
        "a wallet without settings reads all (upstream social-service-ea and the old gatekeeper)"
    );
    let Json(patched) = voice::patch_private_messages_privacy(
        State(state.clone()),
        Path(me.as_str().to_uppercase()),
        Json(voice::PrivateMessagesPrivacyBody {
            private_messages_privacy: Some("ONLY_FRIENDS".into()),
        }),
    )
    .await
    .unwrap();
    assert_eq!(patched["private_messages_privacy"], "only_friends");
    let Json(resp) = voice::private_messages_token(
        State(state.clone()),
        at("/private-messages/token"),
        headers.clone(),
        writes_on(),
    )
    .await
    .unwrap();
    let claims = super::livekit::decode_jwt_payload(
        resp["adapter"]
            .as_str()
            .unwrap()
            .split("access_token=")
            .nth(1)
            .unwrap(),
    );
    assert_eq!(
        claims["metadata"],
        r#"{"private_messages_privacy":"only_friends"}"#
    );
    let bad = voice::patch_private_messages_privacy(
        State(state.clone()),
        Path(me.as_str().into()),
        Json(voice::PrivateMessagesPrivacyBody {
            private_messages_privacy: Some("friends".into()),
        }),
    )
    .await;
    assert_eq!(bad.unwrap_err().code, 400);
    let unsigned_signer = signed(&key, "get", "/private-messages/token", "{}").await;
    assert_eq!(
        voice::private_messages_token(
            State(state.clone()),
            at("/private-messages/token"),
            unsigned_signer,
            writes_on(),
        )
        .await
        .unwrap_err()
        .code,
        400,
        "the DM token is explorer-signed only"
    );
    state
        .moderation
        .create_ban(CreateBan {
            banned_address: me.clone(),
            banned_by: "m".into(),
            reason: "r".into(),
            custom_message: None,
            banned_device_id: None,
            duration_ms: None,
        })
        .await
        .unwrap();
    let err = voice::private_messages_token(
        State(state),
        at("/private-messages/token"),
        headers,
        writes_on(),
    )
    .await
    .unwrap_err();
    assert_eq!((err.code, err.message.as_str()), (403, PLATFORM_BANNED_MSG));
}

/// The social switch as its gate hands it to a handler, on.
fn writes_on() -> Option<axum::Extension<crate::writes::Switch>> {
    Some(axum::Extension(crate::writes::Switch(true)))
}

async fn connections_for(pool: &PgPool, address: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM social.player_connections WHERE address = $1")
        .bind(address)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The DM token's connection record is a social write: off or held, the
/// token still mints and nothing is recorded; on with fresh stamps, the
/// connection is stored.
#[tokio::test]
async fn the_private_messages_token_records_the_connection_only_while_social_writes_go_through() {
    use crate::shared::writes::restore::{lab::StampLab, scoped};
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let key = random_key();
    let me = Address::parse(&address_of(&key)).unwrap();
    let headers = signed(
        &key,
        "get",
        "/private-messages/token",
        r#"{"signer":"dcl:explorer"}"#,
    )
    .await;
    let lab = StampLab::new();
    for switch in [
        None,
        Some(axum::Extension(crate::writes::Switch(false))),
        writes_on(),
    ] {
        let held = scoped(
            lab.stamps.clone(),
            voice::private_messages_token(
                State(state.clone()),
                at("/private-messages/token"),
                headers.clone(),
                switch,
            ),
        )
        .await;
        assert!(held.is_ok(), "the token mints whatever the switch");
    }
    assert_eq!(
        connections_for(&pool, me.as_str()).await,
        0,
        "no stamps: nothing recorded"
    );
    let lab = StampLab::fresh();
    let minted = scoped(
        lab.stamps.clone(),
        voice::private_messages_token(
            State(state.clone()),
            at("/private-messages/token"),
            headers.clone(),
            writes_on(),
        ),
    )
    .await;
    assert!(minted.is_ok());
    assert_eq!(connections_for(&pool, me.as_str()).await, 1);
}

/// The scene adapter's connection record is a social write too: off or
/// held, the adapter still mints and nothing is recorded.
#[tokio::test]
async fn the_scene_adapter_records_the_connection_only_while_social_writes_go_through() {
    use crate::shared::writes::restore::{lab::StampLab, scoped};
    let Some(pool) = scratch().await else { return };
    let state = state_with(pool.clone(), Settings::for_tests());
    let key = random_key();
    let me = Address::parse(&address_of(&key)).unwrap();
    let scene_id = format!("bafkrei{}", tag());
    let headers = signed(
        &key,
        "post",
        "/get-scene-adapter",
        &meta("main", &scene_id, Some("dev-held")),
    )
    .await;
    let mint = |lab: &StampLab, switch| {
        scoped(
            lab.stamps.clone(),
            handlers::get_scene_adapter(
                State(state.clone()),
                at("/get-scene-adapter"),
                headers.clone(),
                switch,
                axum::body::Bytes::new(),
            ),
        )
    };
    let held = StampLab::new();
    for switch in [
        None,
        Some(axum::Extension(crate::writes::Switch(false))),
        writes_on(),
    ] {
        assert!(
            mint(&held, switch).await.is_ok(),
            "the adapter mints whatever the switch"
        );
    }
    assert_eq!(
        connections_for(&pool, me.as_str()).await,
        0,
        "held or off: nothing recorded"
    );
    assert!(mint(&StampLab::fresh(), writes_on()).await.is_ok());
    assert_eq!(connections_for(&pool, me.as_str()).await, 1);
}

#[tokio::test]
async fn a_private_call_is_exclusive_per_wallet_and_follows_the_webhook_lifecycle() {
    let Some(pool) = scratch().await else { return };
    let (settings, media) = webhook_voice_settings().await;
    let state = state_with(pool.clone(), settings);
    let (a, b, c) = (wallet(), wallet(), wallet());
    let call = Uuid::new_v4();
    assert!(
        state
            .voice_db
            .create_private_call(call, &a, &b)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        state.voice_db.is_private_room_active(call).await.unwrap(),
        "two fresh not_connected participants count as present within the initial TTL"
    );
    assert_eq!(
        state
            .voice_db
            .join_user_to_active_call(a.as_str(), call)
            .await
            .unwrap(),
        PrivateJoin::Joined { old_call: call }
    );
    assert_eq!(
        state
            .voice_db
            .join_user_to_active_call(c.as_str(), call)
            .await
            .unwrap(),
        PrivateJoin::NotInRoom
    );
    let second = Uuid::new_v4();
    let displaced = state
        .voice_db
        .create_private_call(second, &a, &c)
        .await
        .unwrap();
    assert_eq!(
        displaced,
        vec![call],
        "a wallet is in at most one call: the old one is torn down"
    );
    assert_eq!(
        state.voice_db.active_call_of(&a).await.unwrap(),
        Some(second)
    );
    assert_eq!(state.voice_db.active_call_of(&b).await.unwrap(), None);
    voice::handle_private_participant_joined(&state, a.as_str(), second)
        .await
        .unwrap();
    voice::handle_private_participant_left(
        &state,
        a.as_str(),
        second,
        voice::DisconnectReason::UnknownReason,
    )
    .await
    .unwrap();
    assert_eq!(
        state
            .voice_db
            .participants_in(second)
            .await
            .unwrap()
            .iter()
            .find(|p| p.address == a.as_str())
            .unwrap()
            .status,
        "connection_interrupted"
    );
    voice::handle_private_participant_left(
        &state,
        c.as_str(),
        second,
        voice::DisconnectReason::ClientInitiated,
    )
    .await
    .unwrap();
    assert!(!state.voice_db.is_private_room_active(second).await.unwrap());
    let to_destroy = state.voice_db.delete_expired_private_calls().await.unwrap();
    assert!(
        !to_destroy.contains(&second),
        "a room a participant left voluntarily was already torn down by the leave"
    );
    assert!(
        state
            .voice_db
            .participants_in(second)
            .await
            .unwrap()
            .is_empty(),
        "the left call and its participants are swept"
    );
    assert!(matches!(
        state
            .voice_db
            .delete_private_call_if_member(Some(second), &a)
            .await,
        Err(super::voice::db::DeleteRoomError::RoomDoesNotExist)
    ));
    let third = Uuid::new_v4();
    state
        .voice_db
        .create_private_call(third, &a, &b)
        .await
        .unwrap();
    let mut ended = voice::end_private_voice_chat_by(&state, &third.to_string(), &b)
        .await
        .unwrap()
        .users;
    ended.sort();
    let mut expected = vec![a.as_str().to_string(), b.as_str().to_string()];
    expected.sort();
    assert_eq!(ended, expected);
    assert_eq!(
        voice::end_private_voice_chat_by(&state, &third.to_string(), &b)
            .await
            .unwrap_err()
            .code,
        404
    );
    media.abort();
}

#[tokio::test]
async fn a_community_room_lives_while_a_moderator_is_present_and_sweeps_otherwise() {
    let Some(pool) = scratch().await else { return };
    let directory = Arc::new(crate::interfaces::test_support::StubDirectory::default());
    let (settings, media) = webhook_voice_settings().await;
    let mut state = comms_state(pool.clone(), settings);
    state.social = directory.clone();
    let state = Arc::new(state);
    let community = seed_community(&pool).await;
    let (moderator, member) = (wallet(), wallet());
    sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'member')")
        .bind(community)
        .bind(&member)
        .execute(&pool)
        .await
        .unwrap();
    state
        .voice_db
        .join_user_to_community_room(community, &moderator, true)
        .await
        .unwrap();
    state
        .voice_db
        .join_user_to_community_room(community, &member, false)
        .await
        .unwrap();
    voice::handle_community_participant_joined(
        &state,
        moderator.as_str(),
        community,
        Some("PA_mod"),
    )
    .await
    .unwrap();
    voice::handle_community_participant_joined(
        &state,
        member.as_str(),
        community,
        Some("PA_member"),
    )
    .await
    .unwrap();
    let Json(status) =
        voice::community_voice_chat_status(State(state.clone()), Path(community.to_string()))
            .await
            .unwrap();
    assert_eq!(
        (
            status.active,
            status.participant_count,
            status.moderator_count
        ),
        (true, 2, 1)
    );
    let bulk = state
        .voice_db
        .bulk_community_status(&[community, CommunityId::new()])
        .await
        .unwrap();
    assert_eq!(bulk.get(&community), Some(&(2, 1)));
    assert!(
        state
            .voice_db
            .is_user_in_any_community_voice_chat(&member)
            .await
            .unwrap()
    );
    assert!(
        state
            .voice_db
            .all_active_community_voice_chats()
            .await
            .unwrap()
            .iter()
            .any(|c| c.community_id == community)
    );

    voice::handle_community_participant_left(
        &state,
        moderator.as_str(),
        community,
        voice::DisconnectReason::ClientInitiated,
        Some("PA_stale"),
        None,
        "stale-leave",
    )
    .await
    .unwrap();
    assert_eq!(
        state
            .voice_db
            .community_users_in_room(community)
            .await
            .unwrap()
            .len(),
        2,
        "a leave from a previous session is ignored"
    );
    voice::handle_community_participant_left(
        &state,
        moderator.as_str(),
        community,
        voice::DisconnectReason::ClientInitiated,
        Some("PA_mod"),
        None,
        "mod-leave",
    )
    .await
    .unwrap();
    assert!(
        state
            .voice_db
            .community_users_in_room(community)
            .await
            .unwrap()
            .is_empty(),
        "the last moderator leaving tears the room down"
    );
    let events = directory.community_events.lock().unwrap();
    assert_eq!(
        events.len(),
        1,
        "the ended event crosses the social seam once"
    );
    assert_eq!(events[0]["subType"], "community-streaming-ended");
    drop(events);

    state
        .voice_db
        .join_user_to_community_room(community, &member, false)
        .await
        .unwrap();
    assert!(
        state
            .voice_db
            .delete_expired_community_voice_chats()
            .await
            .unwrap()
            .contains(&community),
        "a room without a moderator row is swept"
    );
    let Json(status) =
        voice::community_voice_chat_status(State(state.clone()), Path(community.to_string()))
            .await
            .unwrap();
    assert!(!status.active);
    voice::expire_voice_chats(&state).await.unwrap();
    media.abort();
}

/// What the worlds access log is in `serve`: records every event the
/// webhook hands over, so the test sees that only verified ones arrive.
#[derive(Default)]
struct RecordingSink(std::sync::Mutex<Vec<(String, String, String)>>);

impl super::events::RoomEventSink for RecordingSink {
    fn observe_room_event(&self, event: &crate::protocol::bus::room_events::RoomEvent) {
        use crate::protocol::bus::room_events::RoomEventKind;
        let kind = match event.kind() {
            RoomEventKind::Joined => "participant_joined",
            RoomEventKind::Left => "participant_left",
            _ => return,
        };
        self.0
            .lock()
            .unwrap()
            .push((kind.to_string(), event.room.clone(), event.identity.clone()));
    }
}

async fn post_webhook(app: &axum::Router, authorization: Option<&str>, body: &[u8]) -> StatusCode {
    let mut req =
        Request::post("/livekit-webhook").header("content-type", "application/webhook+json");
    if let Some(a) = authorization {
        req = req.header("authorization", a);
    }
    app.clone()
        .oneshot(req.body(Body::from(body.to_vec())).unwrap())
        .await
        .unwrap()
        .status()
}

/// Tests with valid lobby events supply explicit authoritative catalogue identity.
fn known_lobby_scenes(name: &str) -> StubScenes {
    let world = crate::world_identity::WorldName::parse(name).unwrap();
    StubScenes {
        room: Ok(crate::rooms::ResolvedRoom {
            room: crate::rooms::worlds_service_world_room(&world),
            realm: crate::world_identity::RealmId::World(world),
            scene_id: None,
            deployment: None,
        }),
        scene: Ok(None),
    }
}

/// One handler serves the SFU (`worlds::core::webhook` had a second, always
/// verifying, while this one skipped verification on the dev credentials and
/// still fed the worlds sink): it verifies always and answers 503 while
/// LiveKit is not configured, so the sink only ever sees a verified event.
#[tokio::test]
async fn the_livekit_webhook_verifies_always_and_feeds_the_worlds_sink_verified_events_only() {
    use super::webhook::test_support::webhook_jwt;
    let body = serde_json::json!({ "id": "verified-join", "createdAt": 1718900000, "event": "participant_joined", "room": { "name": "world-w.dcl.eth" }, "participant": { "identity": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "sid": "PA_verified" } });
    let body = serde_json::to_vec(&body).unwrap();
    let sink = Arc::new(RecordingSink::default());
    let settings = Settings::for_tests();
    let (key, secret) = (
        settings.livekit.api_key.clone(),
        settings.livekit.api_secret.clone(),
    );
    let pool = PgPool::connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("a lazy pool never connects");
    let state = Arc::new(
        CommsState::new(
            pool.clone(),
            Outbound::for_tests(),
            settings,
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(known_lobby_scenes("w.dcl.eth")),
            Arc::new(crate::presence::Presence::new()),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        )
        .with_room_event_sink(sink.clone()),
    );
    let app = super::router(state);

    assert_eq!(
        post_webhook(&app, None, &body).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_webhook(
            &app,
            Some(&webhook_jwt(&body, &key, "not-the-livekit-secret")),
            &body
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert!(
        sink.0.lock().unwrap().is_empty(),
        "an unverified event never reaches the worlds sink"
    );
    assert_eq!(
        post_webhook(&app, Some(&webhook_jwt(&body, &key, &secret)), &body).await,
        StatusCode::OK
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(
            "participant_joined".to_string(),
            "world-w.dcl.eth".to_string(),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
        )]
    );

    let dev = Settings {
        livekit: super::LiveKit {
            configured: false,
            ..Settings::for_tests().livekit
        },
        ..Settings::for_tests()
    };
    let dev_sink = Arc::new(RecordingSink::default());
    let dev_state = Arc::new(
        CommsState::new(
            pool,
            Outbound::for_tests(),
            dev,
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(known_lobby_scenes("w.dcl.eth")),
            Arc::new(crate::presence::Presence::new()),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        )
        .with_room_event_sink(dev_sink.clone()),
    );
    let dev_app = super::router(dev_state);
    assert_eq!(
        post_webhook(&dev_app, None, &body).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "the dev-credential fallback signs nothing a real SFU sends"
    );
    assert_eq!(
        post_webhook(&dev_app, Some(&webhook_jwt(&body, &key, &secret)), &body).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(dev_sink.0.lock().unwrap().is_empty());
}

/// What worlds' `CoreState` is in `serve` since the webhook split: the
/// access-log half only, one `(action, world, identity)` row per join or
/// leave on a worlds room.
#[derive(Default)]
struct AccessLogSink(std::sync::Mutex<Vec<(String, String, String)>>);

impl super::events::RoomEventSink for AccessLogSink {
    fn observe_room_event(&self, event: &crate::protocol::bus::room_events::RoomEvent) {
        use crate::protocol::bus::room_events::{RoomEventKind, RoomType};
        if event.room_type() != RoomType::World {
            return;
        }
        let action = match event.kind() {
            RoomEventKind::Joined => "join",
            RoomEventKind::Left => "leave",
            _ => return,
        };
        self.0.lock().unwrap().push((
            action.to_string(),
            event.realm_name.clone(),
            event.identity.clone(),
        ));
    }
}

/// The webhook feeds the live presence itself, then hands the event to the
/// access-log sink: a join counts the wallet in the world and logs one
/// `join`, its leave drops it and logs one `leave`.
#[tokio::test]
async fn webhook_invalidates_occupancy_and_preserves_access_log_ordering() {
    use super::webhook::test_support::webhook_jwt;
    use crate::world_identity::WorldName;
    let room = crate::rooms::worlds_service_world_room(&WorldName::parse("alice.dcl.eth").unwrap());
    let event = |kind: &str, identity: &str| {
        serde_json::to_vec(&serde_json::json!({ "id": format!("{kind}:{identity}"), "createdAt": 1718900000, "event": kind, "room": { "name": room }, "participant": { "identity": identity, "sid": format!("PA_{identity}"), "metadata": "{}" } })).unwrap()
    };
    let settings = Settings::for_tests();
    let (key, secret) = (
        settings.livekit.api_key.clone(),
        settings.livekit.api_secret.clone(),
    );
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(50))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("a lazy pool never connects");
    let sink = Arc::new(AccessLogSink::default());
    let state = Arc::new(
        CommsState::new(
            pool,
            Outbound::for_tests(),
            settings,
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(known_lobby_scenes("alice.dcl.eth")),
            Arc::new(crate::presence::Presence::new()),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        )
        .with_room_event_sink(sink.clone()),
    );
    let app = super::router(state.clone());
    let row = |action: &str, identity: &str| {
        (
            action.to_string(),
            "alice.dcl.eth".to_string(),
            identity.to_string(),
        )
    };

    let joined = event("participant_joined", "0xabc");
    let first_jwt = webhook_jwt(&joined, &key, &secret);
    let (first, duplicate) = tokio::join!(
        post_webhook(&app, Some(&first_jwt), &joined),
        post_webhook(&app, Some(&first_jwt), &joined)
    );
    assert_eq!((first, duplicate), (StatusCode::OK, StatusCode::OK));
    assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
    assert_eq!(sink.0.lock().unwrap().as_slice(), &[row("join", "0xabc")]);
    // Concurrent redelivery of one verified event applies no second access row.
    let jwt = webhook_jwt(&joined, &key, &secret);
    let (a, b) = tokio::join!(
        post_webhook(&app, Some(&jwt), &joined),
        post_webhook(&app, Some(&jwt), &joined)
    );
    assert_eq!((a, b), (StatusCode::OK, StatusCode::OK));
    assert_eq!(sink.0.lock().unwrap().as_slice(), &[row("join", "0xabc")]);

    // Server events still reach the access sink without consuming a player's seat.
    for kind in ["participant_joined", "participant_left"] {
        let body = event(kind, "authoritative-server");
        assert_eq!(
            post_webhook(&app, Some(&webhook_jwt(&body, &key, &secret)), &body).await,
            StatusCode::OK
        );
        assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
    }
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[
            row("join", "0xabc"),
            row("join", "authoritative-server"),
            row("leave", "authoritative-server")
        ]
    );

    let left = event("participant_left", "0xabc");
    assert_eq!(
        post_webhook(&app, Some(&webhook_jwt(&left, &key, &secret)), &left).await,
        StatusCode::OK
    );
    assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[
            row("join", "0xabc"),
            row("join", "authoritative-server"),
            row("leave", "authoritative-server"),
            row("leave", "0xabc")
        ]
    );
    // An unseen join from the same second cannot resurrect a SID that left.
    let stale = serde_json::to_vec(&serde_json::json!({"id": "late-join",
        "createdAt": 1718900000, "event": "participant_joined",
        "room": {"name": room}, "participant": {"identity": "0xabc", "sid": "PA_0xabc", "metadata": "{}"}}))
    .unwrap();
    assert_eq!(
        post_webhook(&app, Some(&webhook_jwt(&stale, &key, &secret)), &stale).await,
        StatusCode::OK
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[
            row("join", "0xabc"),
            row("join", "authoritative-server"),
            row("leave", "authoritative-server"),
            row("leave", "0xabc")
        ]
    );
    assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
    let invalid = br#"{"event":"participant_joined"}"#;
    assert_eq!(
        post_webhook(&app, Some(&webhook_jwt(invalid, &key, &secret)), invalid).await,
        StatusCode::BAD_REQUEST
    );
    let non_world = serde_json::to_vec(
        &serde_json::json!({"id": "non-world", "createdAt": 1718900001,
        "event": "participant_joined", "room": {"name": "unrelated"},
        "participant": {"identity": "0xabc", "sid": "PA_other"}}),
    )
    .unwrap();
    assert_eq!(
        post_webhook(
            &app,
            Some(&webhook_jwt(&non_world, &key, &secret)),
            &non_world
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[
            row("join", "0xabc"),
            row("join", "authoritative-server"),
            row("leave", "authoritative-server"),
            row("leave", "0xabc")
        ]
    );

    // A real dispatch failure (unavailable database) is non-2xx and is not
    // recorded as applied, so a later LiveKit attempt can perform the work.
    let ingress = serde_json::to_vec(
        &serde_json::json!({"id": "retry-ingress", "createdAt": 1718900002,
        "event": "ingress_started", "ingressInfo": {"ingressId": "IN_test"}}),
    )
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            post_webhook(&app, Some(&webhook_jwt(&ingress, &key, &secret)), &ingress).await,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    // SID ordering still gates sink delivery. No webhook alone establishes
    // a successful complete membership observation.
    // Equal-second joins choose the same current SID in either arrival order.
    for (case, deliveries) in [
        vec![
            ("join-a", "participant_joined", "PA_A", 100),
            ("join-b", "participant_joined", "PA_B", 200),
            ("left-a", "participant_left", "PA_A", 150),
        ],
        vec![
            ("join-a", "participant_joined", "PA_A", 300),
            ("join-b", "participant_joined", "PA_B", 300),
            ("left-a", "participant_left", "PA_A", 300),
        ],
        vec![
            ("join-b", "participant_joined", "PA_B", 400),
            ("join-a", "participant_joined", "PA_A", 400),
            ("left-a", "participant_left", "PA_A", 400),
        ],
    ]
    .into_iter()
    .enumerate()
    {
        for (id, kind, sid, at) in deliveries {
            let body = serde_json::to_vec(&serde_json::json!({"id": format!("case{case}-{id}"), "createdAt": at,
                "event": kind, "room": {"name": room}, "participant": {"identity": format!("wallet{case}"), "sid": format!("{sid}-{case}")}})).unwrap();
            assert_eq!(
                post_webhook(&app, Some(&webhook_jwt(&body, &key, &secret)), &body).await,
                StatusCode::OK
            );
            assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
        }
        let end = serde_json::to_vec(&serde_json::json!({"id": format!("end{case}"), "createdAt": 500,
            "event": "participant_left", "room": {"name": room}, "participant": {"identity": format!("wallet{case}"), "sid": format!("PA_B-{case}")}})).unwrap();
        assert_eq!(
            post_webhook(&app, Some(&webhook_jwt(&end, &key, &secret)), &end).await,
            StatusCode::OK
        );
        assert_eq!(state.presence.world_participant_count("alice.dcl.eth"), 0);
    }
    // Exact host exclusion also separates coordinator keys from retained case variants.
    let before = sink.0.lock().unwrap().len();
    for (index, kind, identity, expected) in [
        (0, "participant_joined", "AUTHORITATIVE-SERVER", 0),
        (1, "participant_joined", "authoritative-server", 0),
        (2, "participant_left", "authoritative-server", 0),
        (3, "participant_left", "AUTHORITATIVE-SERVER", 0),
    ] {
        let body = serde_json::to_vec(&serde_json::json!({
            "id": format!("case-collision-{index}"), "createdAt": 1718900010 + index,
            "event": kind, "room": {"name": room},
            "participant": {"identity": identity, "sid": format!("PA_case_{identity}"), "metadata": "{}"}
        })).unwrap();
        assert_eq!(
            post_webhook(&app, Some(&webhook_jwt(&body, &key, &secret)), &body).await,
            StatusCode::OK
        );
        assert_eq!(
            state.presence.world_participant_count("alice.dcl.eth"),
            expected
        );
    }
    assert_eq!(
        &sink.0.lock().unwrap()[before..],
        &[
            row("join", "AUTHORITATIVE-SERVER"),
            row("join", "authoritative-server"),
            row("leave", "authoritative-server"),
            row("leave", "AUTHORITATIVE-SERVER")
        ]
    );
}

/// Every world mint uses the same player count: the server takes no slot,
/// while a streamer or Cast participant fills the remaining capacity.
#[tokio::test]
async fn world_mint_refuses_at_capacity() {
    use crate::rooms::ResolvedRoom;
    use crate::world_identity::{RealmId, WorldName};
    let world = WorldName::parse("alice.dcl.eth").unwrap();
    let room = crate::rooms::worlds_service_world_room(&world);
    let scenes = StubScenes {
        room: Ok(ResolvedRoom {
            realm: RealmId::World(world.clone()),
            scene_id: None,
            deployment: None,
            room: room.clone(),
        }),
        ..StubScenes::default()
    };
    let presence = Arc::new(crate::presence::Presence::new());
    let pool = PgPool::connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("a lazy pool never connects");
    let state = Arc::new(
        CommsState::new(
            pool,
            Outbound::for_tests(),
            Settings::for_tests(),
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(scenes),
            presence.clone(),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        )
        .with_ban_status(Arc::new(super::moderation::test_support::StubBanStatus(
            Ok(None),
        )))
        .with_max_users_per_world(2),
    );
    let app = super::admission::world_mint::router(state);
    let scene_room = "w1:d:alice.dcl.ethbafkreiscene";
    let paths = [
        "/worlds/alice.dcl.eth/comms",
        "/worlds/alice.dcl.eth/scenes/bafkreiscene/comms",
        "/get-comms-adapter/world-alice.dcl.eth",
    ];
    let mint = |path: String| {
        let app = app.clone();
        async move {
            let mut req = Request::post(&path).body(Body::empty()).unwrap();
            let metadata = if path.starts_with("/get-comms-adapter/") {
                r#"{"signer":"dcl:explorer","intent":"dcl:explorer:comms-handshake"}"#
            } else {
                "{}"
            };
            req.headers_mut()
                .extend(signed(KEY, "post", &path, metadata).await);
            body_json(app.oneshot(req).await.unwrap()).await
        }
    };

    presence.peer_joined("0x01", "alice.dcl.eth", &room, false);
    presence.peer_joined("0x01", "alice.dcl.eth", scene_room, true);
    presence.peer_joined("authoritative-server", "alice.dcl.eth", &room, false);
    for path in paths {
        let (status, body) = mint(path.to_string()).await;
        assert_eq!(status, StatusCode::OK, "below capacity at {path}: {body}");
    }

    for identity in [
        "0x02",
        "11111111-1111-4111-8111-111111111111-streamer",
        "stream:example",
        "watch:example",
    ] {
        presence.peer_joined(identity, "alice.dcl.eth", &room, false);
        presence.peer_joined(identity, "alice.dcl.eth", scene_room, true);
        for path in paths {
            let (status, body) = mint(path.to_string()).await;
            assert_eq!(
                (status, body["message"].as_str()),
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Some("World \"alice.dcl.eth\" has reached its maximum capacity.")
                ),
                "{identity} fills capacity at {path}: {body}"
            );
        }
        presence.peer_left(identity, &room, false);
        presence.peer_left(identity, scene_room, true);
    }
}

/// The one revocation entry point (`CommsState::revoke`, gatekeeper-change-5)
/// with LiveKit not configured: every arm returns at once, before the task
/// that would reach LiveKit is spawned, and logs the debug line worlds' sink
/// logged. The configured arms (`live_world_rooms` + `kick`,
/// `readmit_world_rooms`, `remove_participant_from_all_rooms`) are not
/// exercised: this crate has no LiveKit room-service double.
#[tokio::test(flavor = "current_thread")]
async fn revoke_maps_each_arm_to_its_livekit_call() {
    use std::io::Write;
    use std::sync::Mutex;

    use super::RoomRevocation;
    use crate::world_identity::WorldName;

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let state = lazy_state(Settings {
        livekit: super::LiveKit {
            configured: false,
            ..Settings::for_tests().livekit
        },
        ..Settings::for_tests()
    });
    let world = WorldName::parse("alice.dcl.eth").unwrap();
    let address = wallet();
    let runtime = tokio::runtime::Handle::current();
    for (revocation, variant) in [
        (
            RoomRevocation::WorldAddress {
                world: world.clone(),
                address: address.clone(),
            },
            "WorldAddress",
        ),
        (
            RoomRevocation::WorldAccess {
                world: world.clone(),
            },
            "WorldAccess",
        ),
        (
            RoomRevocation::Everywhere {
                address: address.clone(),
            },
            "Everywhere",
        ),
    ] {
        let tasks = runtime.metrics().num_alive_tasks();
        state.revoke(revocation);
        assert_eq!(
            runtime.metrics().num_alive_tasks(),
            tasks,
            "{variant}: no LiveKit task is spawned while LiveKit is not configured"
        );
        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        let line = logs.lines().last().unwrap_or_default();
        assert!(
            line.contains("LiveKit not configured; no participant evicted")
                && line.contains(&format!("revocation={variant}")),
            "{variant}: {logs}"
        );
    }
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(!logs.contains("comms-revocation"), "{logs}");
}

/// `serve`'s one NATS link is the gatekeeper's whenever `NATS_URL` is set: the `/ws` connector
/// publishes and subscribes over it in every process, the island subscriber in the primary.
#[tokio::test]
async fn nats_url_alone_builds_the_link() {
    use super::admission::cluster::ClusterConfig;
    use super::bus::InProcessBus;

    let cluster = |nats_url: Option<&str>| ClusterConfig {
        nats_url: nats_url.map(str::to_string),
        ..ClusterConfig::default()
    };
    let linked = comms_state(lazy_pool(), Settings::for_tests())
        .with_cluster(cluster(Some("nats://127.0.0.1:4222")))
        .unwrap();
    assert!(linked.bus.is_enabled(), "NATS_URL alone builds the link");
    assert!(!linked.bus.is_connected(), "nothing dials before connect()");

    let unlinked = comms_state(lazy_pool(), Settings::for_tests())
        .with_cluster(cluster(None))
        .unwrap();
    assert!(!unlinked.bus.is_enabled());
    assert!(
        !comms_state(lazy_pool(), Settings::for_tests())
            .bus
            .is_enabled(),
        "a state without a cluster config has no link"
    );

    let faked =
        comms_state(lazy_pool(), Settings::for_tests()).with_bus(Arc::new(InProcessBus::new()));
    assert!(faked.bus.is_enabled() && faked.bus.is_ready());
}

/// `announce_session` reaches the island subscriber `start_island_feed` started and nothing
/// else: before the start and after shutdown a connect is a no-op, as a `peer.*.connect`
/// nobody consumed was. A delivered connect makes the subscriber ask Pulse for the session's
/// cluster.
#[tokio::test]
async fn a_connect_reaches_only_a_running_island_subscriber() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::admission::cluster::sockets::{IslandDelivery, IslandSockets};
    use super::bus::{InProcessBus, RequestError};
    use crate::protocol::archipelago::archipelago::IslandChangedMessage;

    struct NoSockets;

    impl IslandSockets for NoSockets {
        fn deliver_island(
            &self,
            _: &str,
            _: &str,
            _: &IslandChangedMessage,
            _pulse_admission: Option<
                &crate::gatekeeper::admission::cluster::fence::PulseAdmissionStamp,
            >,
        ) -> IslandDelivery {
            IslandDelivery::Undelivered
        }
    }

    const WALLET: &str = "0x000000000000000000000000000000000000000a";
    const SESSION: &str = "0x000000000000000000000000000000000000000b";
    let bus = Arc::new(InProcessBus::new());
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = asked.clone();
    bus.respond_to_requests(Arc::new(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        Err(RequestError::TimedOut)
    }));
    let state = Arc::new(comms_state(lazy_pool(), Settings::for_tests()).with_bus(bus.clone()));
    state.announce_session(WALLET, SESSION);
    let jobs = crate::jobs::Jobs::new();
    state.start_island_feed(&jobs, Arc::new(NoSockets));
    state.announce_session(WALLET, SESSION);
    tokio::time::timeout(Duration::from_secs(3), async {
        while asked.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the running subscriber asks Pulse");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(asked.load(Ordering::SeqCst), 1);

    jobs.shutdown(Duration::from_secs(5)).await;
    state.announce_session(WALLET, SESSION);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert!(bus.published().is_empty());
}

/// The island subscriber has no switch of its own: over a link (`NATS_URL`) and the default
/// cluster settings, `start_island_feed` subscribes to Pulse's cluster feed, and a session the
/// connector announces makes it ask Pulse for that session's cluster.
#[tokio::test]
async fn the_island_feed_needs_no_switch() {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::admission::cluster::ClusterConfig;
    use super::admission::cluster::sockets::{IslandDelivery, IslandSockets};
    use super::bus::{
        InProcessBus, NatsBus, NatsHandler, NatsSubscription, PublishOutcome, RequestError,
    };
    use crate::protocol::archipelago::archipelago::IslandChangedMessage;
    use crate::protocol::bus::cluster::ClusterChange;

    /// The in-process link, recording the subject of every subscription asked of it.
    #[derive(Default)]
    struct Recorded {
        bus: InProcessBus,
        subjects: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl NatsBus for Recorded {
        fn is_enabled(&self) -> bool {
            self.bus.is_enabled()
        }

        fn is_connected(&self) -> bool {
            self.bus.is_connected()
        }

        fn is_ready(&self) -> bool {
            self.bus.is_ready()
        }

        fn connect(&self) {}

        fn subscribe(
            &self,
            subject: &str,
            queue: Option<&str>,
            handler: NatsHandler,
        ) -> NatsSubscription {
            self.subjects.lock().unwrap().push(subject.to_owned());
            self.bus.subscribe(subject, queue, handler)
        }

        async fn publish(&self, subject: &str, payload: Vec<u8>) -> PublishOutcome {
            self.bus.publish(subject, payload).await
        }

        async fn request(&self, subject: &str, payload: Vec<u8>) -> Result<Vec<u8>, RequestError> {
            self.bus.request(subject, payload).await
        }
    }

    struct NoSockets;

    impl IslandSockets for NoSockets {
        fn deliver_island(
            &self,
            _: &str,
            _: &str,
            _: &IslandChangedMessage,
            _pulse_admission: Option<
                &crate::gatekeeper::admission::cluster::fence::PulseAdmissionStamp,
            >,
        ) -> IslandDelivery {
            IslandDelivery::Undelivered
        }
    }

    const WALLET: &str = "0x000000000000000000000000000000000000000a";
    const SESSION: &str = "0x000000000000000000000000000000000000000b";
    let bus = Arc::new(Recorded::default());
    let state = Arc::new(
        comms_state(lazy_pool(), Settings::for_tests())
            .with_cluster(ClusterConfig::default())
            .unwrap()
            .with_bus(bus.clone()),
    );
    let jobs = crate::jobs::Jobs::new();
    state.start_island_feed(&jobs, Arc::new(NoSockets));
    assert!(
        bus.subjects
            .lock()
            .unwrap()
            .iter()
            .any(|subject| subject == ClusterChange::SUBJECTS),
        "{:?}",
        bus.subjects.lock().unwrap()
    );

    state.announce_session(WALLET, SESSION);
    let lookup = format!("peer.{WALLET}.cluster_lookup");
    tokio::time::timeout(Duration::from_secs(3), async {
        while !bus
            .bus
            .requested()
            .iter()
            .any(|(subject, _)| *subject == lookup)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the announced session is looked up");
    jobs.shutdown(Duration::from_secs(5)).await;
}

/// Signed metadata is checked after cryptographic verification.
#[tokio::test]
async fn world_adapter_requires_exact_signed_handshake() {
    let app = super::admission::world_mint::router(lazy_state(Settings::for_tests()));
    let path = "/get-comms-adapter/world-alice.dcl.eth";
    for metadata in [
        serde_json::json!({}),
        serde_json::json!({"signer":"dcl:explorer"}),
        serde_json::json!({"intent":"dcl:explorer:comms-handshake"}),
        serde_json::json!({"signer":"other","intent":"dcl:explorer:comms-handshake"}),
        serde_json::json!({"signer":"dcl:explorer","intent":"other"}),
        serde_json::json!({"signer":"DCL:EXPLORER","intent":"dcl:explorer:comms-handshake"}),
        serde_json::json!({"signer":"dcl:explorer","intent":"DCL:EXPLORER:COMMS-HANDSHAKE"}),
    ] {
        let mut request = Request::post(path).body(Body::empty()).unwrap();
        request
            .headers_mut()
            .extend(signed(KEY, "post", path, &metadata.to_string()).await);
        let (status, body) = body_json(app.clone().oneshot(request).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{metadata}: {body}");
    }
}

#[tokio::test]
async fn deny_listed_wallet_is_refused_by_every_world_mint_and_private_messages() {
    use crate::rooms::ResolvedRoom;
    use crate::world_identity::{RealmId, WorldName};
    use axum::routing::get;
    let denied = address_of(KEY).to_uppercase();
    let fixture = axum::Router::new().route(
        "/list",
        get(move || {
            let denied = denied.clone();
            async move { Json(serde_json::json!({"users":[{"wallet":denied}]})) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/list", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
    let world = WorldName::parse("alice.dcl.eth").unwrap();
    let scenes = StubScenes {
        room: Ok(ResolvedRoom {
            realm: RealmId::World(world.clone()),
            scene_id: None,
            deployment: None,
            room: crate::rooms::worlds_service_world_room(&world),
        }),
        ..Default::default()
    };
    let state = Arc::new(
        CommsState::new(
            lazy_pool(),
            Outbound::for_tests().allowing_private_hosts(),
            Settings::for_tests(),
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(scenes),
            Arc::new(crate::presence::Presence::new()),
            Arc::new(crate::interfaces::test_support::StubDirectory::default()),
        )
        .with_ban_status(Arc::new(super::moderation::test_support::StubBanStatus(
            Ok(None),
        )))
        .with_deny_list_url(Some(url)),
    );
    state.presence.complete_empty_fixture();
    let app = super::admission::world_mint::router(state.clone());
    let metadata = r#"{"signer":"dcl:explorer","intent":"dcl:explorer:comms-handshake"}"#;
    for prefix in ["", "/worlds-content-server"] {
        let mounted = if prefix.is_empty() {
            app.clone()
        } else {
            axum::Router::new().nest(prefix, app.clone())
        };
        for route in [
            "/worlds/alice.dcl.eth/comms",
            "/worlds/alice.dcl.eth/scenes/scene-1/comms",
            "/get-comms-adapter/world-alice.dcl.eth",
        ] {
            let path = format!("{prefix}{route}");
            let mut request = Request::post(&path).body(Body::empty()).unwrap();
            request
                .headers_mut()
                .extend(signed(KEY, "post", &path, metadata).await);
            let (status, body) = body_json(mounted.clone().oneshot(request).await.unwrap()).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}: {body}");
        }
    }
    // An unlisted wallet still reaches each mint's existing policy and token path.
    for path in [
        "/worlds/alice.dcl.eth/comms",
        "/worlds/alice.dcl.eth/scenes/scene-1/comms",
        "/get-comms-adapter/world-alice.dcl.eth",
    ] {
        let mut request = Request::post(path).body(Body::empty()).unwrap();
        request
            .headers_mut()
            .extend(signed(OUTSIDER_KEY, "post", path, metadata).await);
        let (status, body) = body_json(app.clone().oneshot(request).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let path = "/private-messages/token";
    let error = voice::private_messages_token(
        State(state),
        at(path),
        signed(KEY, "get", path, metadata).await,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, 401);
    server.abort();
}

#[tokio::test]
async fn bans_route_conditional_request_authenticates_before_snapshot_lookup() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(20))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .unwrap();
    let mut settings = Settings::for_tests();
    settings.moderator_token = Some("fixture".into());
    let app = super::router(state_with(pool, settings));
    for (bearer, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("Bearer fixture"), StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        let mut request = Request::builder().uri("/bans").header("if-none-match", "*");
        if let Some(bearer) = bearer {
            request = request.header("authorization", bearer);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert!(!response.headers().contains_key("etag"));
    }
}

#[tokio::test]
async fn server_adapter_uses_signed_context_with_optional_irrelevant_body() {
    for prefix in ["", "/comms-gatekeeper"] {
        for (metadata, expected) in [
            (
                r#"{"realm":{"serverName":"LocalPreview"},"sceneId":"SignedScene"}"#,
                "scene:LocalPreview:SignedScene",
            ),
            (
                r#"{"realmName":"main","sceneId":"SignedScene"}"#,
                "scene:main:SignedScene",
            ),
        ] {
            for (body, content_type) in [
                ("", false),
                ("", true),
                ("{}", true),
                (
                    r#"{"realmName":"attacker.eth","sceneId":"UnsignedScene"}"#,
                    true,
                ),
                ("irrelevant", false),
            ] {
                let path = format!("{prefix}/get-server-scene-adapter");
                let mut headers = signed(AUTHORITATIVE_KEY, "post", &path, metadata).await;
                if content_type {
                    headers.insert("content-type", HeaderValue::from_static("application/json"));
                }
                let app = axum::Router::new()
                    .route(
                        &path,
                        axum::routing::post(handlers::get_server_scene_adapter),
                    )
                    .with_state(lazy_state(with_authoritative(AUTHORITATIVE_KEY)));
                let mut request = Request::builder()
                    .method("POST")
                    .uri(&path)
                    .body(Body::from(body))
                    .unwrap();
                *request.headers_mut() = headers;
                let (status, response) = body_json(app.oneshot(request).await.unwrap()).await;
                assert_eq!(status, StatusCode::OK, "{response}");
                let jwt = response["adapter"]
                    .as_str()
                    .unwrap()
                    .split("access_token=")
                    .nth(1)
                    .unwrap();
                let claims = super::livekit::decode_jwt_payload(jwt);
                assert_eq!(claims["sub"], "authoritative-server");
                assert_eq!(claims["video"]["room"], expected);
            }
        }
    }
}

#[tokio::test]
async fn server_adapter_missing_signed_context_preserves_error_precedence() {
    for (key, metadata, expected) in [
        (AUTHORITATIVE_KEY, "{}", 401),
        (AUTHORITATIVE_KEY, r#"{"sceneId":"s"}"#, 401),
        (AUTHORITATIVE_KEY, r#"{"realmName":"main"}"#, 400),
        (OUTSIDER_KEY, r#"{"realmName":"main","sceneId":"s"}"#, 401),
    ] {
        let path = "/get-server-scene-adapter";
        let headers = signed(key, "post", path, metadata).await;
        let app = axum::Router::new()
            .route(
                path,
                axum::routing::post(handlers::get_server_scene_adapter),
            )
            .with_state(lazy_state(with_authoritative(AUTHORITATIVE_KEY)));
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::from(r#"{"realmName":"main","sceneId":"body-only"}"#))
            .unwrap();
        *request.headers_mut() = headers;
        let (status, _) = body_json(app.oneshot(request).await.unwrap()).await;
        assert_eq!(status.as_u16(), expected);
    }
}

#[tokio::test]
async fn participant_resolution_preserves_raw_lookup_misses_and_route_statuses() {
    use handlers::ParticipantsQuery;
    let mut state = comms_state(lazy_pool(), Settings::for_tests());
    state.scenes = Arc::new(StubScenes {
        room: Ok(crate::rooms::ResolvedRoom {
            realm: crate::world_identity::RealmId::Shared("main".to_string()),
            scene_id: Some(crate::world_identity::SceneId::parse("SignedScene").unwrap()),
            deployment: None,
            room: "unused-worlds-contract".to_string(),
        }),
        scene: Ok(None),
    });
    for (realm, pointer) in [
        (None, Some("0,0")),
        (Some("LocalPreview"), Some("0,0")),
        (Some("Foo.dcl.eth"), Some("0,0")),
        (Some("Foo.dcl.eth"), None),
    ] {
        assert!(
            handlers::participants(
                &state,
                ParticipantsQuery {
                    realm_name: realm.map(str::to_string),
                    pointer: pointer.map(str::to_string),
                    room: None,
                }
            )
            .await
            .unwrap()
            .is_empty()
        );
    }
    for (realm, pointer, code) in [
        (None, None, 400),
        (Some("main"), None, 400),
        (None, Some(""), 400),
        (None, Some("bad"), 404),
        (None, Some("00,0"), 404),
        (None, Some(" 0,0"), 404),
        (Some(" foo.eth"), Some("0,0"), 404),
        (Some("foo/bar.eth"), Some("0,0"), 404),
    ] {
        let error = handlers::participants(
            &state,
            ParticipantsQuery {
                realm_name: realm.map(str::to_string),
                pointer: pointer.map(str::to_string),
                room: None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, code, "{realm:?} {pointer:?}");
    }
    state.scenes = Arc::new(StubScenes {
        room: Err(crate::rooms::ResolveError::NoSceneAtParcel {
            realm: "main".to_string(),
            parcel: "0,0".to_string(),
        }),
        scene: Ok(None),
    });
    assert_eq!(
        handlers::participants(
            &state,
            ParticipantsQuery {
                realm_name: None,
                pointer: Some("0,0".to_string()),
                room: None
            }
        )
        .await
        .unwrap_err()
        .code,
        404
    );
    // A store validation failure is an unavailable answer, never an empty room.
    state.scenes = Arc::new(StubScenes {
        room: Err(crate::rooms::ResolveError::NoParcels {
            scene: "broken".to_string(),
        }),
        scene: Ok(None),
    });
    assert_eq!(
        handlers::participants(
            &state,
            ParticipantsQuery {
                realm_name: None,
                pointer: Some("0,0".to_string()),
                room: None
            }
        )
        .await
        .unwrap_err()
        .code,
        500
    );
    for world in ["missing.eth", " foo.eth", "foo/bar.eth"] {
        let error = scene::resolve_listing_place_id(
            &state,
            Some(world.to_string()),
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, 400);
        assert_eq!(
            error.message,
            format!("Failed to resolve scene ID for world {world}")
        );
    }
    state.scenes = Arc::new(StubScenes {
        room: Err(crate::rooms::ResolveError::SceneNotInWorld {
            world: "missing.eth".into(),
            scene: "explicit".into(),
        }),
        scene: Ok(None),
    });
    assert_eq!(
        scene::resolve_world_scene_id(&state, "missing.eth", "explicit")
            .await
            .unwrap_err()
            .code,
        404
    );
    assert_eq!(
        scene::resolve_world_scene_id(&state, "foo/bar.eth", "explicit")
            .await
            .unwrap_err()
            .code,
        400
    );
}

#[tokio::test]
async fn participant_consumers_share_room_spelling_addresses_and_livekit_failure() {
    let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let capture = observed.clone();
    let fixture = axum::Router::new().route(
        "/twirp/livekit.RoomService/ListParticipants",
        axum::routing::post(move |Json(body): Json<serde_json::Value>| {
            let capture = capture.clone();
            async move {
                capture
                    .lock()
                    .unwrap()
                    .push(body["room"].as_str().unwrap().to_string());
                Json(serde_json::json!({"participants":[
                    {"identity":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                    {"identity":"authoritative-server"}, {"identity":"not-a-wallet"}
                ]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
    let mut state = comms_state(lazy_pool(), Settings::for_tests());
    state.livekit.configured = true;
    state.livekit.api_url = api_url;
    state.scenes = Arc::new(StubScenes {
        room: Ok(crate::rooms::ResolvedRoom {
            realm: crate::world_identity::RealmId::Shared("main".to_string()),
            scene_id: Some(crate::world_identity::SceneId::parse("SignedScene").unwrap()),
            deployment: None,
            room: "unused-worlds-room".to_string(),
        }),
        scene: Ok(None),
    });
    let expected = vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()];
    for (realm, pointer) in [
        ("Foo.eth", Some("0,0")),
        ("LocalPreview", Some("0,0")),
        ("Foo.eth", None),
    ] {
        let parsed = crate::world_identity::RealmId::parse(realm).unwrap();
        let id = crate::world_identity::SceneId::parse("SignedScene").unwrap();
        let room = match &parsed {
            crate::world_identity::RealmId::World(world) => {
                crate::rooms::worlds_service_scene_room(world, &id)
            }
            crate::world_identity::RealmId::Shared(name) => {
                crate::rooms::gatekeeper_scene_room(name, id.as_str())
            }
        };
        state.scenes = Arc::new(StubScenes {
            room: Ok(crate::rooms::ResolvedRoom {
                realm: parsed,
                scene_id: Some(id),
                deployment: None,
                room,
            }),
            scene: Ok(None),
        });
        let addresses = handlers::participants(
            &state,
            handlers::ParticipantsQuery {
                realm_name: None,
                room: Some(realm.to_string()),
                pointer: pointer.map(str::to_string),
            },
        )
        .await
        .unwrap();
        assert_eq!(addresses, expected);
    }
    use crate::explore::places::live::Participants;
    state.scenes = Arc::new(StubScenes {
        room: Ok(crate::rooms::ResolvedRoom {
            realm: crate::world_identity::RealmId::Shared("main".into()),
            scene_id: Some(crate::world_identity::SceneId::parse("SignedScene").unwrap()),
            deployment: None,
            room: "scene:main:SignedScene".into(),
        }),
        scene: Ok(None),
    });
    assert_eq!(
        state.participants(false, "0,0").await,
        Some(expected.clone())
    );
    assert_eq!(state.participants(true, "Foo.eth").await, Some(expected));
    assert_eq!(
        *observed.lock().unwrap(),
        vec![
            "w1:7:foo.ethSignedScene",
            "scene:LocalPreview:SignedScene",
            "world-foo.eth",
            "scene:main:SignedScene",
            "world-foo.eth"
        ]
    );
    server.abort();
    let state = Arc::new(state);
    let Json(response) = handlers::list_participants(
        State(state),
        axum::extract::Query(handlers::ParticipantsQuery {
            realm_name: Some("Foo.eth".to_string()),
            pointer: None,
            room: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(response).unwrap(),
        serde_json::json!({"ok":true,"data":{"addresses":[]}})
    );
}

#[tokio::test]
async fn webhook_required_room_delete_retries_after_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let fixture = axum::Router::new().route(
        "/twirp/livekit.RoomService/DeleteRoom",
        axum::routing::post(move || {
            let observed = observed.clone();
            async move {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error":"retry"})),
                    )
                } else {
                    (StatusCode::OK, Json(serde_json::json!({})))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut settings = Settings::for_tests();
    settings.livekit.api_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
    let state = lazy_state(settings);
    let room = crate::rooms::private_voice_chat_room_name(&Uuid::new_v4());
    assert!(voice::delete_webhook_room(&state, &room).await.is_err());
    voice::delete_webhook_room(&state, &room).await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn webhook_community_notification_failure_keeps_rows_for_complete_retry() {
    use super::webhook::test_support::webhook_jwt;
    let Some(pool) = scratch().await else { return };
    let directory = Arc::new(crate::interfaces::test_support::StubDirectory::default());
    directory
        .fail_community_notifications
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let settings = Settings::for_tests();
    let (key, secret) = (
        settings.livekit.api_key.clone(),
        settings.livekit.api_secret.clone(),
    );
    let mut state = comms_state(pool.clone(), settings);
    state.social = directory.clone();
    let state = Arc::new(state);
    let community = seed_community(&pool).await;
    let moderator = wallet();
    state
        .voice_db
        .join_user_to_community_room(community, &moderator, true)
        .await
        .unwrap();
    let body = serde_json::to_vec(&serde_json::json!({"id": "community-end-retry", "createdAt": 1718900000,
        "event": "participant_left", "room": {"name": crate::rooms::community_voice_chat_room_name(&community)},
        "participant": {"identity": moderator.as_str(), "sid": "PA_end", "disconnectReason": "ROOM_DELETED"}})).unwrap();
    let app = super::router(state.clone());
    let jwt = webhook_jwt(&body, &key, &secret);
    assert_eq!(
        post_webhook(&app, Some(&jwt), &body).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        state
            .voice_db
            .community_users_in_room(community)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(directory.community_events.lock().unwrap().is_empty());
    assert_eq!(post_webhook(&app, Some(&jwt), &body).await, StatusCode::OK);
    assert!(
        state
            .voice_db
            .community_users_in_room(community)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(directory.community_events.lock().unwrap().len(), 1);
    assert_eq!(
        directory.community_events.lock().unwrap()[0]["key"],
        "community-streaming-ended-community-end-retry"
    );
    assert_eq!(post_webhook(&app, Some(&jwt), &body).await, StatusCode::OK);
    assert_eq!(directory.community_events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn webhook_private_join_restart_reconstructs_old_room_after_delete_failure() {
    use super::webhook::test_support::webhook_jwt;
    let Some(pool) = scratch().await else { return };
    let (participant, previous_peer, next_peer) = (wallet(), wallet(), wallet());
    let (previous, next) = (Uuid::new_v4(), Uuid::new_v4());
    // Historical overlapping call membership: the wallet was caller in A and
    // is callee in B, so the old connected call wins the current-call query.
    sqlx::query("INSERT INTO social.private_voice_chats (id,caller_address,callee_address) VALUES ($1,$2,$3),($4,$5,$2)")
        .bind(previous).bind(&participant).bind(&previous_peer).bind(next).bind(&next_peer).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO social.private_voice_chat_participants (call_id,address,status,joined_at,status_updated_at) VALUES ($1,$2,'connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),($1,$3,'connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),($4,$2,'not_connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),($4,$5,'not_connected',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        .bind(previous).bind(&participant).bind(&previous_peer).bind(next).bind(&next_peer).execute(&pool).await.unwrap();
    let rooms = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let observed = rooms.clone();
    let fixture = axum::Router::new().route(
        "/twirp/livekit.RoomService/DeleteRoom",
        axum::routing::post(move |Json(body): Json<serde_json::Value>| {
            let observed = observed.clone();
            async move {
                let mut rooms = observed.lock().unwrap();
                rooms.push(body["room"].as_str().unwrap().to_owned());
                let status = if rooms.len() == 1 {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::OK
                };
                (status, Json(serde_json::json!({})))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut settings = Settings::for_tests();
    settings.livekit.api_url = format!("http://{}", listener.local_addr().unwrap());
    let (key, secret) = (
        settings.livekit.api_key.clone(),
        settings.livekit.api_secret.clone(),
    );
    let server = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
    let body = serde_json::to_vec(&serde_json::json!({"id": "restart-private-join", "createdAt": 1718900000,
        "event": "participant_joined", "room": {"name": crate::rooms::private_voice_chat_room_name(&next)},
        "participant": {"identity": participant.as_str(), "sid": "PA_restart"}})).unwrap();
    let jwt = webhook_jwt(&body, &key, &secret);
    let first = state_with(pool.clone(), settings.clone());
    assert_eq!(
        post_webhook(&super::router(first.clone()), Some(&jwt), &body).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        first.voice_db.active_call_of(&participant).await.unwrap(),
        Some(previous)
    );
    assert_eq!(
        first
            .voice_db
            .participants_in(next)
            .await
            .unwrap()
            .iter()
            .find(|row| row.address == participant.as_str())
            .unwrap()
            .status,
        "not_connected"
    );
    drop(first);
    // A fresh coordinator has no in-memory checkpoint or deduplication entry.
    let restarted = state_with(pool.clone(), settings);
    let app = super::router(restarted.clone());
    assert_eq!(post_webhook(&app, Some(&jwt), &body).await, StatusCode::OK);
    assert_eq!(
        restarted
            .voice_db
            .active_call_of(&participant)
            .await
            .unwrap(),
        Some(next)
    );
    assert_eq!(
        rooms.lock().unwrap().as_slice(),
        &[
            crate::rooms::private_voice_chat_room_name(&previous),
            crate::rooms::private_voice_chat_room_name(&previous)
        ]
    );
    assert_eq!(post_webhook(&app, Some(&jwt), &body).await, StatusCode::OK);
    assert_eq!(rooms.lock().unwrap().len(), 2);
    restarted
        .voice_db
        .delete_private_call(previous)
        .await
        .unwrap();
    restarted.voice_db.delete_private_call(next).await.unwrap();
    server.abort();
}
