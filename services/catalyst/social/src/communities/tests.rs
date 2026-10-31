//! The community surface's contracts against the real schema: every route's
//! signed / unsigned / forged-signature answer, the private-community
//! visibility rules, the ban and request semantics, the derived voice
//! status and the explorer boot corpus shapes. DB-backed through
//! `db::scratch` (skipped without `CATALYST_TEST_DATABASE_URL`); rows are
//! scoped by fresh uuids and random wallets, so the tests share one
//! database without seeing one another.

use std::sync::Arc;

use alloy::signers::{Signer as _, local::PrivateKeySigner};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use crate::auth::{AuthLink, AuthLinkType};
use crate::shared::signed_fetch::{
    AUTH_CHAIN_HEADER_PREFIX, AUTH_METADATA_HEADER, AUTH_TIMESTAMP_HEADER, build_payload_v6,
};
use crate::{Address, CommunityId, CommunityStanding};

use super::http::ADR44_MESSAGE;
use super::standing::load_standing;
use super::{Settings, State, router};

mod fixture_shapes;
mod mute_body;
mod post_create;
mod post_edit;
mod request_lifecycle;
mod selected_community;
mod social_api_parity;
pub(super) mod world_access;

const ADMIN_TOKEN: &str = "cg-social-admin";
const FORGED: &str = "/v1/forged/not-a-route";
/// The active voice-chat list is database-wide: the test that seeds voice
/// rows and the one that reads the list empty take turns.
static VOICE_ROWS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Wallet {
    signer: PrivateKeySigner,
    address: Address,
}

fn wallet() -> Wallet {
    let signer = PrivateKeySigner::random();
    let address = Address::parse(&format!("{:#x}", signer.address())).unwrap();
    Wallet { signer, address }
}

async fn signed_headers(w: &Wallet, method: &str, path: &str) -> Vec<(String, String)> {
    let timestamp = chrono::Utc::now().timestamp_millis().to_string();
    let payload = build_payload_v6(method, path, &timestamp, "{}");
    let signature = w
        .signer
        .sign_message(payload.as_bytes())
        .await
        .unwrap()
        .to_string();
    let chain = [
        AuthLink {
            link_type: AuthLinkType::SIGNER,
            payload: format!("{:#x}", w.signer.address()),
            signature: None,
        },
        AuthLink {
            link_type: AuthLinkType::EcdsaSignedEntity,
            payload,
            signature: Some(signature),
        },
    ];
    let mut out = vec![
        (AUTH_TIMESTAMP_HEADER.to_string(), timestamp),
        (AUTH_METADATA_HEADER.to_string(), "{}".to_string()),
    ];
    for (i, link) in chain.iter().enumerate() {
        out.push((
            format!("{AUTH_CHAIN_HEADER_PREFIX}{i}"),
            serde_json::to_string(link).unwrap(),
        ));
    }
    out
}

struct Case {
    method: &'static str,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<(Vec<u8>, String)>,
}

impl Case {
    fn new(method: &'static str, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            headers: Vec::new(),
            body: None,
        }
    }
    async fn signed(mut self, w: &Wallet) -> Self {
        let path = self.path.split('?').next().unwrap().to_string();
        self.headers
            .extend(signed_headers(w, self.method, &path).await);
        self
    }
    /// A valid signature over a path that is not this route's.
    async fn forged(mut self, w: &Wallet) -> Self {
        self.headers
            .extend(signed_headers(w, self.method, FORGED).await);
        self
    }
    fn bearer(mut self, token: &str) -> Self {
        self.headers
            .push(("authorization".to_string(), format!("Bearer {token}")));
        self
    }
    fn json(mut self, v: &Value) -> Self {
        self.body = Some((
            serde_json::to_vec(v).unwrap(),
            "application/json".to_string(),
        ));
        self
    }
    fn body(mut self, bytes: Vec<u8>, content_type: &str) -> Self {
        self.body = Some((bytes, content_type.to_string()));
        self
    }
}

async fn hit(app: &Router, case: Case) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(case.method.to_uppercase().as_str())
        .uri(&case.path);
    for (k, v) in &case.headers {
        builder = builder.header(k, v);
    }
    let request = match case.body {
        Some((bytes, ct)) => builder
            .header(header::CONTENT_TYPE, ct)
            .body(Body::from(bytes))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()))
    };
    (status, value)
}

fn multipart(parts: &[(&str, &[u8])]) -> (Vec<u8>, String) {
    let b = "XCONTRACTBOUNDARY";
    let mut body = Vec::new();
    for (name, bytes) in parts {
        body.extend_from_slice(
            format!("--{b}\r\nContent-Disposition: form-data; name=\"{name}\"").as_bytes(),
        );
        if *name == "thumbnail" {
            body.extend_from_slice(b"; filename=\"t.png\"\r\nContent-Type: image/png");
        }
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{b}--\r\n").as_bytes());
    (body, format!("multipart/form-data; boundary={b}"))
}

fn png(len: usize) -> Vec<u8> {
    let mut v = vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
    v.resize(len, 0);
    v
}

struct Rig {
    pool: PgPool,
    app: Router,
    state: Arc<State>,
    storage_dir: std::path::PathBuf,
}

impl Rig {
    async fn new(settings: Settings) -> Option<Self> {
        Self::with_explore(settings, State::explore_for_tests).await
    }

    /// `explore` builds the explore state the communities state reads its
    /// destinations from, given the scratch pool.
    async fn with_explore(
        settings: Settings,
        explore: impl FnOnce(&PgPool) -> crate::explore::SharedState,
    ) -> Option<Self> {
        let pool = crate::db::scratch::pool(6).await?;
        let storage_dir =
            std::env::temp_dir().join(format!("catalyst-communities-{}", Uuid::new_v4()));
        let storage = Arc::new(
            crate::storage::Storage::from_config(&storage_dir)
                .await
                .unwrap(),
        );
        let state = Arc::new(State::new(
            pool.clone(),
            storage,
            settings,
            None,
            &crate::http::Outbound::for_tests(),
            explore(&pool),
        ));
        let app = Router::new()
            .merge(router(state.clone()))
            .nest("/social-api", router(state.clone()));
        Some(Self {
            pool,
            app,
            state,
            storage_dir,
        })
    }

    async fn community(&self, owner: &Wallet, private: bool) -> CommunityId {
        let id = CommunityId::new();
        sqlx::query(
            "INSERT INTO social.communities (id, name, description, owner_address, private, active, unlisted) \
             VALUES ($1, $2, 'seeded community', $3, $4, TRUE, FALSE)",
        )
        .bind(id)
        .bind(format!("community {}", &id.to_string()[..8]))
        .bind(&owner.address)
        .bind(private)
        .execute(&self.pool)
        .await
        .unwrap();
        self.member(id, owner, "owner").await;
        id
    }

    async fn member(&self, id: CommunityId, w: &Wallet, role: &str) {
        sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(&w.address)
            .bind(role)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn post(&self, id: CommunityId, author: &Wallet) -> Uuid {
        let post_id = Uuid::new_v4();
        sqlx::query("INSERT INTO social.community_posts (id, community_id, author_address, content) VALUES ($1, $2, $3, 'seeded post')")
            .bind(post_id)
            .bind(id)
            .bind(&author.address)
            .execute(&self.pool)
            .await
            .unwrap();
        post_id
    }

    async fn request(&self, id: CommunityId, w: &Wallet, kind: &str) -> Uuid {
        let rid = Uuid::new_v4();
        sqlx::query("INSERT INTO social.community_requests (id, community_id, member_address, status, type) VALUES ($1, $2, $3, 'pending', $4)")
            .bind(rid)
            .bind(id)
            .bind(&w.address)
            .bind(kind)
            .execute(&self.pool)
            .await
            .unwrap();
        rid
    }

    async fn friends(&self, a: &Wallet, b: &Wallet) {
        let fid = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO social.friendships (id, address_requester, address_requested, is_active, last_action, last_actor) \
             VALUES ($1, $2, $3, TRUE, 'accept', $3)",
        )
        .bind(fid)
        .bind(&a.address)
        .bind(&b.address)
        .execute(&self.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO social.friendship_actions (id, friendship_id, action, acting_user) VALUES ($1, $2, 'accept', $3)")
            .bind(Uuid::new_v4())
            .bind(fid)
            .bind(&b.address)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    /// An active profile entity for the wallet, as peer sync would leave
    /// it: the create route requires a claimed name.
    async fn profile(&self, w: &Wallet, name: &str, claimed: bool) {
        let metadata = json!({ "v": { "avatars": [{
            "name": name, "hasClaimedName": claimed,
            "avatar": { "snapshots": { "face256": "bafkface" } },
            "nameColor": { "r": 0.5, "g": 0.25, "b": 1.0 }
        }] } });
        sqlx::query(
            "INSERT INTO content.deployments (deployer_address, version, entity_type, entity_id, entity_metadata, \
                 entity_timestamp, entity_pointers, local_timestamp, auth_chain) \
             VALUES ($1, 'v3', 'profile', $2, $3, timezone('UTC', now()), ARRAY[$1]::text[], timezone('UTC', now()), '[]'::json)",
        )
        .bind(&w.address)
        .bind(format!("bafkprofile{}", Uuid::new_v4().simple()))
        .bind(metadata)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn voice(&self, id: CommunityId, w: &Wallet, status: &str, moderator: bool) {
        sqlx::query(
            "INSERT INTO social.community_voice_chat_users (community_id, address, is_moderator, status) VALUES ($1, $2, $3, $4)",
        )
        .bind(id)
        .bind(&w.address)
        .bind(moderator)
        .bind(status)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn role_of(&self, id: CommunityId, w: &Wallet) -> Option<String> {
        sqlx::query_scalar("SELECT role FROM social.community_members WHERE community_id = $1 AND member_address = $2")
            .bind(id)
            .bind(&w.address)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    fn finish(self) {
        let _ = std::fs::remove_dir_all(&self.storage_dir);
    }
}

fn admin_settings(moderator: Option<&Wallet>) -> Settings {
    Settings {
        admin_token: Some(ADMIN_TOKEN.to_string()),
        global_moderators: moderator
            .map(|m| vec![m.address.clone()])
            .unwrap_or_default(),
        ..Settings::for_tests()
    }
}

fn assert_gate_400(status: StatusCode, body: &Value, what: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {body}");
    assert_eq!(
        body,
        &json!({ "error": "Invalid Auth Chain", "message": ADR44_MESSAGE }),
        "{what}"
    );
}

fn assert_forged_401(status: StatusCode, body: &Value, what: &str) {
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{what}: {body}");
    assert!(
        body.get("ok").is_none(),
        "{what}: no ok key upstream: {body}"
    );
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .starts_with("Invalid signature"),
        "{what}: {body}"
    );
    assert_eq!(body["message"], ADR44_MESSAGE, "{what}");
}

#[tokio::test]
async fn an_active_community_still_reports_the_stored_role_and_a_soft_deleted_one_none() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let (owner, member) = (wallet(), wallet());
    let community = rig.community(&owner, false).await;
    rig.member(community, &member, "member").await;

    let standing = load_standing(&rig.pool, community, &owner.address)
        .await
        .unwrap();
    assert_eq!(standing.tier(), CommunityStanding::Owner);
    assert!(standing.row_exists());
    assert_eq!(standing.stored_role_or_none(), "owner");
    assert_eq!(
        load_standing(&rig.pool, community, &member.address)
            .await
            .unwrap()
            .tier(),
        CommunityStanding::Member
    );

    sqlx::query("UPDATE social.communities SET active = FALSE WHERE id = $1")
        .bind(community)
        .execute(&rig.pool)
        .await
        .unwrap();
    let rows_still_present: i64 =
        sqlx::query_scalar("SELECT count(*) FROM social.community_members WHERE community_id = $1")
            .bind(community)
            .fetch_one(&rig.pool)
            .await
            .unwrap();
    assert_eq!(
        rows_still_present, 2,
        "the soft delete must leave the membership rows in place"
    );
    let standing = load_standing(&rig.pool, community, &owner.address)
        .await
        .expect("the lookup succeeds and answers 'not a member'");
    assert_eq!(
        standing.tier(),
        CommunityStanding::NotAMember,
        "a former owner of a deleted community must hold no tier"
    );
    assert!(
        !standing.row_exists(),
        "the loader must report no row, so presence checks fail closed too"
    );
    assert!(!standing.is_member());
    assert!(!standing.has(crate::CommunityPermission::BanPlayers));
    assert_eq!(standing.stored_role_or_none(), "none");
    rig.finish();
}

/// The join must filter on *this* community's `active`, not on any community's.
#[tokio::test]
async fn a_deleted_community_does_not_suppress_a_live_one_for_the_same_wallet() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let (owner, other) = (wallet(), wallet());
    let deleted = rig.community(&owner, false).await;
    let live = rig.community(&other, false).await;
    rig.member(live, &owner, "moderator").await;
    sqlx::query("UPDATE social.communities SET active = FALSE WHERE id = $1")
        .bind(deleted)
        .execute(&rig.pool)
        .await
        .unwrap();
    assert_eq!(
        load_standing(&rig.pool, deleted, &owner.address)
            .await
            .unwrap()
            .tier(),
        CommunityStanding::NotAMember
    );
    assert_eq!(
        load_standing(&rig.pool, live, &owner.address)
            .await
            .unwrap()
            .tier(),
        CommunityStanding::Moderator,
        "the guard must not leak across communities"
    );
    rig.finish();
}

/// GET /v1/referral-progress marks the stats viewed only while social
/// writes go through: off or held, the stats still answer and nothing is
/// marked.
#[tokio::test]
async fn the_referral_viewed_mark_waits_for_social_writes() {
    use crate::shared::writes::restore::{lab::StampLab, scoped};
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let referrer = wallet();
    let path = "/v1/referral-progress";
    let (address, pool) = (&referrer.address, &rig.pool);
    let marked = move || async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM social.referral_progress_viewed WHERE referrer = $1",
        )
        .bind(address)
        .fetch_one(pool)
        .await
        .unwrap()
    };
    let off = crate::writes::guard(router(rig.state.clone()), false);
    let on = crate::writes::guard(router(rig.state.clone()), true);
    let (status, _) = hit(&off, Case::new("get", path).signed(&referrer).await).await;
    assert_eq!(status, StatusCode::OK, "off: the stats still answer");
    let lab = StampLab::new();
    let case = Case::new("get", path).signed(&referrer).await;
    let (status, _) = scoped(lab.stamps.clone(), hit(&on, case)).await;
    assert_eq!(status, StatusCode::OK, "held: the stats still answer");
    assert_eq!(marked().await, 0);
    let lab = StampLab::fresh();
    let case = Case::new("get", path).signed(&referrer).await;
    let (status, _) = scoped(lab.stamps.clone(), hit(&on, case)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(marked().await, 1);
    rig.finish();
}

#[tokio::test]
async fn referral_zero_shape_gate_and_flow() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let app = &rig.app;
    let (referrer, invited, other) = (wallet(), wallet(), wallet());
    let path = "/v1/referral-progress";

    let expected =
        json!({ "invitedUsersAccepted": 0, "invitedUsersAcceptedViewed": 0, "rewardImages": [] });
    for _ in 0..2 {
        let (status, body) = hit(app, Case::new("get", path).signed(&referrer).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, expected);
    }

    for (method, body) in [
        ("get", None),
        (
            "post",
            Some(json!({ "referrer": referrer.address.as_str() })),
        ),
        ("patch", None),
    ] {
        let mut case = Case::new(method, path);
        if let Some(b) = body {
            case = case.json(&b);
        }
        let (status, v) = hit(app, case).await;
        assert_gate_400(status, &v, &format!("{method} unauthenticated"));
    }

    let refer = json!({ "referrer": referrer.address.as_str() });
    let (status, _) = hit(
        app,
        Case::new("post", path).signed(&invited).await.json(&refer),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = hit(
        app,
        Case::new("post", path).signed(&invited).await.json(&refer),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "a same-referrer retry converges"
    );
    let (status, v) = hit(
        app,
        Case::new("post", path)
            .signed(&invited)
            .await
            .json(&json!({ "referrer": other.address.as_str() })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        v["message"].as_str().unwrap().contains("already exists"),
        "{v}"
    );
    let (status, v) = hit(
        app,
        Case::new("post", path).signed(&referrer).await.json(&refer),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains("cannot refer themselves"),
        "{v}"
    );
    let (status, _) = hit(
        app,
        Case::new("post", path)
            .signed(&other)
            .await
            .json(&json!({ "referrer": "not-an-address" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = hit(app, Case::new("patch", path).signed(&invited).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, v) = hit(app, Case::new("patch", path).signed(&invited).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains("Invalid referral status"),
        "{v}"
    );
    let (status, v) = hit(app, Case::new("patch", path).signed(&other).await).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(v["message"].as_str().unwrap().contains("not found"), "{v}");
    let (status, v) = hit(app, Case::new("get", path).signed(&referrer).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["invitedUsersAccepted"], 0);
    rig.finish();
}

#[tokio::test]
async fn the_explorer_boot_shapes_answer_as_the_baseline_did() {
    let _voice = VOICE_ROWS.lock().await;
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let app = &rig.app;
    let user = wallet();

    // row 42: unsigned onlyMemberOf is a 401 on the membership filter, not a gate error
    let (status, v) = hit(
        app,
        Case::new(
            "get",
            "/social-api/v2/communities?search=&onlyMemberOf=true&offset=-100&limit=100",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{v}");
    assert!(v.get("ok").is_none(), "no ok key upstream: {v}");
    assert_eq!(v["error"], "Not Authorized");
    // rows 43 and 58: no auth chain at all is the upstream 400 gate body
    let (status, v) = hit(
        app,
        Case::new("get", "/social-api/v1/community-voice-chats/active"),
    )
    .await;
    assert_gate_400(status, &v, "voice chats unsigned");
    let (status, v) = hit(
        app,
        Case::new("get", "/social-api/v1/mutes?limit=100&offset=0"),
    )
    .await;
    assert_gate_400(status, &v, "mutes unsigned");

    // the same three, signed over the gateway path, are the 200 shapes
    let (status, v) = hit(
        app,
        Case::new(
            "get",
            "/social-api/v2/communities?search=&onlyMemberOf=true&offset=-100&limit=100",
        )
        .signed(&user)
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    for key in ["results", "total", "page", "pages", "limit"] {
        assert!(
            v["data"].get(key).is_some(),
            "communities page missing {key}: {v}"
        );
    }
    assert_eq!(v["data"]["page"], 1, "offset=-100 reads as the first page");
    let (status, v) = hit(
        app,
        Case::new("get", "/social-api/v1/community-voice-chats/active")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v, json!({ "data": { "activeChats": [], "total": 0 } }));
    let (status, v) = hit(
        app,
        Case::new("get", "/social-api/v1/mutes?limit=100&offset=0")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["results"], json!([]));
    assert_eq!(v["data"]["limit"], 100);

    // and signed over the root path they are served at the root too
    let (status, v) = hit(app, Case::new("get", "/v1/mutes").signed(&user).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    rig.finish();
}

#[tokio::test]
async fn every_route_answers_its_contract() {
    let modw = wallet();
    let Some(rig) = Rig::new(admin_settings(Some(&modw))).await else {
        return;
    };
    let app = &rig.app;
    let (user, joiner, m1, m2, m3, req_user, friend) = (
        wallet(),
        wallet(),
        wallet(),
        wallet(),
        wallet(),
        wallet(),
        wallet(),
    );
    rig.profile(&user, "Owner", true).await;
    rig.profile(&m1, "MemberOne", false).await;
    let c1 = rig.community(&user, false).await;
    let c_del = rig.community(&user, false).await;
    let c_priv = rig.community(&user, true).await;
    rig.member(c1, &m1, "member").await;
    rig.member(c1, &m2, "member").await;
    let p1 = rig.post(c1, &user).await;
    let p2 = rig.post(c1, &user).await;
    let r1 = rig.request(c_priv, &m3, "request_to_join").await;
    rig.friends(&user, &friend).await;
    let missing = CommunityId::new();

    // thumbnail seeded through the store, as an upload would leave it
    let thumb = png(2048);
    super::thumbnail::store_thumbnail(&rig.pool, &rig.state.storage, c1, &thumb)
        .await
        .unwrap();

    // The list reads search this test's own rows: sibling tests seed the same
    // scratch database, and their communities would page c1 out.
    let tag = Uuid::new_v4().simple().to_string();
    sqlx::query("UPDATE social.communities SET description = $2 WHERE id::text = ANY($1)")
        .bind(vec![c1.to_string(), c_priv.to_string()])
        .bind(&tag)
        .execute(&rig.pool)
        .await
        .unwrap();
    for signed in [false, true] {
        for path in ["/v1/communities", "/v2/communities"] {
            let mut case = Case::new("get", format!("{path}?search={tag}"));
            if signed {
                case = case.signed(&user).await;
            }
            let (status, v) = hit(app, case).await;
            assert_eq!(status, StatusCode::OK, "{path} signed={signed}: {v}");
            let results = v["data"]["results"].as_array().unwrap();
            let mine = results
                .iter()
                .find(|c| c["id"] == c1.to_string())
                .unwrap_or_else(|| panic!("{path}: c1 listed: {v}"));
            assert_eq!(mine["membersCount"], 3);
            assert_eq!(mine["thumbnailUrl"], rig.state.thumbnail_url(c1));
            assert_eq!(mine["isLive"], false);
            if signed {
                assert_eq!(mine["role"], "owner");
                assert_eq!(mine["visibility"], "all");
                assert!(mine["friends"].is_array());
            } else {
                assert!(mine.get("role").is_none());
                assert!(mine.get("friends").is_none());
            }
            if path == "/v1/communities" {
                assert_eq!(mine["ownerName"], "Owner");
            } else {
                assert!(mine.get("ownerName").is_none(), "v2 sends no ownerName");
            }
            assert_eq!(
                results.iter().any(|c| c["id"] == c_priv.to_string()),
                signed,
                "{path}: a private community is listed to a signed viewer, never to an anonymous one"
            );
        }
    }
    let (status, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["role"], "owner");
    assert_eq!(v["data"]["isBanned"], false);
    assert_eq!(v["data"]["ownerName"], "Owner");
    let (status, v) = hit(app, Case::new("get", format!("/v1/communities/{c1}"))).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(v["data"].get("role").is_none());
    let (status, _) = hit(app, Case::new("get", format!("/v1/communities/{missing}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = hit(app, Case::new("get", "/v1/communities/not-a-uuid")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, v) = hit(app, Case::new("get", format!("/v2/communities/{c1}"))).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(v["data"].get("ownerName").is_none());
    let (status, _) = hit(app, Case::new("get", format!("/v2/communities/{missing}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for (base, seg) in [
        ("/v1/communities", "members"),
        ("/v2/communities", "members"),
        ("/v1/communities", "places"),
        ("/v1/communities", "posts"),
        ("/v2/communities", "posts"),
    ] {
        let (status, v) = hit(
            app,
            Case::new("get", format!("{base}/{c1}/{seg}"))
                .signed(&user)
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{base}/{seg} signed: {v}");
        let (status, v) = hit(app, Case::new("get", format!("{base}/{c1}/{seg}"))).await;
        assert_eq!(status, StatusCode::OK, "{base}/{seg} anon: {v}");
        let (status, _) = hit(app, Case::new("get", format!("{base}/{missing}/{seg}"))).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{base}/{seg} missing");
    }
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}/members"))
            .signed(&user)
            .await,
    )
    .await;
    let rows = v["data"]["results"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["role"], "owner", "owner first: {v}");
    assert_eq!(rows[0]["name"], "Owner");
    assert_eq!(rows[0]["hasClaimedName"], true);
    assert_eq!(rows[0]["nameColor"]["g"], 0.25);
    let m1_row = rows
        .iter()
        .find(|r| r["memberAddress"] == m1.address.as_str())
        .unwrap();
    assert_eq!(m1_row["name"], "MemberOne");
    assert_eq!(m1_row["friendshipStatus"], 7);
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v2/communities/{c1}/members"))
            .signed(&user)
            .await,
    )
    .await;
    assert!(
        v["data"]["results"][0].get("name").is_none(),
        "v2 members carry no profile fields: {v}"
    );
    assert_eq!(v["data"]["results"][0]["friendshipStatus"], 7);
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}/posts"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(v["data"]["total"], 2);
    assert_eq!(v["data"]["posts"][0]["authorName"], "Owner");
    assert!(
        v["data"]["posts"][0].get("type").is_none(),
        "production's post has no type"
    );
    assert_eq!(v["data"]["posts"][0]["isLikedByUser"], false);

    for (base, seg) in [
        ("/v1/communities", "bans"),
        ("/v2/communities", "bans"),
        ("/v1/communities", "requests"),
        ("/v2/communities", "requests"),
    ] {
        let actual = format!("{base}/{c1}/{seg}");
        let (status, v) = hit(app, Case::new("get", &actual).signed(&user).await).await;
        assert_eq!(status, StatusCode::OK, "{actual}: {v}");
        let (status, v) = hit(app, Case::new("get", &actual)).await;
        assert_gate_400(status, &v, &actual);
        let (status, v) = hit(app, Case::new("get", &actual).forged(&user).await).await;
        assert_forged_401(status, &v, &actual);
        let (status, v) = hit(app, Case::new("get", &actual).signed(&m1).await).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "a member may not list {seg}: {v}"
        );
    }

    let mcomms = format!("/v1/members/{}/communities", user.address);
    let (status, v) = hit(app, Case::new("get", &mcomms).signed(&user).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["data"]["total"], 3,
        "the wallet sees its private membership too: {v}"
    );
    let (status, v) = hit(app, Case::new("get", &mcomms)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["data"]["total"], 2,
        "a stranger sees the public listed ones: {v}"
    );

    for version in ["v1", "v2"] {
        let actual = format!("/{version}/members/{}/requests", user.address);
        let (status, v) = hit(app, Case::new("get", &actual).signed(&user).await).await;
        assert_eq!(status, StatusCode::OK, "{actual}: {v}");
        let (status, v) = hit(app, Case::new("get", &actual)).await;
        assert_gate_400(status, &v, &actual);
        let (status, v) = hit(app, Case::new("get", &actual).forged(&user).await).await;
        assert_forged_401(status, &v, &actual);
        let (status, _) = hit(app, Case::new("get", &actual).signed(&m1).await).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "another wallet's requests"
        );
    }
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/members/{}/requests", m3.address))
            .signed(&m3)
            .await,
    )
    .await;
    assert_eq!(v["data"]["total"], 1, "{v}");
    assert_eq!(
        v["data"]["results"][0]["thumbnailUrl"],
        rig.state.thumbnail_url(c_priv)
    );
    assert_eq!(v["data"]["results"][0]["ownerName"], "Owner");
    assert!(v["data"]["results"][0].get("_hasThumbnail").is_none());

    let invites = format!("/v1/members/{}/invites", modw.address);
    let (status, v) = hit(app, Case::new("get", &invites).signed(&user).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"].as_array().unwrap().len(), 3);
    let (status, v) = hit(app, Case::new("get", &invites)).await;
    assert_gate_400(status, &v, "invites");
    let (status, v) = hit(app, Case::new("get", &invites).forged(&user).await).await;
    assert_forged_401(status, &v, "invites");
    let (status, _) = hit(
        app,
        Case::new("get", format!("/v1/members/{}/invites", user.address))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "self-invite");

    let managed = format!("/v1/communities/{}/managed", user.address);
    let (status, v) = hit(app, Case::new("get", &managed).bearer(ADMIN_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["total"], 3);
    let (status, _) = hit(app, Case::new("get", &managed)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = hit(app, Case::new("get", "/v1/mutes").signed(&user).await).await;
    assert_eq!(status, StatusCode::OK);
    let (status, v) = hit(app, Case::new("get", "/v1/mutes").forged(&user).await).await;
    assert_forged_401(status, &v, "mutes");

    let (status, v) = hit(app, Case::new("get", "/v1/friends").signed(&user).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["total"], 1);
    assert_eq!(v["friends"][0]["address"], friend.address.as_str());
    let (status, v) = hit(app, Case::new("get", "/v1/friends")).await;
    assert_gate_400(status, &v, "friends");
    let (status, v) = hit(app, Case::new("get", "/v1/friends").forged(&user).await).await;
    assert_forged_401(status, &v, "friends");

    let messages = format!("/v1/friends/{}/messages", friend.address);
    let (status, v) = hit(app, Case::new("get", &messages).signed(&user).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, v) = hit(app, Case::new("get", &messages)).await;
    assert_gate_400(status, &v, "messages");
    let (status, v) = hit(app, Case::new("get", &messages).forged(&user).await).await;
    assert_forged_401(status, &v, "messages");
    let (status, _) = hit(
        app,
        Case::new("get", format!("/v1/friends/{}/messages", m1.address))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "not accepted friends");

    let (status, _) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, v) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .forged(&user)
            .await,
    )
    .await;
    assert_forged_401(status, &v, "voice");

    let (status, v) = hit(
        app,
        Case::new("get", "/v1/moderation/communities")
            .signed(&modw)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, v) = hit(
        app,
        Case::new("get", "/v1/moderation/communities")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{v}");
    let (status, v) = hit(app, Case::new("get", "/v1/moderation/communities")).await;
    assert_gate_400(status, &v, "moderation");
    let (status, v) = hit(
        app,
        Case::new("get", "/v1/moderation/communities")
            .forged(&modw)
            .await,
    )
    .await;
    assert_forged_401(status, &v, "moderation");

    let (status, v) = hit(
        app,
        Case::new("get", format!("/social/communities/{c1}/raw-thumbnail.png")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, _) = hit(
        app,
        Case::new(
            "get",
            format!("/social/communities/{missing}/raw-thumbnail.png"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (mp, mp_ct) = multipart(&[
        ("name", b"Gate Community"),
        ("description", b"created by the contract gate"),
    ]);
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities")
            .signed(&user)
            .await
            .body(mp.clone(), &mp_ct),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{v}");
    assert_eq!(v["data"]["role"], "owner");
    assert_eq!(v["data"]["membersCount"], 1);
    let created = CommunityId::parse(v["data"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(rig.role_of(created, &user).await.as_deref(), Some("owner"));
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities").body(mp.clone(), &mp_ct),
    )
    .await;
    assert_gate_400(status, &v, "create unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities")
            .forged(&user)
            .await
            .body(mp.clone(), &mp_ct),
    )
    .await;
    assert_forged_401(status, &v, "create forged");
    let (status, _) = hit(
        app,
        Case::new("post", "/v1/communities")
            .signed(&user)
            .await
            .body(b"not-multipart".to_vec(), "text/plain"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities")
            .signed(&m1)
            .await
            .body(mp.clone(), &mp_ct),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "no claimed name: {v}");
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains("doesn't have any names"),
        "{v}"
    );
    let (mp_thumb, thumb_ct) = multipart(&[
        ("name", b"With Thumb"),
        ("description", b"has a thumbnail"),
        ("privacy", b"private"),
        ("thumbnail", &png(1500)),
    ]);
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities")
            .signed(&user)
            .await
            .body(mp_thumb, &thumb_ct),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{v}");
    assert_eq!(v["data"]["privacy"], "private");
    let with_thumb = v["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        v["data"]["thumbnailUrl"],
        rig.state
            .thumbnail_url(CommunityId::parse(&with_thumb).unwrap())
    );
    let (status, _) = hit(
        app,
        Case::new(
            "get",
            format!("/social/communities/{with_thumb}/raw-thumbnail.png"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (mp_bad, bad_ct) = multipart(&[
        ("name", b"Bad Thumb"),
        ("description", b"x"),
        ("thumbnail", &vec![1u8; 1500]),
    ]);
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/communities")
            .signed(&user)
            .await
            .body(mp_bad, &bad_ct),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");

    let c1_path = format!("/v1/communities/{c1}");
    let (upd, upd_ct) = multipart(&[("name", b"Renamed")]);
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&user)
            .await
            .body(upd.clone(), &upd_ct),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["name"], "Renamed");
    let (status, v) = hit(app, Case::new("put", &c1_path).body(upd.clone(), &upd_ct)).await;
    assert_gate_400(status, &v, "put unsigned");
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .forged(&user)
            .await
            .body(upd.clone(), &upd_ct),
    )
    .await;
    assert_forged_401(status, &v, "put forged");
    let (status, _) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&user)
            .await
            .body(b"nope".to_vec(), "text/plain"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&m1)
            .await
            .body(upd.clone(), &upd_ct),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a member may not edit: {v}"
    );

    let (status, _) = hit(
        app,
        Case::new("patch", &c1_path)
            .signed(&user)
            .await
            .json(&json!({ "editorsChoice": true })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, v) = hit(
        app,
        Case::new("patch", &c1_path).json(&json!({ "editorsChoice": true })),
    )
    .await;
    assert_gate_400(status, &v, "patch unsigned");
    let (status, v) = hit(
        app,
        Case::new("patch", &c1_path)
            .signed(&m1)
            .await
            .json(&json!({ "editorsChoice": true })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "owner only: {v}");

    let (status, _) = hit(
        app,
        Case::new("delete", format!("/v1/communities/{c_del}"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = hit(app, Case::new("get", format!("/v1/communities/{c_del}"))).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a soft-deleted community reads as missing"
    );
    let (status, v) = hit(app, Case::new("delete", &c1_path)).await;
    assert_gate_400(status, &v, "delete unsigned");
    let (status, v) = hit(app, Case::new("delete", &c1_path).forged(&user).await).await;
    assert_forged_401(status, &v, "delete forged");

    let members = format!("/v1/communities/{c1}/members");
    let (status, _) = hit(app, Case::new("post", &members).signed(&joiner).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(rig.role_of(c1, &joiner).await.as_deref(), Some("member"));
    let (status, v) = hit(app, Case::new("post", &members)).await;
    assert_gate_400(status, &v, "join unsigned");
    let (status, v) = hit(app, Case::new("post", &members).forged(&joiner).await).await;
    assert_forged_401(status, &v, "join forged");
    let (status, v) = hit(
        app,
        Case::new("post", format!("/v1/communities/{c_priv}/members"))
            .signed(&joiner)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "private join: {v}");
    let (status, _) = hit(
        app,
        Case::new("post", format!("/v1/communities/{missing}/members"))
            .signed(&joiner)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let leave = format!("/v1/communities/{c1}/members/{}", joiner.address);
    let (status, _) = hit(app, Case::new("delete", &leave).signed(&joiner).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(rig.role_of(c1, &joiner).await, None);
    let (status, v) = hit(app, Case::new("delete", &leave)).await;
    assert_gate_400(status, &v, "leave unsigned");
    let (status, v) = hit(app, Case::new("delete", &leave).forged(&joiner).await).await;
    assert_forged_401(status, &v, "leave forged");
    let (status, v) = hit(
        app,
        Case::new(
            "delete",
            format!("/v1/communities/{c1}/members/{}", user.address),
        )
        .signed(&user)
        .await,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "the owner cannot leave: {v}"
    );
    let (status, v) = hit(
        app,
        Case::new(
            "delete",
            format!("/v1/communities/{c1}/members/{}", m2.address),
        )
        .signed(&m1)
        .await,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a member cannot kick: {v}"
    );

    let role_path = format!("/v1/communities/{c1}/members/{}", m1.address);
    let (status, _) = hit(
        app,
        Case::new("patch", &role_path)
            .signed(&user)
            .await
            .json(&json!({ "role": "moderator" })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(rig.role_of(c1, &m1).await.as_deref(), Some("moderator"));
    // EditName is owner-only in the capability matrix: a moderator edits the
    // description (EditInfo) but a rename answers the same 401 envelope; a
    // name equal to the stored one is not a rename.
    let (mod_upd, mod_ct) = multipart(&[("description", b"edited by a moderator")]);
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&m1)
            .await
            .body(mod_upd, &mod_ct),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a moderator edits the info: {v}");
    let (mod_ren, ren_ct) = multipart(&[("name", b"Moderator Renamed")]);
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&m1)
            .await
            .body(mod_ren, &ren_ct),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a moderator may not rename: {v}"
    );
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains("edit the community name"),
        "{v}"
    );
    let (_, v) = hit(app, Case::new("get", &c1_path).signed(&user).await).await;
    assert_eq!(
        v["data"]["name"], "Renamed",
        "the refused rename changed nothing"
    );
    let (same_name, same_ct) = multipart(&[
        ("name", b"Renamed"),
        ("description", b"still a moderator edit"),
    ]);
    let (status, v) = hit(
        app,
        Case::new("put", &c1_path)
            .signed(&m1)
            .await
            .body(same_name, &same_ct),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unchanged name is no rename: {v}"
    );
    let (status, v) = hit(
        app,
        Case::new("patch", &role_path).json(&json!({ "role": "moderator" })),
    )
    .await;
    assert_gate_400(status, &v, "role unsigned");
    let (status, v) = hit(
        app,
        Case::new("patch", &role_path)
            .forged(&user)
            .await
            .json(&json!({ "role": "moderator" })),
    )
    .await;
    assert_forged_401(status, &v, "role forged");
    let (status, _) = hit(
        app,
        Case::new("patch", &role_path)
            .signed(&user)
            .await
            .json(&json!({ "role": "bogus" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = hit(
        app,
        Case::new("patch", &role_path)
            .signed(&user)
            .await
            .json(&json!({ "role": "owner" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "ownership is not assigned here"
    );
    let (status, v) = hit(
        app,
        Case::new(
            "patch",
            format!("/v1/communities/{c1}/members/{}", m2.address),
        )
        .signed(&m1)
        .await
        .json(&json!({ "role": "moderator" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a moderator does not assign roles: {v}"
    );
    // the moderator (m1) may kick a member (m2) but not the owner
    let (status, _) = hit(
        app,
        Case::new(
            "delete",
            format!("/v1/communities/{c1}/members/{}", user.address),
        )
        .signed(&m1)
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let ban_path = format!("/v1/communities/{c1}/members/{}/bans", m2.address);
    let (status, _) = hit(app, Case::new("post", &ban_path).signed(&user).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        rig.role_of(c1, &m2).await,
        None,
        "the ban removes the member row"
    );
    let (status, v) = hit(app, Case::new("post", &members).signed(&m2).await).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a banned wallet cannot rejoin: {v}"
    );
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}/bans"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(v["data"]["total"], 1, "{v}");
    assert_eq!(
        v["data"]["results"][0]["memberAddress"],
        m2.address.as_str()
    );
    assert_eq!(v["data"]["results"][0]["bannedBy"], user.address.as_str());
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}"))
            .signed(&m2)
            .await,
    )
    .await;
    assert_eq!(v["data"]["isBanned"], true);
    let (status, v) = hit(app, Case::new("post", &ban_path)).await;
    assert_gate_400(status, &v, "ban unsigned");
    let (status, v) = hit(app, Case::new("post", &ban_path).forged(&user).await).await;
    assert_forged_401(status, &v, "ban forged");
    let (status, v) = hit(
        app,
        Case::new(
            "post",
            format!("/v1/communities/{c1}/members/{}/bans", user.address),
        )
        .signed(&m1)
        .await,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a moderator cannot ban the owner: {v}"
    );
    let (status, _) = hit(app, Case::new("delete", &ban_path).signed(&user).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = hit(app, Case::new("post", &members).signed(&m2).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "unbanned wallets rejoin");
    let (status, _) = hit(app, Case::new("post", &ban_path).signed(&user).await).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "a re-ban reactivates the row"
    );
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/communities/{c1}/bans"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(v["data"]["total"], 1);
    let (status, v) = hit(app, Case::new("delete", &ban_path)).await;
    assert_gate_400(status, &v, "unban unsigned");
    let (status, v) = hit(app, Case::new("delete", &ban_path).forged(&user).await).await;
    assert_forged_401(status, &v, "unban forged");

    let places = format!("/v1/communities/{c1}/places");
    let (status, _) = hit(
        app,
        Case::new("post", &places)
            .signed(&user)
            .await
            .json(&json!({ "placeIds": ["place-1"] })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, v) = hit(app, Case::new("get", &places)).await;
    assert_eq!(v["data"]["total"], 1, "{v}");
    assert_eq!(v["data"]["results"][0]["id"], "place-1");
    let (status, v) = hit(
        app,
        Case::new("post", &places).json(&json!({ "placeIds": ["place-1"] })),
    )
    .await;
    assert_gate_400(status, &v, "places unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", &places)
            .forged(&user)
            .await
            .json(&json!({ "placeIds": ["place-1"] })),
    )
    .await;
    assert_forged_401(status, &v, "places forged");
    let (status, _) = hit(
        app,
        Case::new("post", &places)
            .signed(&user)
            .await
            .json(&json!({ "placeIds": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let place_del = format!("/v1/communities/{c1}/places/place-1");
    let (status, _) = hit(app, Case::new("delete", &place_del).signed(&user).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = hit(app, Case::new("delete", &place_del).signed(&user).await).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "already removed");
    let (status, v) = hit(app, Case::new("delete", &place_del)).await;
    assert_gate_400(status, &v, "place delete unsigned");

    let posts = format!("/v1/communities/{c1}/posts");
    let (status, v) = hit(
        app,
        Case::new("post", &posts)
            .signed(&user)
            .await
            .json(&json!({ "content": "hello from the gate" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{v}");
    assert_eq!(v["data"]["content"], "hello from the gate");
    let (status, v) = hit(
        app,
        Case::new("post", &posts).json(&json!({ "content": "x" })),
    )
    .await;
    assert_gate_400(status, &v, "post unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", &posts)
            .forged(&user)
            .await
            .json(&json!({ "content": "x" })),
    )
    .await;
    assert_forged_401(status, &v, "post forged");
    let (status, _) = hit(
        app,
        Case::new("post", &posts)
            .signed(&user)
            .await
            .json(&json!({ "content": "   " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, v) = hit(
        app,
        Case::new("post", &posts)
            .signed(&m2)
            .await
            .json(&json!({ "content": "as a member" })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "m2 is banned: {v}");
    let post_del = format!("/v1/communities/{c1}/posts/{p1}");
    let (status, v) = hit(app, Case::new("delete", &post_del).signed(&m1).await).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a moderator deletes only their own: {v}"
    );
    let (status, _) = hit(app, Case::new("delete", &post_del).signed(&user).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, v) = hit(app, Case::new("delete", &post_del)).await;
    assert_gate_400(status, &v, "post delete unsigned");
    let like = format!("/v1/communities/{c1}/posts/{p2}/like");
    let (status, _) = hit(app, Case::new("post", &like).signed(&user).await).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, v) = hit(app, Case::new("get", &posts).signed(&user).await).await;
    let liked = v["data"]["posts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == p2.to_string())
        .unwrap();
    assert_eq!(liked["likesCount"], 1);
    assert_eq!(liked["isLikedByUser"], true);
    let (status, v) = hit(app, Case::new("post", &like)).await;
    assert_gate_400(status, &v, "like unsigned");
    let (status, v) = hit(app, Case::new("post", &like).forged(&user).await).await;
    assert_forged_401(status, &v, "like forged");
    let (status, _) = hit(app, Case::new("delete", &like).signed(&user).await).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = hit(
        app,
        Case::new("post", format!("/v1/communities/{c1}/posts/{missing}/like"))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let req_priv = format!("/v1/communities/{c_priv}/requests");
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv)
            .signed(&req_user)
            .await
            .json(&json!({ "type": "request_to_join" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["status"], "pending");
    let first_id = v["data"]["id"].clone();
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv)
            .signed(&req_user)
            .await
            .json(&json!({ "type": "request_to_join" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["data"]["id"], first_id,
        "a repeat answers the pending row"
    );
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv).json(&json!({ "type": "request_to_join" })),
    )
    .await;
    assert_gate_400(status, &v, "request unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv)
            .forged(&req_user)
            .await
            .json(&json!({ "type": "request_to_join" })),
    )
    .await;
    assert_forged_401(status, &v, "request forged");
    let (status, _) = hit(
        app,
        Case::new("post", &req_priv)
            .signed(&req_user)
            .await
            .json(&json!({ "type": "bogus" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, v) = hit(
        app,
        Case::new("post", posts.replace("posts", "requests"))
            .signed(&req_user)
            .await
            .json(&json!({ "type": "request_to_join" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "public communities take no requests: {v}"
    );
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv)
            .signed(&m1)
            .await
            .json(&json!({ "type": "invite", "targetedAddress": m3.address.as_str() })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a non-member cannot invite: {v}"
    );
    // the owner invites req_user, who has a pending request: the two sides meet
    let (status, v) = hit(
        app,
        Case::new("post", &req_priv)
            .signed(&user)
            .await
            .json(&json!({ "type": "invite", "targetedAddress": req_user.address.as_str() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["status"], "accepted");
    assert_eq!(
        rig.role_of(c_priv, &req_user).await.as_deref(),
        Some("member")
    );

    let req_upd = format!("/v1/communities/{c_priv}/requests/{r1}");
    let (status, v) = hit(
        app,
        Case::new("patch", &req_upd)
            .signed(&m3)
            .await
            .json(&json!({ "intention": "accepted" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "the requester cannot accept their own request: {v}"
    );
    let (status, _) = hit(
        app,
        Case::new("patch", &req_upd)
            .signed(&user)
            .await
            .json(&json!({ "intention": "accepted" })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(rig.role_of(c_priv, &m3).await.as_deref(), Some("member"));
    let (status, v) = hit(
        app,
        Case::new("patch", &req_upd).json(&json!({ "intention": "accepted" })),
    )
    .await;
    assert_gate_400(status, &v, "request update unsigned");
    let (status, v) = hit(
        app,
        Case::new("patch", &req_upd)
            .forged(&user)
            .await
            .json(&json!({ "intention": "accepted" })),
    )
    .await;
    assert_forged_401(status, &v, "request update forged");
    let (status, _) = hit(
        app,
        Case::new("patch", &req_upd)
            .signed(&user)
            .await
            .json(&json!({ "intention": "bogus" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mcomms = format!("/v1/members/{}/communities", user.address);
    let (status, v) = hit(
        app,
        Case::new("post", &mcomms).bearer(ADMIN_TOKEN).json(
            &json!({ "communityIds": [c1.to_string(), c_del.to_string(), missing.to_string()] }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["data"]["communities"],
        json!([{ "id": c1.to_string(), "role": "owner" }]),
        "deleted and unknown ids drop out"
    );
    let (status, _) = hit(
        app,
        Case::new("post", &mcomms).json(&json!({ "communityIds": [c1.to_string()] })),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = hit(
        app,
        Case::new("post", &mcomms)
            .bearer(ADMIN_TOKEN)
            .json(&json!({ "communityIds": ["short"] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = hit(
        app,
        Case::new("post", "/v1/mutes")
            .signed(&user)
            .await
            .json(&json!({ "muted_address": m1.address.as_str() })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, v) = hit(
        app,
        Case::new("get", format!("/v1/mutes?address={}", m1.address))
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(v["data"]["total"], 1, "{v}");
    assert!(
        v["data"]["results"][0]["muted_at"]
            .as_str()
            .unwrap()
            .ends_with('Z')
    );
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/mutes").json(&json!({ "muted_address": m1.address.as_str() })),
    )
    .await;
    assert_gate_400(status, &v, "mute unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", "/v1/mutes")
            .forged(&user)
            .await
            .json(&json!({ "muted_address": m1.address.as_str() })),
    )
    .await;
    assert_forged_401(status, &v, "mute forged");
    let (status, _) = hit(
        app,
        Case::new("post", "/v1/mutes")
            .signed(&user)
            .await
            .json(&json!({ "muted_address": "not-an-address" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = hit(
        app,
        Case::new("post", "/v1/mutes")
            .signed(&user)
            .await
            .json(&json!({ "muted_address": user.address.as_str() })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "self-mute");
    let (status, _) = hit(
        app,
        Case::new("delete", "/v1/mutes")
            .signed(&user)
            .await
            .json(&json!({ "muted_address": m1.address.as_str() })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, v) = hit(app, Case::new("get", "/v1/mutes").signed(&user).await).await;
    assert_eq!(v["data"]["total"], 0);
    let (status, v) = hit(
        app,
        Case::new("delete", "/v1/mutes").json(&json!({ "muted_address": m1.address.as_str() })),
    )
    .await;
    assert_gate_400(status, &v, "unmute unsigned");
    let (status, _) = hit(
        app,
        Case::new("delete", "/v1/mutes")
            .signed(&user)
            .await
            .json(&json!({ "muted_address": "not-an-address" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let send = format!("/v1/friends/{}/messages", friend.address);
    let (status, v) = hit(
        app,
        Case::new("post", &send)
            .signed(&user)
            .await
            .json(&json!({ "body": "hi" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["message"]["from"], user.address.as_str());
    assert_eq!(v["message"]["to"], friend.address.as_str());
    let send_back = format!("/v1/friends/{}/messages", user.address);
    let (status, v) = hit(
        app,
        Case::new("post", &send_back)
            .signed(&friend)
            .await
            .json(&json!({ "body": "hello back" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (_, v) = hit(app, Case::new("get", &messages).signed(&user).await).await;
    assert_eq!(v["total"], 2, "{v}");
    assert_eq!(v["messages"][0]["body"], "hi", "oldest first");
    assert_eq!(v["messages"][1]["body"], "hello back");
    let (_, v) = hit(
        app,
        Case::new("get", format!("{send_back}?limit=1"))
            .signed(&friend)
            .await,
    )
    .await;
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    assert_eq!(v["messages"][0]["body"], "hello back", "the newest page");
    let (status, v) = hit(app, Case::new("post", &send).json(&json!({ "body": "hi" }))).await;
    assert_gate_400(status, &v, "send unsigned");
    let (status, v) = hit(
        app,
        Case::new("post", &send)
            .forged(&user)
            .await
            .json(&json!({ "body": "hi" })),
    )
    .await;
    assert_forged_401(status, &v, "send forged");
    let (status, _) = hit(
        app,
        Case::new("post", &send)
            .signed(&user)
            .await
            .json(&json!({ "body": "   " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = hit(
        app,
        Case::new("post", format!("/v1/friends/{}/messages", m1.address))
            .signed(&user)
            .await
            .json(&json!({ "body": "hi" })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // The per-signer send budget: the limiter runs after the signature and
    // before the body and friendship checks, so `user` has spent 3 of 60 on
    // this route so far (the send, the blank body, the non-friend). The
    // bucket refills at one token a second, so the exact count depends on
    // how long the loop takes; the bound is the budget plus a few seconds.
    let mut sent_ok = 0;
    let mut refused = None;
    for _ in 0..80 {
        let (status, v) = hit(
            app,
            Case::new("post", &send)
                .signed(&user)
                .await
                .json(&json!({ "body": "again" })),
        )
        .await;
        if status == StatusCode::TOO_MANY_REQUESTS {
            refused = Some(v);
            break;
        }
        assert_eq!(status, StatusCode::OK, "{v}");
        sent_ok += 1;
    }
    assert!(
        (57..=70).contains(&sent_ok),
        "60 per minute per signer, one refilled a second: {sent_ok}"
    );
    assert_eq!(
        refused.expect("a send past the budget is refused")["message"],
        "rate limit exceeded"
    );
    let (status, _) = hit(
        app,
        Case::new("post", &send_back)
            .signed(&friend)
            .await
            .json(&json!({ "body": "unaffected" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the budget is per signer");
    let (_, v) = hit(app, Case::new("get", &messages).signed(&user).await).await;
    assert_eq!(v["total"], 3 + sent_ok, "{}", v["total"]);

    let (status, v) = hit(
        app,
        Case::new("get", "/v1/admin/communities").bearer(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, _) = hit(app, Case::new("get", "/v1/admin/communities")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = hit(
        app,
        Case::new("get", "/v1/admin/communities").bearer("wrong"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let suspend = format!("/v1/admin/communities/{c1}/suspend");
    let (status, v) = hit(
        app,
        Case::new("post", &suspend)
            .bearer(ADMIN_TOKEN)
            .json(&json!({ "reason": "gate" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["suspended"], true);
    let (status, _) = hit(app, Case::new("get", &c1_path)).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a suspended community reads as missing"
    );
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/admin/communities?status=suspended").bearer(ADMIN_TOKEN),
    )
    .await;
    let row = v["data"]["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == c1.to_string())
        .expect("listed as suspended");
    assert_eq!(row["suspensionReason"], "gate");
    assert_eq!(row["suspendedBy"], "admin-token");
    let (status, _) = hit(app, Case::new("post", &suspend)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let unsuspend = format!("/v1/admin/communities/{c1}/unsuspend");
    let (status, v) = hit(app, Case::new("post", &unsuspend).bearer(ADMIN_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, _) = hit(app, Case::new("get", &c1_path)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = hit(app, Case::new("post", &unsuspend)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = hit(
        app,
        Case::new("post", format!("/v1/admin/communities/{missing}/suspend")).bearer(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    rig.finish();
}

#[tokio::test]
async fn a_private_community_is_visible_to_members_pending_invitees_and_the_admin_bearer_only() {
    let Some(rig) = Rig::new(admin_settings(None)).await else {
        return;
    };
    let app = &rig.app;
    let (owner, member, invitee, stranger) = (wallet(), wallet(), wallet(), wallet());
    let c = rig.community(&owner, true).await;
    rig.member(c, &member, "member").await;
    rig.request(c, &invitee, "invite").await;
    let detail = format!("/v1/communities/{c}");
    let members = format!("/v1/communities/{c}/members");
    let posts = format!("/v1/communities/{c}/posts");

    let (status, _) = hit(app, Case::new("get", &detail)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "anonymous detail");
    let (status, _) = hit(app, Case::new("get", &detail).signed(&stranger).await).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "stranger detail");
    let (status, v) = hit(app, Case::new("get", &detail).signed(&invitee).await).await;
    assert_eq!(status, StatusCode::OK, "pending invitee detail: {v}");
    assert_eq!(v["data"]["role"], "none");
    let (status, v) = hit(app, Case::new("get", &detail).signed(&member).await).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["role"], "member");

    let (status, _) = hit(app, Case::new("get", &members)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "anonymous members");
    let (status, _) = hit(app, Case::new("get", &members).signed(&stranger).await).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "signed non-member");
    let (status, _) = hit(app, Case::new("get", &members).signed(&member).await).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = hit(app, Case::new("get", &members).bearer(ADMIN_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "admin bearer bypass");

    let (status, _) = hit(app, Case::new("get", &posts)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "anonymous posts");
    let (status, _) = hit(app, Case::new("get", &posts).signed(&stranger).await).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "stranger posts");
    let (status, _) = hit(app, Case::new("get", &posts).signed(&member).await).await;
    assert_eq!(status, StatusCode::OK);

    let (_, v) = hit(
        app,
        Case::new("get", "/v1/communities?onlyMemberOf=true")
            .signed(&member)
            .await,
    )
    .await;
    assert!(
        v["data"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["id"] == c.to_string()),
        "onlyMemberOf lists the private membership: {v}"
    );
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/communities?roles=member")
            .signed(&member)
            .await,
    )
    .await;
    assert!(
        v["data"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["id"] == c.to_string()),
        "{v}"
    );
    let (status, _) = hit(
        app,
        Case::new("get", "/v1/communities?roles=bogus")
            .signed(&member)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = hit(
        app,
        Case::new("get", "/v1/communities?minimal=true&search=co")
            .signed(&member)
            .await,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "minimal search needs three characters"
    );
    let (status, v) = hit(
        app,
        Case::new("get", "/v1/communities?minimal=true&search=community")
            .signed(&member)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(
        v["data"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|x| x.get("privacy").is_some() && x.get("membersCount").is_some())
    );
    assert_eq!(v["data"]["limit"], 50);
    let (status, _) = hit(
        app,
        Case::new("get", "/v1/communities?minimal=true&search=community"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    rig.finish();
}

#[tokio::test]
async fn voice_status_derives_from_the_presence_rows() {
    let _voice = VOICE_ROWS.lock().await;
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let app = &rig.app;
    let (owner, a, b, gone, viewer) = (wallet(), wallet(), wallet(), wallet(), wallet());
    let c = rig.community(&owner, false).await;
    rig.voice(c, &owner, "connected", true).await;
    rig.voice(c, &a, "connected", false).await;
    rig.voice(c, &b, "connection_interrupted", false).await;
    rig.voice(c, &gone, "disconnected", false).await;

    let (_, v) = hit(app, Case::new("get", format!("/v1/communities/{c}"))).await;
    assert_eq!(v["data"]["isLive"], true, "{v}");
    assert_eq!(
        v["data"]["voiceChatStatus"],
        json!({ "isActive": true, "participantCount": 3, "moderatorCount": 1 })
    );
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/communities?onlyWithActiveVoiceChat=true")
            .signed(&viewer)
            .await,
    )
    .await;
    let row = v["data"]["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["id"] == c.to_string())
        .expect("listed as live");
    assert_eq!(row["isLive"], true);
    assert_eq!(row["voiceChatStatus"]["participantCount"], 3);
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .signed(&viewer)
            .await,
    )
    .await;
    let chat = v["data"]["activeChats"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["communityId"] == c.to_string())
        .expect("active");
    assert_eq!(chat["participantCount"], 3);
    assert_eq!(chat["moderatorCount"], 1);
    assert_eq!(chat["isMember"], false);
    assert!(chat["communityImage"].is_null());
    assert_eq!(chat["positions"], json!([]));

    sqlx::query("UPDATE social.community_voice_chat_users SET status = 'disconnected' WHERE community_id = $1").bind(c).execute(&rig.pool).await.unwrap();
    let (_, v) = hit(app, Case::new("get", format!("/v1/communities/{c}"))).await;
    assert_eq!(v["data"]["isLive"], false);
    assert_eq!(v["data"]["voiceChatStatus"]["participantCount"], 0);
    rig.finish();
}

/// Upstream's `getActiveCommunityVoiceChatsForUser`
/// (`logic/community-voice/community-voice.ts:444-449`): a non-member sees a
/// live room only in a public, listed community; a member sees their own.
#[tokio::test]
async fn an_unlisted_room_is_listed_only_for_members() {
    let _voice = VOICE_ROWS.lock().await;
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let app = &rig.app;
    let (owner, member, viewer) = (wallet(), wallet(), wallet());
    let c = rig.community(&owner, false).await;
    rig.member(c, &member, "member").await;
    rig.voice(c, &owner, "connected", true).await;
    let listed = |v: &serde_json::Value| {
        v["data"]["activeChats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["communityId"] == c.to_string())
    };
    // Control: while the community is listed, the non-member sees the room.
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .signed(&viewer)
            .await,
    )
    .await;
    assert!(listed(&v), "{v}");
    sqlx::query("UPDATE social.communities SET unlisted = true WHERE id = $1")
        .bind(c)
        .execute(&rig.pool)
        .await
        .unwrap();
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .signed(&viewer)
            .await,
    )
    .await;
    assert!(!listed(&v), "{v}");
    let (_, v) = hit(
        app,
        Case::new("get", "/v1/community-voice-chats/active")
            .signed(&member)
            .await,
    )
    .await;
    assert!(listed(&v), "{v}");
    rig.finish();
}

/// `GET /v1/moderation/communities` as upstream's
/// `getAllCommunitiesForModeration` answers (`communities-db.ts` at
/// `fb3dcb8`): active communities only, the name or description matching,
/// upstream's row (`description`, `privacy`, the member count plus one,
/// `createdAt` with milliseconds and `Z`), the largest first.
#[tokio::test]
async fn the_moderation_listing_answers_upstreams_row() {
    let modw = wallet();
    let Some(rig) = Rig::new(admin_settings(Some(&modw))).await else {
        return;
    };
    let app = &rig.app;
    let (owner, member) = (wallet(), wallet());
    let tag = Uuid::new_v4().simple().to_string();
    let small = rig.community(&owner, false).await;
    let big = rig.community(&owner, true).await;
    let gone = rig.community(&owner, false).await;
    rig.member(big, &member, "member").await;
    for (id, name) in [(small, "small"), (big, "big"), (gone, "gone")] {
        sqlx::query("UPDATE social.communities SET description = $2, name = $3, created_at = '2026-01-02 03:04:05.678912' WHERE id = $1")
            .bind(id)
            .bind(format!("moderation {tag}"))
            .bind(format!("{name} {tag}"))
            .execute(&rig.pool)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE social.communities SET active = FALSE WHERE id = $1")
        .bind(gone)
        .execute(&rig.pool)
        .await
        .unwrap();
    let (status, v) = hit(
        app,
        Case::new(
            "get",
            format!("/v1/moderation/communities?search=moderation%20{tag}"),
        )
        .signed(&modw)
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let rows = v["data"]["results"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "the inactive one is not listed: {v}");
    assert_eq!(rows[0]["id"], big.to_string(), "the largest first");
    let mut keys: Vec<&str> = rows[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "active",
            "createdAt",
            "description",
            "id",
            "membersCount",
            "name",
            "ownerAddress",
            "privacy"
        ]
    );
    assert_eq!(rows[0]["privacy"], "private");
    assert_eq!(rows[0]["membersCount"], 3, "owner and member, plus one");
    assert_eq!(rows[1]["membersCount"], 2);
    assert_eq!(rows[0]["createdAt"], "2026-01-02T03:04:05.678Z");
    assert_eq!(v["data"]["total"], 2);
    rig.finish();
}

#[test]
fn settings_fold_restricted_names_and_validate_moderators() {
    assert_eq!(
        Settings::restricted_names_from(" Admin, decentra\u{04CF}and ,, "),
        vec!["admin", "decentraland"]
    );
    let mods = Settings::global_moderators_from(
        "0x00000000000000000000000000000000000000AA, nope ,0x00000000000000000000000000000000000000bb",
    );
    assert_eq!(
        mods.iter().map(|a| a.as_str()).collect::<Vec<_>>(),
        vec![
            "0x00000000000000000000000000000000000000aa",
            "0x00000000000000000000000000000000000000bb"
        ]
    );
    assert_eq!(
        super::thumbnail_url("http://cdn/", "x"),
        "http://cdn/social/communities/x/raw-thumbnail.png"
    );
}
