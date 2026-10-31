//! The social-api answers production gives (`decentraland/social-service-ea`
//! at `fb3dcb8`; captures `tests/fixtures/conformance/social/social/`): the
//! CORS layer, the probes under the gateway prefix, the error bodies, the
//! member-communities order and the community places merged with their
//! destinations.

use axum::http::{HeaderMap, Method};

use super::*;
use crate::test_fixtures;

fn fixture(name: &str) -> catalyst_testing::conformance::fixture::Fixture {
    test_fixtures::fixture("social", name)
}

async fn replayed(app: &Router, prefix: &str, name: &str) -> Vec<String> {
    test_fixtures::replay(app, prefix, name, &fixture(name)).await
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(path);
    for (k, v) in headers {
        request = request.header(*k, *v);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, headers, body.to_vec())
}

/// The headers the CORS layer owns; `allow` (the route's methods) and
/// `content-length` are axum's, added after the layer as before it.
fn names(headers: &HeaderMap) -> Vec<&str> {
    let mut names: Vec<&str> = headers
        .keys()
        .map(|k| k.as_str())
        .filter(|k| !matches!(*k, "allow" | "content-length"))
        .collect();
    names.sort();
    names
}

const ORIGIN: (&str, &str) = ("origin", "https://decentraland.org");

#[tokio::test]
async fn the_preflight_and_an_origin_get_answer_as_production() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    for prefix in ["", "/social-api"] {
        let notes = replayed(&rig.app, prefix, "preflight-communities").await;
        assert!(notes.is_empty(), "{prefix}: {notes:#?}");
    }

    // every route of the router, the local extensions too, and any requested headers
    let (status, headers, body) = send(
        &rig.app,
        Method::OPTIONS,
        "/social-api/v1/friends",
        &[
            ORIGIN,
            ("access-control-request-method", "POST"),
            (
                "access-control-request-headers",
                "x-identity-auth-chain-0, content-type",
            ),
        ],
    )
    .await;
    assert_eq!((status, body.len()), (StatusCode::NO_CONTENT, 0));
    assert_eq!(
        names(&headers),
        [
            "access-control-allow-headers",
            "access-control-allow-methods",
            "access-control-allow-origin",
            "access-control-max-age",
            "vary",
            "x-content-type-options",
        ]
    );
    for (name, value) in [
        ("access-control-allow-headers", "*"),
        (
            "access-control-allow-methods",
            "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
        ),
        ("access-control-allow-origin", "*"),
        ("access-control-max-age", "86400"),
        ("vary", "accept-encoding"),
        ("x-content-type-options", "nosniff"),
    ] {
        assert_eq!(headers[name], value, "{name}");
    }

    // the Origin GET: the capture's CORS headers, no credentials or exposed headers
    let fx = fixture("communities-list-origin");
    let (status, headers, _) = send(
        &rig.app,
        Method::GET,
        "/v1/communities?limit=1&offset=0",
        &[ORIGIN],
    )
    .await;
    assert_eq!(status.as_u16(), fx.response.status);
    for (name, value) in &fx.response.headers {
        let actual = headers[name.as_str()].to_str().unwrap();
        if name == "content-type" {
            assert!(actual.starts_with(value.as_str()), "{name}: {actual}");
        } else {
            assert_eq!(actual, value, "{name}");
        }
    }
    assert!(headers.get("access-control-allow-credentials").is_none());
    assert!(headers.get("access-control-expose-headers").is_none());

    // without an Origin, as the captures without one: no CORS headers
    let (_, headers, _) = send(&rig.app, Method::GET, "/v1/communities?limit=1", &[]).await;
    assert!(headers.get("access-control-allow-origin").is_none());
    assert!(headers.get("vary").is_none());
    assert_eq!(headers["x-content-type-options"], "nosniff");
    rig.finish();
}

#[tokio::test]
async fn the_probes_answer_under_the_gateway_prefix_only() {
    let probes = Router::new().nest("/social-api", super::super::health_router());
    for name in ["health-live", "health-ready", "health-startup"] {
        let notes = replayed(&probes, "/social-api", name).await;
        assert!(notes.is_empty(), "{name}: {notes:#?}");
    }
    let (_, headers, body) = send(&probes, Method::GET, "/social-api/health/ready", &[]).await;
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"details":{"httpServer":{"status":"pass"}},"status":"pass"}"#
    );
    assert_eq!(
        headers["content-type"],
        "application/health+json; charset=utf-8"
    );
    let (_, _, body) = send(&probes, Method::GET, "/social-api/health/live", &[]).await;
    assert_eq!(body, b"alive");

    // the communities router, merged at the node's root, has none
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    for path in ["/health/live", "/health/ready", "/social-api/health/live"] {
        let (status, _, _) = send(&rig.app, Method::GET, path, &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
    rig.finish();
}

#[tokio::test]
async fn the_error_bodies_are_productions() {
    let moderator = wallet();
    let Some(rig) = Rig::new(admin_settings(Some(&moderator))).await else {
        return;
    };
    for prefix in ["", "/social-api"] {
        for name in [
            "community-by-id-bad-id",
            "member-communities-bad-address",
            "managed-communities-401-no-bearer",
        ] {
            let notes = replayed(&rig.app, prefix, name).await;
            assert!(notes.is_empty(), "{prefix} {name}: {notes:#?}");
        }
    }
    let invalid_identifier = json!({ "error": "Bad request", "message": "Invalid identifier" });
    for path in [
        "/v2/communities/not-a-uuid",
        "/v1/communities/not-a-uuid/places",
        "/v1/communities/not-a-uuid/posts",
    ] {
        let (status, body) = hit(&rig.app, Case::new("get", path)).await;
        assert_eq!(
            (status, body),
            (StatusCode::BAD_REQUEST, invalid_identifier.clone()),
            "{path}"
        );
    }
    let user = wallet();
    let (status, body) = hit(
        &rig.app,
        Case::new("delete", "/v1/communities/not-a-uuid")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(
        (status, body),
        (StatusCode::BAD_REQUEST, invalid_identifier)
    );

    // upstream's bearerTokenMiddleware on both bearer routes: an empty header
    // reads as missing, any other that is not `Bearer <token>` is invalid
    let address = format!("0x{}", "8d".repeat(20));
    let managed = format!("/v1/communities/{address}/managed");
    let by_ids = format!("/v1/members/{address}/communities");
    for (method, path) in [("get", managed.as_str()), ("post", by_ids.as_str())] {
        for (header, message) in [
            (None, "Authorization header is missing"),
            (Some(""), "Authorization header is missing"),
            (Some("Bearer wrong"), "Invalid authorization header"),
            (
                Some("bearer cg-social-admin"),
                "Invalid authorization header",
            ),
            (Some("Basic abc"), "Invalid authorization header"),
        ] {
            let mut case = Case::new(method, path).json(&json!({ "communityIds": [] }));
            if let Some(value) = header {
                case.headers
                    .push(("authorization".to_string(), value.to_string()));
            }
            let (status, body) = hit(&rig.app, case).await;
            assert_eq!(
                (status, body),
                (
                    StatusCode::UNAUTHORIZED,
                    json!({ "error": "Not Authorized", "message": message })
                ),
                "{method} {path} {header:?}"
            );
        }
    }

    // the moderation refusal: `{ message }` alone
    let (status, body) = hit(
        &rig.app,
        Case::new("get", "/v1/moderation/communities")
            .signed(&user)
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        Some(body),
        fixture("moderation-communities-signed-not-moderator")
            .response
            .body_json
    );
    rig.finish();
}

/// Without `API_ADMIN_TOKEN` upstream mounts neither bearer route.
#[tokio::test]
async fn the_bearer_routes_are_hidden_without_a_token() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let address = format!("0x{}", "8d".repeat(20));
    for (method, path) in [
        ("get", format!("/v1/communities/{address}/managed")),
        ("post", format!("/v1/members/{address}/communities")),
    ] {
        let (status, _) = hit(&rig.app, Case::new(method, &path).bearer(ADMIN_TOKEN)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
    }
    rig.finish();
}

/// A stored `nameColor` read through `jsonb` (which keeps `1.0` as written)
/// comes out as `JSON.stringify` writes it.
#[tokio::test]
async fn a_stored_name_color_reads_as_production_writes_it() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let stored: Value = sqlx::query_scalar(
        "SELECT ('{\"v\":{\"avatars\":[{\"nameColor\":{\"r\":1.0,\"g\":1.50,\"b\":0.6801473,\"a\":1}}]}}'::jsonb) #> '{v,avatars,0,nameColor}'",
    )
    .fetch_one(&rig.pool)
    .await
    .unwrap();
    let color = super::super::profiles::NameColor::from_profile(&stored).unwrap();
    assert_eq!(
        serde_json::to_string(&color).unwrap(),
        r#"{"r":1,"g":1.5,"b":0.6801473,"a":1}"#
    );
    rig.finish();
}

#[tokio::test]
async fn a_wallets_communities_come_by_role_then_name() {
    let Some(rig) = Rig::new(admin_settings(None)).await else {
        return;
    };
    let me = wallet();
    let other = wallet();
    let tag = Uuid::new_v4().simple().to_string();
    // joined in an order neither the role nor the name follows
    let mut ids = Vec::new();
    for (name, role) in [
        ("Aardvark", "member"),
        ("Birch", "owner"),
        ("Aspen", "moderator"),
        ("Alder", "owner"),
    ] {
        let id = CommunityId::new();
        sqlx::query(
            "INSERT INTO social.communities (id, name, description, owner_address) VALUES ($1, $2, '', $3)",
        )
        .bind(id)
        .bind(format!("{name} {tag}"))
        .bind(&other.address)
        .execute(&rig.pool)
        .await
        .unwrap();
        rig.member(id, &me, role).await;
        ids.push(id);
    }
    let order = |v: &Value| -> Vec<String> {
        v["data"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r["name"]
                    .as_str()
                    .unwrap()
                    .split(' ')
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect()
    };
    let (status, v) = hit(
        &rig.app,
        Case::new("get", format!("/v1/members/{}/communities", me.address)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(order(&v), ["Alder", "Birch", "Aspen", "Aardvark"]);
    assert!(v["data"]["results"][0].get("joinedAt").is_none());
    let (status, v) = hit(
        &rig.app,
        Case::new("get", format!("/v1/communities/{}/managed", me.address)).bearer(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(order(&v), ["Alder", "Birch", "Aspen"]);
    sqlx::query("DELETE FROM social.communities WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&rig.pool)
        .await
        .unwrap();
    rig.finish();
}

/// A catalogue row shaped as the captured destination (`social/community-places`).
async fn seed_destination(
    pool: &PgPool,
    id: &str,
    title: &str,
    disabled: bool,
    world_name: Option<&str>,
) {
    let mut raw =
        fixture("community-places").response.body_json.unwrap()["data"]["results"][0].clone();
    raw["id"] = json!(id);
    raw["title"] = json!(title);
    raw["disabled"] = json!(disabled);
    if let Some(name) = world_name {
        raw["world"] = json!(true);
        raw["world_name"] = json!(name);
    }
    sqlx::query(
        "INSERT INTO explore.place (id, base_position, title, disabled, raw) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(raw["base_position"].as_str().unwrap())
    .bind(title)
    .bind(disabled)
    .bind(&raw)
    .execute(pool)
    .await
    .unwrap();
}

struct Places {
    community: CommunityId,
    seeded: Uuid,
    missing: Uuid,
    disabled: Uuid,
    world: Uuid,
    world_name: String,
    upper: Uuid,
}

impl Places {
    /// A public community whose places are, newest first: a seeded place,
    /// one with no destination, a disabled one, a world name, a seeded place
    /// stored in uppercase and an id that is neither a UUID nor a name.
    async fn seed(rig: &Rig) -> Self {
        let owner = wallet();
        let community = rig.community(&owner, false).await;
        let tag = Uuid::new_v4().simple().to_string();
        let p = Self {
            community,
            seeded: Uuid::new_v4(),
            missing: Uuid::new_v4(),
            disabled: Uuid::new_v4(),
            world: Uuid::new_v4(),
            world_name: format!("w{}.dcl.eth", &tag[..12]),
            upper: Uuid::new_v4(),
        };
        seed_destination(
            &rig.pool,
            &p.seeded.to_string(),
            "seeded place",
            false,
            None,
        )
        .await;
        seed_destination(
            &rig.pool,
            &p.disabled.to_string(),
            "disabled place",
            true,
            None,
        )
        .await;
        seed_destination(
            &rig.pool,
            &p.world.to_string(),
            "a world",
            false,
            Some(&p.world_name),
        )
        .await;
        seed_destination(&rig.pool, &p.upper.to_string(), "upper place", false, None).await;
        for (age, id) in p.stored().into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO social.community_places (place_id, community_id, added_by, added_at) \
                 VALUES ($1, $2, $3, timezone('UTC', now()) - make_interval(mins => $4))",
            )
            .bind(&id)
            .bind(community)
            .bind(&owner.address)
            .bind(age as i32)
            .execute(&rig.pool)
            .await
            .unwrap();
        }
        p
    }

    fn stored(&self) -> Vec<String> {
        vec![
            self.seeded.to_string(),
            self.missing.to_string(),
            self.disabled.to_string(),
            self.world_name.clone(),
            self.upper.to_string().to_uppercase(),
            "not-a-place".to_string(),
        ]
    }

    async fn rows(&self, rig: &Rig) -> Vec<Value> {
        let (status, v) = hit(
            &rig.app,
            Case::new("get", format!("/v1/communities/{}/places", self.community)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["data"]["total"], 6, "{v}");
        let rows = v["data"]["results"].as_array().unwrap().clone();
        let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(ids, self.stored(), "added_at DESC");
        rows
    }

    async fn clear(&self, rig: &Rig) {
        let ids = [self.seeded, self.disabled, self.world, self.upper].map(|u| u.to_string());
        sqlx::query("DELETE FROM explore.place WHERE id = ANY($1)")
            .bind(&ids[..])
            .execute(&rig.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM social.communities WHERE id = $1")
            .bind(self.community)
            .execute(&rig.pool)
            .await
            .unwrap();
    }
}

fn keys(row: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = row
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    keys
}

const BARE: [&str; 3] = ["addedAt", "addedBy", "id"];

#[tokio::test]
async fn a_community_place_is_its_destination_when_the_id_names_one_exactly() {
    let Some(rig) = Rig::new(Settings::for_tests()).await else {
        return;
    };
    let places = Places::seed(&rig).await;
    let rows = places.rows(&rig).await;

    // the seeded place: production's destination row, `addedBy`/`addedAt` kept
    let seeded = &rows[0];
    assert_eq!(seeded["title"], "seeded place");
    assert_eq!(seeded["world"], false);
    assert!(
        seeded["positions"]
            .as_array()
            .is_some_and(|p| !p.is_empty()),
        "{seeded}"
    );
    assert!(
        seeded["addedBy"].is_string() && seeded["addedAt"].is_string(),
        "{seeded}"
    );
    assert!(
        seeded.get("communityId").is_none() && seeded.get("kind").is_none(),
        "{seeded}"
    );
    let captured = fixture("community-places").response.body_json.unwrap();
    let mut expected: Vec<&str> = captured["data"]["results"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .chain(["addedAt", "addedBy"])
        .collect();
    expected.sort();
    assert_eq!(keys(seeded), expected);

    // no destination, a disabled place, a world name, an uppercase id, neither: bare
    for row in &rows[1..] {
        assert_eq!(keys(row), BARE, "{row}");
    }

    // the world and the uppercase place exist: only the exact-id merge leaves them bare
    let explore = &rig.state.explore;
    let found = crate::explore::places::destinations::destinations_by_ids(
        explore,
        vec![places.world.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].world_name.as_deref(),
        Some(places.world_name.as_str())
    );
    let by_name = crate::explore::places::destinations::destinations_by_ids(
        explore,
        vec![places.world_name.clone()],
    )
    .await
    .unwrap();
    assert!(by_name.is_empty(), "array semantics match ids only");
    let found = crate::explore::places::destinations::destinations_by_ids(
        explore,
        vec![places.upper.to_string().to_uppercase()],
    )
    .await
    .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, places.upper.to_string());
    // as `POST /api/destinations` answers an array body: no ids, no rows;
    // more than the batch cap, 400
    let none = crate::explore::places::destinations::destinations_by_ids(explore, Vec::new())
        .await
        .unwrap();
    assert!(none.is_empty());
    let too_many = crate::explore::places::destinations::destinations_by_ids(
        explore,
        vec![places.upper.to_string(); crate::explore::places::destinations::MAX_BATCH_ITEMS + 1],
    )
    .await;
    assert!(
        matches!(
            too_many,
            Err(crate::shared::errors::ApiError::Http { status: 400, .. })
        ),
        "{too_many:?}"
    );

    places.clear(&rig).await;
    rig.finish();
}

/// The explore state reads a pool that never connects; the communities
/// state's own pool is the scratch one.
fn unreachable_explore(_: &PgPool) -> crate::explore::SharedState {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://nobody@127.0.0.1:1/nowhere")
        .unwrap();
    Arc::new(crate::explore::ExploreState::new(
        pool,
        crate::explore::Settings::for_tests(),
        None,
    ))
}

#[tokio::test]
async fn a_failed_destinations_lookup_answers_the_bare_rows() {
    let Some(rig) = Rig::with_explore(Settings::for_tests(), unreachable_explore).await else {
        return;
    };
    let places = Places::seed(&rig).await;
    for row in places.rows(&rig).await {
        assert_eq!(keys(&row), BARE, "{row}");
    }
    places.clear(&rig).await;

    // an empty page asks nothing
    let owner = wallet();
    let empty = rig.community(&owner, false).await;
    let (status, v) = hit(
        &rig.app,
        Case::new("get", format!("/v1/communities/{empty}/places")),
    )
    .await;
    assert_eq!(
        (status, &v["data"]["results"]),
        (StatusCode::OK, &json!([]))
    );
    rig.finish();
}
