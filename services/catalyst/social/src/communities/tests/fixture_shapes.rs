//! The social-api fixtures the live replay cannot match because the mirror
//! copies no members, posts, places or profiles
//! (`tests/conformance/REPORT.md`, "empty node"): for each, the test seeds
//! exactly the rows the fixture's body names (the community, its members with
//! their roles and join times, their profile entities, the posts with
//! their like counts, the places, the online set), replays the recorded
//! request in process and compares under the replay's own semantics
//! (`catalyst_testing::conformance`: status, the recorded headers by media
//! type, the body after the fixture's volatile paths). What still differs
//! is pinned per fixture, so a change that closes or opens a difference
//! shows up here.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use catalyst_testing::conformance::fixture::Fixture;

use super::*;
use crate::communities::profiles::FaceUrls;
use crate::test_fixtures;

/// The capture's community (`social/community-by-id`) and the owner's second
/// one (`social/member-communities`).
const COMMUNITY: &str = "e99471aa-31c4-4952-abf6-99905445f43b";
const PROFILE_CDN: &str = "https://profile-images.decentraland.org";

/// The fixtures the rounds read, each parsed once.
static FIXTURES: LazyLock<BTreeMap<&'static str, Fixture>> = LazyLock::new(|| {
    [
        "community-by-id",
        "community-v2-by-id",
        "community-members",
        "community-v2-members",
        "community-members-only-online",
        "community-posts",
        "community-posts-v2",
        "community-places",
        "member-communities",
    ]
    .into_iter()
    .map(|name| (name, test_fixtures::fixture("social", name)))
    .collect()
});

fn fixture(name: &str) -> &'static Fixture {
    &FIXTURES[name]
}

fn rows<'a>(fx: &'a Fixture, key: &str) -> &'a Vec<Value> {
    fx.response.body_json.as_ref().unwrap()["data"][key]
        .as_array()
        .unwrap()
}

fn ts(iso: &str) -> chrono::NaiveDateTime {
    chrono::DateTime::parse_from_rfc3339(iso)
        .unwrap()
        .naive_utc()
}

/// The profile entity id a captured picture URL names
/// (`.../entities/<id>/face.png`).
fn entity_of(picture: &str) -> String {
    picture
        .rsplit('/')
        .nth(1)
        .expect("a face URL names its entity")
        .to_string()
}

/// A presence source answering `/peers` with `online`.
async fn peers(online: Vec<String>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = json!({ "ok": true, "peers": online.iter().map(|a| json!({ "id": a })).collect::<Vec<_>>() });
    let app = axum::Router::new().route(
        "/peers",
        axum::routing::get(move || {
            let body = body.clone();
            async move { axum::Json(body) }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

struct Seed<'a> {
    rig: &'a Rig,
}

impl Seed<'_> {
    /// Removes what an earlier run or round left for the captured ids (the
    /// rows and the memoised profiles), then the community as the by-id
    /// capture describes it.
    async fn community(&self) {
        let data = &fixture("community-by-id")
            .response
            .body_json
            .as_ref()
            .unwrap()["data"];
        self.clear().await;
        self.rig.state.profiles.clear();
        self.community_row(
            COMMUNITY,
            data["name"].as_str().unwrap(),
            data["description"].as_str().unwrap(),
            data["ownerAddress"].as_str().unwrap(),
        )
        .await;
    }

    async fn community_row(&self, id: &str, name: &str, description: &str, owner: &str) {
        sqlx::query(
            "INSERT INTO social.communities (id, name, description, owner_address) VALUES ($1::uuid, $2, $3, $4)",
        )
        .bind(id)
        .bind(name)
        .bind(description)
        .bind(owner)
        .execute(&self.rig.pool)
        .await
        .unwrap();
    }

    async fn member(&self, id: &str, address: &str, role: &str, joined: &str) {
        sqlx::query(
            "INSERT INTO social.community_members (community_id, member_address, role, joined_at) \
             VALUES ($1::uuid, $2, $3, $4) ON CONFLICT (community_id, member_address) DO NOTHING",
        )
        .bind(id)
        .bind(address)
        .bind(role)
        .bind(ts(joined))
        .execute(&self.rig.pool)
        .await
        .unwrap();
    }

    /// The active profile entity a captured row's name, claimed flag, name
    /// colour and picture describe.
    async fn profile(
        &self,
        address: &str,
        picture: &str,
        name: &str,
        claimed: bool,
        color: Option<&Value>,
    ) {
        let mut avatar = json!({
            "name": name, "hasClaimedName": claimed,
            "avatar": { "snapshots": { "face256": "bafkface" } }
        });
        if let Some(color) = color {
            avatar["nameColor"] = color.clone();
        }
        sqlx::query(
            "INSERT INTO content.deployments (deployer_address, version, entity_type, entity_id, entity_metadata, \
                 entity_timestamp, entity_pointers, local_timestamp, auth_chain) \
             VALUES ($1, 'v3', 'profile', $2, $3, timezone('UTC', now()), ARRAY[$1]::text[], timezone('UTC', now()), '[]'::json)",
        )
        .bind(address)
        .bind(entity_of(picture))
        .bind(json!({ "v": { "avatars": [avatar] } }))
        .execute(&self.rig.pool)
        .await
        .unwrap();
    }

    /// A member row of a members capture and its profile.
    async fn member_row(&self, row: &Value) {
        let address = row["memberAddress"].as_str().unwrap();
        self.member(
            COMMUNITY,
            address,
            row["role"].as_str().unwrap(),
            row["joinedAt"].as_str().unwrap(),
        )
        .await;
        if let Some(picture) = row["profilePictureUrl"].as_str() {
            self.profile(
                address,
                picture,
                row["name"].as_str().unwrap(),
                row["hasClaimedName"].as_bool().unwrap(),
                row.get("nameColor"),
            )
            .await;
        }
    }

    async fn post(&self, row: &Value) {
        let id: Uuid = row["id"].as_str().unwrap().parse().unwrap();
        sqlx::query(
            "INSERT INTO social.community_posts (id, community_id, author_address, content, created_at) \
             VALUES ($1, $2::uuid, $3, $4, $5)",
        )
        .bind(id)
        .bind(COMMUNITY)
        .bind(row["authorAddress"].as_str().unwrap())
        .bind(row["content"].as_str().unwrap())
        .bind(ts(row["createdAt"].as_str().unwrap()))
        .execute(&self.rig.pool)
        .await
        .unwrap();
        for _ in 0..row["likesCount"].as_i64().unwrap() {
            sqlx::query(
                "INSERT INTO social.community_post_likes (post_id, user_address) VALUES ($1, $2)",
            )
            .bind(id)
            .bind(wallet().address)
            .execute(&self.rig.pool)
            .await
            .unwrap();
        }
    }

    /// The places catalogue row a captured destination describes (a mirror
    /// stores the upstream row as `raw`, its columns beside).
    async fn destination(&self, row: &Value) {
        let text = |k: &str| row[k].as_str().map(str::to_string);
        let count = |k: &str| row[k].as_i64().unwrap_or(0) as i32;
        let categories: Vec<String> = row["categories"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|c| c.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO explore.place (id, base_position, title, description, creator_address, content_rating, \
                 categories, likes, dislikes, favorites, deployed_at, disabled, highlighted, raw) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        )
        .bind(text("id").unwrap())
        .bind(text("base_position").unwrap())
        .bind(text("title"))
        .bind(text("description"))
        .bind(text("creator_address"))
        .bind(text("content_rating"))
        .bind(&categories)
        .bind(count("likes"))
        .bind(count("dislikes"))
        .bind(count("favorites"))
        .bind(row["deployed_at"].as_str().map(ts))
        .bind(row["disabled"].as_bool().unwrap_or(false))
        .bind(row["highlighted"].as_bool().unwrap_or(false))
        .bind(row)
        .execute(&self.rig.pool)
        .await
        .unwrap();
    }

    /// Every row the captured ids own, so the database holds only what the
    /// test seeds (the community rows cascade to members, posts and places).
    async fn clear(&self) {
        let ids: Vec<String> = rows(fixture("member-communities"), "results")
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_string())
            .chain([COMMUNITY.to_string()])
            .collect();
        sqlx::query("DELETE FROM social.communities WHERE id = ANY($1::uuid[])")
            .bind(&ids)
            .execute(&self.rig.pool)
            .await
            .unwrap();
        let mut entities = Vec::new();
        for name in [
            "community-members",
            "community-v2-members",
            "community-members-only-online",
            "community-posts",
        ] {
            let key = if name.contains("posts") {
                "posts"
            } else {
                "results"
            };
            for row in rows(fixture(name), key) {
                for field in ["profilePictureUrl", "authorProfilePictureUrl"] {
                    if let Some(p) = row[field].as_str() {
                        entities.push(entity_of(p));
                    }
                }
            }
        }
        sqlx::query(
            "DELETE FROM content.deployments WHERE entity_type = 'profile' AND entity_id = ANY($1)",
        )
        .bind(&entities)
        .execute(&self.rig.pool)
        .await
        .unwrap();
        let places: Vec<String> = rows(fixture("community-places"), "results")
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect();
        sqlx::query("DELETE FROM explore.place WHERE id = ANY($1)")
            .bind(&places)
            .execute(&self.rig.pool)
            .await
            .unwrap();
    }
}

fn settings(presence: Option<String>) -> Settings {
    Settings {
        faces: FaceUrls::new(
            Some(PROFILE_CDN),
            "http://127.0.0.1:9/profile-images/",
            false,
        ),
        archipelago_stats_url: presence,
        ..Settings::for_tests()
    }
}

fn fields(prefix: &str, kind: &str, names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|n| format!("{prefix}{n} {kind}"))
        .collect()
}

/// What each fixture still differs in: only the keys `docs/compatibility.md`
/// records as social-api extensions ("social-api recorded extensions"), each
/// with the consumer that reads it.
fn pinned() -> BTreeMap<&'static str, Vec<String>> {
    let page = fields("data.", "extra", &["isLive"]);
    let posts = fields("data.posts[].", "extra", &["isLikedByUser"]);
    BTreeMap::from([
        ("community-by-id", page.clone()),
        ("community-v2-by-id", page),
        ("community-members", Vec::new()),
        ("community-members-only-online", Vec::new()),
        ("community-v2-members", Vec::new()),
        ("community-posts", posts.clone()),
        ("community-posts-v2", posts),
        (
            "community-places",
            fields("data.results[].", "extra", &["addedAt", "addedBy"]),
        ),
        ("member-communities", Vec::new()),
    ])
}

async fn check(rig: &Rig, got: &mut BTreeMap<&'static str, Vec<String>>, names: &[&'static str]) {
    for &name in names {
        got.insert(
            name,
            test_fixtures::replay(&rig.app, "/social-api", name, fixture(name)).await,
        );
    }
}

/// One test: the captured ids are shared rows, so the seeds take turns.
#[tokio::test]
async fn the_data_bound_social_api_fixtures_replay_against_their_seeded_rows() {
    let online: Vec<String> = rows(fixture("community-members-only-online"), "results")
        .iter()
        .map(|r| r["memberAddress"].as_str().unwrap().to_string())
        .collect();
    let Some(rig) = Rig::new(settings(Some(peers(online.clone()).await))).await else {
        return;
    };
    let seed = Seed { rig: &rig };
    let mut got = BTreeMap::new();

    // the by-id pages, with the owner's profile (`ownerName`)
    seed.community().await;
    seed.member_row(&rows(fixture("community-members"), "results")[0])
        .await;
    check(&rig, &mut got, &["community-by-id", "community-v2-by-id"]).await;

    // the member pages: each capture's rows alone; the online page also
    // holds the owner, offline (not in the presence set) and first in member
    // order, so a page that ignored `onlyOnline` would differ
    let owner = fixture("community-by-id")
        .response
        .body_json
        .as_ref()
        .unwrap()["data"]["ownerAddress"]
        .as_str()
        .unwrap()
        .to_string();
    for name in [
        "community-members",
        "community-v2-members",
        "community-members-only-online",
    ] {
        seed.community().await;
        for row in rows(fixture(name), "results") {
            seed.member_row(row).await;
        }
        if name == "community-members-only-online" {
            assert!(!online.contains(&owner), "the owner must be offline here");
            seed.member(COMMUNITY, &owner, "owner", "2025-01-01T00:00:00.000Z")
                .await;
        }
        check(&rig, &mut got, &[name]).await;
    }

    // the posts, their authors' profiles and like counts
    seed.community().await;
    let posts = fixture("community-posts");
    for row in rows(posts, "posts") {
        seed.post(row).await;
    }
    let author = &rows(posts, "posts")[0];
    seed.profile(
        author["authorAddress"].as_str().unwrap(),
        author["authorProfilePictureUrl"].as_str().unwrap(),
        author["authorName"].as_str().unwrap(),
        author["authorHasClaimedName"].as_bool().unwrap(),
        None,
    )
    .await;
    check(&rig, &mut got, &["community-posts", "community-posts-v2"]).await;

    // the places, added by the owner, and their destinations in the catalogue
    seed.community().await;
    for row in rows(fixture("community-places"), "results") {
        sqlx::query("INSERT INTO social.community_places (place_id, community_id, added_by) VALUES ($1, $2::uuid, $3)")
            .bind(row["id"].as_str().unwrap())
            .bind(COMMUNITY)
            .bind(&owner)
            .execute(&rig.pool)
            .await
            .unwrap();
        seed.destination(row).await;
    }
    check(&rig, &mut got, &["community-places"]).await;

    // the owner's communities, joined newest first against the capture's
    // order (it names no join time): both sort by role, then name
    seed.community().await;
    let mine = fixture("member-communities");
    for (joined, row) in ["2025-08-21T19:31:24.575Z", "2025-08-20T19:31:24.575Z"]
        .into_iter()
        .zip(rows(mine, "results"))
    {
        let id = row["id"].as_str().unwrap();
        if id != COMMUNITY {
            seed.community_row(
                id,
                row["name"].as_str().unwrap(),
                "",
                row["ownerAddress"].as_str().unwrap(),
            )
            .await;
        }
        seed.member(id, &owner, row["role"].as_str().unwrap(), joined)
            .await;
    }
    assert_eq!(
        rows(mine, "results").len(),
        2,
        "one join time per captured row"
    );
    check(&rig, &mut got, &["member-communities"]).await;

    seed.clear().await;
    rig.finish();
    let report: String = got
        .iter()
        .map(|(name, notes)| format!("{name}:\n{}\n", notes.join("\n")))
        .collect();
    assert_eq!(
        got,
        pinned(),
        "the differences from the captures changed:\n{report}"
    );
}
