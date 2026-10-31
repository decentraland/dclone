//! Every route that serves the ONE blob store by hash refuses a withheld
//! hash, whichever table handed the hash to it. The routes come off the
//! router tables themselves: each module's `.route("...")` registrations
//! (its `mod.rs` source, read at compile time), every path parameter filled
//! with the listed hash (or, where a route is keyed otherwise, the id of the
//! row that names the hash), every one of them requested under each of the
//! module's mounts, in four phases: the projection empty (the seeded routes
//! must serve the bytes: the positive control that the seeding is right),
//! the hash on the operator's half (no route may hand the bytes out), the
//! profile entity alone (its images refuse on the entity, the rest serve
//! again) and the hash on the camera reel's half. A new blob route in any
//! of these tables is enumerated the day it lands; a route in another
//! module is a gap in `MODULES` below, and the worlds nest is pinned by
//! source to the content handler it reuses.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use crate::auth::hash_bytes_v1;
use crate::storage::Storage;

struct Module {
    name: &'static str,
    /// The module's router table (`.route("...")` registrations).
    table: &'static str,
    /// Where `serve` mounts the router (`""` is the root).
    mounts: &'static [&'static str],
    /// The router's own nests, prefixed to every table path.
    nests: &'static [&'static str],
}

const MODULES: &[Module] = &[
    Module {
        name: "content",
        table: include_str!(concat!(
            env!("DCL_ONE_ROOT"),
            "/services/catalyst/content/src/lib.rs"
        )),
        mounts: &[""],
        nests: &[""],
    },
    Module {
        name: "builder",
        table: include_str!(concat!(
            env!("DCL_ONE_ROOT"),
            "/services/catalyst/builder/src/lib.rs"
        )),
        mounts: &["", "/builder-api"],
        nests: &[""],
    },
    Module {
        name: "camera_reel",
        table: include_str!(concat!(
            env!("DCL_ONE_ROOT"),
            "/services/catalyst/camera-reel/src/lib.rs"
        )),
        mounts: &["/camera-reel-service"],
        nests: &["", "/api"],
    },
    Module {
        name: "communities",
        table: include_str!(concat!(
            env!("DCL_ONE_ROOT"),
            "/services/catalyst/social/src/communities/mod.rs"
        )),
        mounts: &["", "/social-api"],
        nests: &[""],
    },
    Module {
        name: "profile_images",
        table: include_str!(concat!(
            env!("DCL_ONE_ROOT"),
            "/services/catalyst/social/src/profile_images/mod.rs"
        )),
        mounts: &["", "/profile-images"],
        nests: &[""],
    },
];

/// The string literal of every `.route("...")` registration in `source`
/// (a registration through a constant is not a literal and is skipped).
fn route_paths(source: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut rest = source;
    while let Some(at) = rest.find(".route(") {
        rest = &rest[at + ".route(".len()..];
        let trimmed = rest.trim_start();
        if let Some(literal) = trimmed.strip_prefix('"')
            && let Some(end) = literal.find('"')
        {
            let path = &literal[..end];
            if !paths.iter().any(|p| p == path) {
                paths.push(path.to_string());
            }
        }
    }
    paths
}

struct Corpus {
    hash: String,
    entity: String,
    community: Uuid,
}

impl Corpus {
    /// `{param}` filled by name: the community id where the route is keyed
    /// by it, the profile entity where it asks for one, the hash everywhere
    /// else (so a route that serves blobs under any other parameter name is
    /// probed with the listed hash too).
    fn fill(&self, module: &str, path: &str) -> String {
        path.split('/')
            .map(|segment| {
                if !segment.starts_with('{') {
                    return segment.to_string();
                }
                let name = segment
                    .trim_start_matches('{')
                    .trim_start_matches('*')
                    .trim_end_matches('}');
                match (module, name) {
                    ("communities", "id") => self.community.to_string(),
                    (_, "entity") => self.entity.clone(),
                    _ => self.hash.clone(),
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    }
}

struct Rig {
    pool: PgPool,
    storage: Arc<Storage>,
    content: Arc<crate::content::ContentState>,
    builder: crate::builder::SharedState,
    reel: crate::camera_reel::SharedState,
    communities: crate::social::communities::SharedState,
    profile: crate::social::profile_images::SharedState,
    deployment: i32,
    image: Uuid,
    dir: std::path::PathBuf,
}

impl Rig {
    async fn new(corpus: &Corpus, blob: &Bytes) -> Option<Self> {
        let pool = crate::db::scratch::pool(6).await?;
        let dir = std::env::temp_dir().join(format!(
            "catalyst-served-routes-{}",
            Uuid::new_v4().simple()
        ));
        let storage = Arc::new(Storage::from_config(&dir).await.unwrap());
        storage.store(&corpus.hash, blob.clone()).await.unwrap();

        let content = Arc::new(crate::content::ContentState::new(
            pool.clone(),
            crate::content::Settings::for_tests(),
            storage.clone(),
        ));
        let builder = Arc::new(
            crate::builder::State::new(
                pool.clone(),
                storage.clone(),
                storage.denylist().clone(),
                crate::http::Outbound::for_tests(),
                None,
                None,
                crate::builder::Settings::default(),
            )
            .unwrap(),
        );
        let reel = Arc::new(crate::camera_reel::State::new(
            pool.clone(),
            storage.clone(),
            crate::camera_reel::Settings::new("http://127.0.0.1/camera-reel-service"),
            None,
        ));
        let communities = Arc::new(crate::social::communities::State::new(
            pool.clone(),
            storage.clone(),
            crate::social::communities::Settings::for_tests(),
            None,
            &crate::http::Outbound::for_tests(),
            crate::social::communities::State::explore_for_tests(&pool),
        ));
        let profile = Arc::new(crate::social::profile_images::ProfileImagesState::new(
            pool.clone(),
            storage.clone(),
            crate::http::Outbound::for_tests(),
            crate::social::profile_images::Settings::for_tests(dir.join("profile-cache")),
        ));

        let user = format!(
            "0x{:0>40}",
            corpus
                .community
                .simple()
                .to_string()
                .chars()
                .take(40)
                .collect::<String>()
        );
        let image = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO camera_reel.images (id, user_address, image_hash, thumbnail_hash, is_public, metadata) \
             VALUES ($1, $2, $3, $3, TRUE, $4)",
        )
        .bind(image)
        .bind(&user)
        .bind(&corpus.hash)
        .bind(serde_json::json!({ "userAddress": user }))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO social.communities (id, name, description, owner_address) VALUES ($1, 'served routes', '', $2)")
            .bind(corpus.community)
            .bind(&user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO social.community_thumbnails (community_id, thumbnail_hash) VALUES ($1, $2)")
            .bind(corpus.community)
            .bind(&corpus.hash)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO builder.uploaded_contents (hash, size, uploaded_by) VALUES ($1, $2, $3)",
        )
        .bind(&corpus.hash)
        .bind(blob.len() as i64)
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap();
        let deployment: i32 = sqlx::query_scalar(
            "INSERT INTO content.deployments (deployer_address, version, entity_type, entity_id, entity_metadata, \
                 entity_timestamp, entity_pointers, local_timestamp, auth_chain) \
             VALUES ($1, 'v3', 'profile', $2, '{\"v\":{\"avatars\":[]}}'::json, to_timestamp(1700000000), ARRAY[$1], \
                     timezone('UTC', now()), '[]'::json) RETURNING id",
        )
        .bind(&user)
        .bind(&corpus.entity)
        .fetch_one(&pool)
        .await
        .unwrap();
        for key in ["face256.png", "body.png"] {
            sqlx::query("INSERT INTO content.content_files (deployment, content_hash, key) VALUES ($1, $2, $3)")
                .bind(deployment)
                .bind(&corpus.hash)
                .bind(key)
                .execute(&pool)
                .await
                .unwrap();
        }

        Some(Self {
            pool,
            storage,
            content,
            builder,
            reel,
            communities,
            profile,
            deployment,
            image,
            dir,
        })
    }

    fn router(&self, module: &str, mount: &str) -> Router {
        let router = match module {
            "content" => crate::content::build_router(self.content.clone()),
            "builder" if mount.is_empty() => crate::builder::root_router(self.builder.clone()),
            "builder" => crate::builder::router(self.builder.clone()),
            "camera_reel" => crate::camera_reel::router(self.reel.clone()),
            "communities" => crate::social::communities::router(self.communities.clone()),
            "profile_images" => crate::social::profile_images::router(self.profile.clone()),
            other => panic!("no router for {other}"),
        };
        if mount.is_empty() {
            router
        } else {
            Router::new().nest(mount, router)
        }
    }

    async fn get(&self, module: &str, mount: &str, path: &str) -> (StatusCode, Bytes) {
        let request = Request::get(path).body(Body::empty()).unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(30),
            self.router(module, mount).oneshot(request),
        )
        .await
        .unwrap_or_else(|_| panic!("{module} {path} did not answer in 30 s"))
        .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, body)
    }

    async fn done(self, corpus: &Corpus) {
        sqlx::query("DELETE FROM content.content_files WHERE deployment = $1")
            .bind(self.deployment)
            .execute(&self.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM content.deployments WHERE id = $1")
            .bind(self.deployment)
            .execute(&self.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM camera_reel.images WHERE id = $1")
            .bind(self.image)
            .execute(&self.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM social.communities WHERE id = $1")
            .bind(corpus.community)
            .execute(&self.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM builder.uploaded_contents WHERE hash = $1")
            .bind(&corpus.hash)
            .execute(&self.pool)
            .await
            .unwrap();
        let _ = tokio::fs::remove_dir_all(&self.dir).await;
    }
}

/// Every (module, mount, request path) the router tables yield.
fn enumerate(corpus: &Corpus) -> Vec<(&'static str, &'static str, String)> {
    let mut out = Vec::new();
    for module in MODULES {
        let paths = route_paths(module.table);
        assert!(
            !paths.is_empty(),
            "{}: no route literal found in its table",
            module.name
        );
        for mount in module.mounts {
            for nest in module.nests {
                for path in &paths {
                    out.push((
                        module.name,
                        *mount,
                        format!("{mount}{nest}{}", corpus.fill(module.name, path)),
                    ));
                }
            }
        }
    }
    out
}

/// The routes that serve the corpus blob once its rows are seeded: the
/// positive control of every phase.
fn blob_routes(corpus: &Corpus) -> Vec<(&'static str, &'static str, String)> {
    let Corpus {
        hash,
        entity,
        community,
    } = corpus;
    vec![
        ("content", "", format!("/contents/{hash}")),
        ("builder", "", format!("/v1/storage/contents/{hash}")),
        (
            "builder",
            "/builder-api",
            format!("/builder-api/v1/storage/contents/{hash}"),
        ),
        (
            "builder",
            "/builder-api",
            format!("/builder-api/contents/{hash}"),
        ),
        (
            "camera_reel",
            "/camera-reel-service",
            format!("/camera-reel-service/api/images/{hash}"),
        ),
        (
            "communities",
            "",
            format!("/social/communities/{community}/raw-thumbnail.png"),
        ),
        (
            "communities",
            "/social-api",
            format!("/social-api/social/communities/{community}/raw-thumbnail.png"),
        ),
        ("profile_images", "", format!("/entities/{entity}/face.png")),
        ("profile_images", "", format!("/entities/{entity}/body.png")),
        (
            "profile_images",
            "/profile-images",
            format!("/profile-images/entities/{entity}/face.png"),
        ),
        (
            "profile_images",
            "/profile-images",
            format!("/profile-images/entities/{entity}/body.png"),
        ),
    ]
}

#[test]
fn the_router_tables_yield_the_known_blob_routes() {
    let corpus = Corpus {
        hash: "bafyhash".into(),
        entity: "bafyentity".into(),
        community: Uuid::nil(),
    };
    let all: Vec<String> = enumerate(&corpus)
        .into_iter()
        .map(|(_, _, path)| path)
        .collect();
    for (_, _, path) in blob_routes(&corpus) {
        assert!(
            all.contains(&path),
            "{path} is not in the enumerated tables: {all:#?}"
        );
    }
    assert!(
        all.iter()
            .any(|p| p == "/camera-reel-service/api/images/bafyhash/metadata")
    );
    assert!(
        all.iter()
            .any(|p| p == "/builder-api/v1/storage/contents/bafyhash/exists")
    );
    assert!(
        all.iter()
            .any(|p| p == "/profile-images/entities/bafyentity/face.png")
    );
    assert!(all.len() > 150, "{} routes", all.len());
}

/// The worlds nest serves `/contents/{hashId}` and `/ipfs/{hashId}` with the
/// content server's handler over the same state, so the enumeration above
/// covers it without the worlds fixtures; this pins that reuse.
#[test]
fn the_worlds_nest_reuses_the_content_handler() {
    let worlds = include_str!(concat!(
        env!("DCL_ONE_ROOT"),
        "/services/catalyst/worlds/src/lib.rs"
    ));
    for path in ["/contents/{hashId}", "/ipfs/{hashId}"] {
        let at = worlds
            .find(&format!("\"{path}\""))
            .unwrap_or_else(|| panic!("worlds registers {path}"));
        let registration = &worlds[at..at + 200];
        assert!(
            registration.contains("crate::content::get_content::get_content"),
            "{path} is not the content handler: {registration}"
        );
    }
}

#[tokio::test]
async fn every_blob_route_refuses_a_withheld_hash_and_a_withheld_entity() {
    let tag = Uuid::new_v4().simple().to_string();
    let blob = Bytes::from(format!("\u{89}PNG served-routes corpus {tag}").into_bytes());
    let corpus = Corpus {
        hash: hash_bytes_v1(&blob),
        entity: hash_bytes_v1(format!("profile entity {tag}").as_bytes()),
        community: Uuid::new_v4(),
    };
    let Some(rig) = Rig::new(&corpus, &blob).await else {
        eprintln!("skipping: CATALYST_TEST_DATABASE_URL not set");
        return;
    };
    let denylist = rig.storage.denylist();
    assert!(
        Arc::ptr_eq(denylist, &rig.content.denylist),
        "the content state projects the store's list"
    );
    assert!(
        Arc::ptr_eq(denylist, &rig.reel.denylist),
        "the camera reel adds its verdicts to the store's list"
    );
    assert!(
        Arc::ptr_eq(denylist, &rig.builder.denylist),
        "the builder consults the store's list"
    );

    let routes = enumerate(&corpus);
    let served = blob_routes(&corpus);

    // Phase 1: nothing listed; the seeded routes serve the bytes.
    for (module, mount, path) in &served {
        let (status, body) = rig.get(module, mount, path).await;
        assert_eq!(
            (status, &body[..]),
            (StatusCode::OK, &blob[..]),
            "open: {path}"
        );
    }

    // Phase 2: the hash on the operator's half; no route hands the bytes out.
    denylist.operator_insert(&corpus.hash);
    for (module, mount, path) in &routes {
        let (status, body) = rig.get(module, mount, path).await;
        assert_ne!(
            &body[..],
            &blob[..],
            "{module} {path} served a withheld hash ({status})"
        );
    }
    for (module, mount, path) in &served {
        let (status, _) = rig.get(module, mount, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "withheld hash: {path}");
    }
    let (status, _) = rig
        .get(
            "camera_reel",
            "/camera-reel-service",
            &format!("/camera-reel-service/api/images/{}/metadata", corpus.hash),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the reel's metadata of a withheld hash"
    );
    let (status, _) = rig
        .get(
            "builder",
            "/builder-api",
            &format!("/builder-api/v1/storage/contents/{}/exists", corpus.hash),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the builder's existence probe of a withheld hash"
    );

    // Phase 3: the profile entity alone; its images refuse, the rest serve again.
    assert!(denylist.operator_remove(&corpus.hash));
    denylist.operator_insert(&corpus.entity);
    for (module, mount, path) in &served {
        let (status, body) = rig.get(module, mount, path).await;
        if *module == "profile_images" {
            assert_eq!(status, StatusCode::NOT_FOUND, "withheld entity: {path}");
        } else {
            assert_eq!(
                (status, &body[..]),
                (StatusCode::OK, &blob[..]),
                "the hash is not listed: {path}"
            );
        }
    }
    for (module, mount, path) in &routes {
        let (_, body) = rig.get(module, mount, path).await;
        if *module == "profile_images" {
            assert_ne!(
                &body[..],
                &blob[..],
                "{path} served a withheld entity's file"
            );
        }
    }

    // Phase 4: the camera reel's half withholds the same way.
    assert!(denylist.operator_remove(&corpus.entity));
    denylist.add(&corpus.hash);
    for (module, mount, path) in &served {
        let (status, _) = rig.get(module, mount, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "reel-withheld hash: {path}");
    }
    denylist.remove(&corpus.hash);

    rig.done(&corpus).await;
}
