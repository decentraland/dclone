//! The community and friendship REST surface unity-explorer calls as
//! `social-api` (`/v1/communities`, `/v2/communities`, members, roles, bans,
//! requests, invites, posts, places, the active voice-chat listing, mutes,
//! friends and their plaintext messages, referral progress, the operator
//! admin and moderation listings), over `social.*` and the ONE pool, blob
//! store and outbound client.
//!
//! Authorization is the old service's, route for route: the ADR-44 signed
//! fetch (`auth`), the membership tier and capability matrix
//! (`crate::identity`, read through `standing`), the ban semantics
//! (a banned wallet has no member row; a re-ban reactivates), the admin
//! bearer and the global-moderator allow-list. The federation envelopes each
//! write route also accepts, the federation log and its content routes are
//! `fed` (over `crate::fed`). A banned or kicked member of a private
//! community leaves its voice room ([`State::evict_from_private_community_voice`]
//! through the [`VoiceEviction`] the comms module attaches in `main`). What
//! did not come along, and why, is listed in `README.md`'s deviations; in
//! short: the archipelago presence probe, which is configured explicitly and
//! degrades to "everyone offline".
//!
//! Mount it twice: at the root (the old `social-api.decentraland.org` host
//! paths) and nested under `/social-api` (the gateway prefix); the
//! signed-fetch check accepts a signature over either path.

pub mod auth;
pub mod copies;
pub mod cors;
pub mod dto;
pub mod fed;
pub mod friendship;
pub mod http;
pub mod mirror;
pub mod presence;
pub mod profiles;
pub mod queries;
pub mod standing;
pub mod thumbnail;
pub mod validate;

pub mod admin;
#[allow(
    clippy::module_inception,
    reason = "named after v1 rest/handlers/communities.rs, the file its `from:` header maps"
)]
mod communities;
mod friends;
mod members;
mod mutes;
mod places;
mod posts;
mod referral;
mod requests;
mod voice;
mod write_support;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, OriginalUri};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use sqlx::PgPool;

use crate::auth::eip1654::Eip1654Validator;
use crate::comms::limits::RateLimiter;
use crate::storage::Storage;
use crate::{Address, CommunityId};

use auth::SignedPath;
use http::CommError;
use presence::Presence;
use profiles::ProfilesCache;

/// The old `catalyrst-communities` service's settings. `main.rs` fills it
/// from `Config`; every field maps to one env name (in the doc of each).
#[derive(Clone, Debug)]
pub struct Settings {
    /// `COMMUNITIES_ADMIN_TOKEN` (the old service's `API_ADMIN_TOKEN`):
    /// bearer of the admin listing/suspend routes, the managed-communities
    /// and member-communities-by-ids lookups and the privacy bypass of the
    /// members listing. Unset refuses them all.
    pub admin_token: Option<String>,
    /// `COMMUNITIES_GLOBAL_MODERATORS`: comma-separated wallets allowed on
    /// `GET /v1/moderation/communities`.
    pub global_moderators: Vec<Address>,
    /// `RESTRICTED_NAMES`: comma-separated community names refused on
    /// create/rename, compared through `validate::fold_name_for_comparison`
    /// (stored here already folded).
    pub restricted_names: Vec<String>,
    /// `COMMUNITIES_CDN_URL` (the old `CDN_URL`), default `PUBLIC_URL`: the
    /// base of `thumbnailUrl`; this node serves the thumbnail at
    /// `/social/communities/{id}/raw-thumbnail.png` under it.
    pub cdn_url: String,
    /// Where profile pictures are served (`profiles::FaceUrls`).
    pub faces: profiles::FaceUrls,
    /// `ARCHIPELAGO_STATS_URL`: the comms presence source of `onlyOnline`;
    /// unset renders every member offline.
    pub archipelago_stats_url: Option<String>,
    /// `COMMUNITIES_PLACES_OWNERSHIP_CHECK` (default false; the old service
    /// checked only when `PLACES_API_URL` was set): when true, places added
    /// to a community must be owned by the caller according to the mirrored
    /// places catalogue (`explore.place_indexed`, a module read instead of
    /// the old HTTP hop to the places API).
    pub places_ownership_check: bool,
    /// `COMMUNITIES_MIRROR_INTERVAL_SECS` (3600): a copy is stale after
    /// twice it (`as_of::stale_after`).
    pub mirror_interval: std::time::Duration,
}

impl Settings {
    /// Folds the restricted names the way the comparison does.
    pub fn restricted_names_from(raw: &str) -> Vec<String> {
        raw.split(',')
            .map(validate::fold_name_for_comparison)
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Lowercases and validates the moderator list; a malformed entry is
    /// dropped with a warning rather than silently matching nothing.
    pub fn global_moderators_from(raw: &str) -> Vec<Address> {
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter_map(|s| match Address::parse(s) {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!(entry = %s, error = %e, "COMMUNITIES_GLOBAL_MODERATORS: dropping a malformed address");
                    None
                }
            })
            .collect()
    }

    pub fn for_tests() -> Self {
        Self {
            admin_token: None,
            global_moderators: Vec::new(),
            restricted_names: Vec::new(),
            cdn_url: "http://cdn.test".to_string(),
            faces: profiles::FaceUrls::new(None, "http://127.0.0.1:9/profile-images/", false),
            archipelago_stats_url: None,
            places_ownership_check: false,
            mirror_interval: std::time::Duration::from_secs(3600),
        }
    }
}

pub struct State {
    pub pool: PgPool,
    pub storage: Arc<Storage>,
    pub settings: Settings,
    /// Contract-wallet (EIP-1654) signature check for signed-fetch chains;
    /// `None` reads such chains as unverifiable.
    pub eip1654: Option<Arc<dyn Eip1654Validator>>,
    pub profiles: ProfilesCache,
    pub presence: Presence,
    /// `POST /v1/friends/{peer}/messages`: 60 sends per signer per minute,
    /// the old service's `RateLimiter::new(60, 60s)`; past it the route
    /// answers 429 before touching the database.
    pub dm_limiter: RateLimiter,
    /// The comms module's voice-room kick, attached once comms is up.
    pub voice: std::sync::OnceLock<Arc<dyn VoiceEviction>>,
    /// The worlds module's re-admission on a membership exit, attached in `serve`.
    pub worlds: std::sync::OnceLock<Arc<dyn WorldAccessRevocation>>,
    /// The communities mirror's upstream, set when its job is attached: the
    /// listing then answers that mirror's copy ([`copies`]).
    pub mirror_source: std::sync::OnceLock<String>,
    pub pass_markers: copies::Markers,
    pub sources: copies::Sources,
    /// The node's ONE explore state (`serve` builds it on the same pool): the
    /// community places read its destinations in process.
    pub explore: crate::explore::SharedState,
}

/// Removes a wallet from a community's voice room (the old gatekeeper's
/// `DELETE /community-voice-chat/{id}/users/{address}`).
pub trait VoiceEviction: Send + Sync {
    fn evict<'a>(
        &'a self,
        community_id: CommunityId,
        target: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<(), String>>;
}

/// The gatekeeper's eviction (upstream #482): the same removal as its
/// `DELETE /community-voice-chat/{id}/users/{address}`.
impl VoiceEviction for crate::comms::gatekeeper::CommsState {
    fn evict<'a>(
        &'a self,
        community_id: CommunityId,
        target: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if !self.livekit.configured {
                return Err("LiveKit is not configured".to_string());
            }
            crate::comms::gatekeeper::voice::kick_from_community_room(
                self,
                community_id,
                &target.to_lowercase(),
            )
            .await
            .map_err(|e| format!("{e:?}"))
        })
    }
}

/// Re-decides world admission when a community membership ends: worlds
/// whose allow-list names the community kick wallets it no longer admits.
/// Both return at once; the re-decision runs detached from the caller.
pub trait WorldAccessRevocation: Send + Sync {
    fn member_removed(&self, community_id: CommunityId, member: &Address);
    /// Deleted, suspended or deactivated: it admits nobody.
    fn community_closed(&self, community_id: CommunityId);
}

pub type SharedState = Arc<State>;

const DM_SENDS_PER_MINUTE: u32 = 60;

impl State {
    /// `http` is the process's outbound client; the presence probe uses its
    /// raw form because `ARCHIPELAGO_STATS_URL` is operator configuration
    /// (typically a loopback service), not user input.
    pub fn new(
        pool: PgPool,
        storage: Arc<Storage>,
        settings: Settings,
        eip1654: Option<Arc<dyn Eip1654Validator>>,
        http: &crate::http::Outbound,
        explore: crate::explore::SharedState,
    ) -> Self {
        let profiles = ProfilesCache::new(pool.clone(), settings.faces.clone());
        let presence = Presence::new(http.raw().clone(), settings.archipelago_stats_url.clone());
        let dm_limiter = RateLimiter::new(DM_SENDS_PER_MINUTE, std::time::Duration::from_secs(60));
        Self {
            pool,
            storage,
            settings,
            eip1654,
            profiles,
            presence,
            dm_limiter,
            voice: std::sync::OnceLock::new(),
            worlds: std::sync::OnceLock::new(),
            mirror_source: std::sync::OnceLock::new(),
            pass_markers: copies::Markers::new(copies::MARKER_TTL),
            sources: copies::Sources::bounded(copies::MARKER_TTL, copies::SOURCES_CAP),
            explore,
        }
    }

    /// An explore state on `pool` for the tests' communities state: its
    /// catalogue reads only, never `spawn_jobs`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn explore_for_tests(pool: &PgPool) -> crate::explore::SharedState {
        Arc::new(crate::explore::ExploreState::new(
            pool.clone(),
            crate::explore::Settings::for_tests(),
            None,
        ))
    }

    /// Records the communities mirror's upstream (once).
    pub fn set_mirror_source(&self, base: &str) {
        let _ = self.mirror_source.set(base.to_string());
    }

    pub async fn require_signer(
        &self,
        headers: &HeaderMap,
        method: &str,
        path: &SignedPath,
    ) -> Result<Address, CommError> {
        Ok(auth::require_signer(headers, method, path, self.eip1654.as_deref()).await?)
    }

    pub async fn optional_signer(
        &self,
        headers: &HeaderMap,
        method: &str,
        path: &SignedPath,
    ) -> Option<Address> {
        auth::try_extract_signer(headers, method, path, self.eip1654.as_deref()).await
    }

    pub fn admin_bearer(&self, headers: &HeaderMap) -> bool {
        auth::admin_bearer_matches(self.settings.admin_token.as_deref(), headers)
    }

    pub fn is_global_moderator(&self, address: &Address) -> bool {
        self.settings.global_moderators.iter().any(|m| m == address)
    }

    pub fn thumbnail_url(&self, id: CommunityId) -> String {
        thumbnail_url(&self.settings.cdn_url, &id.to_string())
    }

    /// Attaches the comms module's voice-room kick (once).
    pub fn set_voice_eviction(&self, voice: Arc<dyn VoiceEviction>) {
        let _ = self.voice.set(voice);
    }

    /// Attaches the worlds module's re-admission (once).
    pub fn set_world_access_revocation(&self, worlds: Arc<dyn WorldAccessRevocation>) {
        let _ = self.worlds.set(worlds);
    }

    /// A wallet left, was kicked or was banned: worlds re-decide its admission.
    pub fn member_removed_from_worlds(&self, community_id: CommunityId, member: &Address) {
        if let Some(worlds) = self.worlds.get() {
            worlds.member_removed(community_id, member);
        }
    }

    /// A deleted, suspended or deactivated community stops admitting anyone
    /// to the worlds listing it.
    pub fn community_closed_to_worlds(&self, community_id: CommunityId) {
        if let Some(worlds) = self.worlds.get() {
            worlds.community_closed(community_id);
        }
    }

    /// Upstream #482's ban-side half: a ban or kick landing after the join
    /// must find the seat in the private community's voice room and remove
    /// it. As the old service did, a failure is logged and never fails the
    /// request that banned or kicked.
    pub async fn evict_from_private_community_voice(
        &self,
        community_id: CommunityId,
        target: &Address,
    ) {
        match queries::is_private(&self.pool, community_id).await {
            Ok(true) => match self.voice.get() {
                Some(voice) => match voice.evict(community_id, target.as_str()).await {
                    Ok(()) => {
                        tracing::info!(%community_id, %target, "kicked user from private community voice chat")
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %community_id, %target, "failed to kick user from community voice chat")
                    }
                },
                None => {
                    tracing::warn!(%community_id, %target, room = %crate::comms::rooms::community_voice_chat_room_name(&community_id), "no voice transport attached; the private community voice seat stays")
                }
            },
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(error = %e, %community_id, %target, "privacy lookup for voice eviction failed; skipping kick")
            }
        }
    }

    /// The old `PLACES_API_URL` `/api/destinations` ownership check as a
    /// read of the mirrored catalogue: every distinct id (a place uuid or a
    /// world name) must resolve to a place whose owner is the caller.
    pub async fn validate_places_ownership(
        &self,
        place_ids: &[String],
        signer: &Address,
    ) -> Result<(), CommError> {
        if place_ids.is_empty() || !self.settings.places_ownership_check {
            return Ok(());
        }
        let mut unique: Vec<String> = Vec::new();
        for id in place_ids {
            if !unique.contains(id) {
                unique.push(id.clone());
            }
        }
        let lowered: Vec<String> = unique.iter().map(|s| s.to_lowercase()).collect();
        let owned: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT p.key) FROM ( \
               SELECT CASE WHEN lower(id) = ANY($1) THEN lower(id) ELSE lower(world_name) END AS key, \
                      lower(COALESCE(raw->>'owner', creator_address)) AS owner \
               FROM explore.place_indexed \
               WHERE lower(id) = ANY($1) OR lower(world_name) = ANY($1) \
             ) p WHERE p.owner = $2",
        )
        .bind(&lowered)
        .bind(signer)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "places ownership validation failed");
            CommError::status(axum::http::StatusCode::BAD_GATEWAY, "failed to validate place ownership")
        })?;
        if owned as usize != unique.len() {
            return Err(CommError::not_authorized(format!(
                "The user {} doesn't own all the places",
                signer
            )));
        }
        Ok(())
    }
}

pub fn thumbnail_url(cdn: &str, id: &str) -> String {
    format!(
        "{}/social/communities/{}/raw-thumbnail.png",
        cdn.trim_end_matches('/'),
        id
    )
}

/// The request's own path (the gateway prefix included when nested) beside
/// the route-local one the old service verified against.
pub(crate) fn signed_path(original: &OriginalUri, local: impl Into<String>) -> SignedPath {
    SignedPath::new(original.0.path().to_string(), local)
}

/// Production's probes under the gateway prefix (`createStatusCheckComponent`
/// in `src/components.ts` at `fb3dcb8`; captures `social/health-*`):
/// `/health/live` answers `alive`, the other two the status checks' pass
/// document. Nested under `/social-api` only: the root `/health/*` belong to
/// the node.
pub fn health_router() -> Router {
    const PASS: &str = r#"{"details":{"httpServer":{"status":"pass"}},"status":"pass"}"#;
    let pass = || async {
        (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/health+json; charset=utf-8",
            )],
            PASS,
        )
    };
    Router::new()
        .route(
            "/health/live",
            get(|| async {
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/plain; charset=utf-8",
                    )],
                    "alive",
                )
            }),
        )
        .route("/health/ready", get(pass))
        .route("/health/startup", get(pass))
        .layer(axum::middleware::from_fn(cors::social_api_cors))
}

/// The route table of the old `catalyrst-communities` service (its
/// `openapi.rs`); an envelope body on a write route takes the federated
/// branch (`fed::envelopes`). Mount with
/// `.merge(router(state.clone())).nest("/social-api", router(state))`.
pub fn router(state: SharedState) -> Router {
    Router::new()
        .route(
            "/v1/communities",
            get(communities::get_communities).post(communities::create_community),
        )
        .route("/v2/communities", get(communities::get_communities_v2))
        .route(
            "/social/communities/{id}/raw-thumbnail.png",
            get(communities::get_raw_thumbnail),
        )
        .route(
            "/v1/communities/{id}",
            get(communities::get_community)
                .put(communities::update_community)
                .patch(communities::update_community_partially)
                .delete(communities::delete_community),
        )
        .route("/v2/communities/{id}", get(communities::get_community_v2))
        .route(
            "/v1/communities/{id}/members",
            get(members::get_members).post(members::add_member),
        )
        .route("/v2/communities/{id}/members", get(members::get_members_v2))
        .route(
            "/v1/communities/{id}/members/{address}",
            axum::routing::delete(members::remove_member).patch(members::update_member_role),
        )
        .route(
            "/v1/communities/{id}/members/{address}/bans",
            post(members::ban_member).delete(members::unban_member),
        )
        .route("/v1/communities/{id}/bans", get(members::get_bans))
        .route("/v2/communities/{id}/bans", get(members::get_bans_v2))
        .route(
            "/v1/communities/{id}/places",
            get(places::get_places).post(places::add_places),
        )
        .route(
            "/v1/communities/{id}/places/{placeId}",
            axum::routing::delete(places::remove_place),
        )
        .route(
            "/v1/communities/{id}/posts",
            get(posts::get_posts).post(posts::create_post),
        )
        .route("/v2/communities/{id}/posts", get(posts::get_posts_v2))
        .route(
            "/v1/communities/{id}/posts/{postId}",
            axum::routing::delete(posts::delete_post).patch(posts::edit_post),
        )
        .route(
            "/v1/communities/{id}/posts/{postId}/like",
            post(posts::like_post).delete(posts::unlike_post),
        )
        .route(
            "/v1/communities/{id}/requests",
            get(requests::get_community_requests).post(requests::create_request),
        )
        .route(
            "/v2/communities/{id}/requests",
            get(requests::get_community_requests_v2),
        )
        .route(
            "/v1/communities/{id}/requests/{requestId}",
            axum::routing::patch(requests::update_request_status),
        )
        .route(
            "/v1/communities/{id}/managed",
            get(members::get_managed_communities),
        )
        .route(
            "/v1/members/{address}/communities",
            get(members::get_member_communities).post(members::member_communities_by_ids),
        )
        .route(
            "/v1/members/{address}/requests",
            get(requests::get_member_requests),
        )
        .route(
            "/v2/members/{address}/requests",
            get(requests::get_member_requests_v2),
        )
        .route("/v1/members/{address}/invites", get(requests::get_invites))
        .route(
            "/v1/community-voice-chats/active",
            get(voice::get_active_voice_chats),
        )
        .route(
            "/v1/referral-progress",
            get(referral::get_referral_progress)
                .post(referral::create_referral)
                .patch(referral::update_referral_signed_up),
        )
        .route(
            "/v1/moderation/communities",
            get(admin::get_moderation_communities),
        )
        .route("/v1/admin/communities", get(admin::list_communities))
        .route(
            "/v1/admin/communities/{id}/suspend",
            post(admin::suspend_community),
        )
        .route(
            "/v1/admin/communities/{id}/unsuspend",
            post(admin::unsuspend_community),
        )
        .route(
            "/v1/mutes",
            get(mutes::get_mutes)
                .post(mutes::add_mute)
                .delete(mutes::remove_mute),
        )
        .route("/v1/friends", get(friends::list_friends))
        .route(
            "/v1/friends/{peer}/messages",
            get(friends::get_messages).post(friends::send_message),
        )
        .route("/federation/communities/snapshot", get(fed::snapshot))
        .route("/federation/communities/changes", get(fed::changes))
        .route("/federation/communities/content", post(fed::put_content))
        .route("/federation/communities/content/gc", post(fed::gc_content))
        .route(
            "/federation/communities/content/{hash}",
            get(fed::get_content),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            fed::envelopes,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            mirror::refuse_mirrored_writes,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            copies::gate,
        ))
        .layer(DefaultBodyLimit::max(512 * 1024))
        .layer(axum::middleware::from_fn(cors::social_api_cors))
        .with_state(state)
}
