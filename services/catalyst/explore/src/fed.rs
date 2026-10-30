//! Signed actions of the explore domain (`catalyst_fed`): the places'
//! favorite, vote and report envelopes and the events' schedule and
//! profile-settings envelopes, each recorded once in
//! `explore.signed_actions` (keyed by its digest, numbered by `seq`) in the
//! transaction that applies it, and the feeds peers pull:
//! `/federation/places/{snapshot,changes}` over that log, and the events
//! feed and attendance, which the old service answered empty.
//!
//! An envelope is refused unless its recovered signer matches the request's
//! auth chain (when one is presented), its domain is this scope's, it is
//! inside the skew window and its nonce is new for the signer
//! (`explore.seen_nonces`, swept by `explore-nonce-sweep`).

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Row};

use super::SharedState;
use crate::shared::errors::ApiError;
use catalyst_fed::{Eip712Domain, Signed, TypedMessage};

pub const NONCES: &str = "explore.seen_nonces";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Places,
    Events,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Places => "places",
            Scope::Events => "events",
        }
    }

    pub fn domain(self) -> Eip712Domain {
        match self {
            Scope::Places => catalyst_fed::domains::places(),
            Scope::Events => catalyst_fed::domains::events(),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct SignedApiData<T> {
    pub ok: bool,
    pub signature_hash: String,
    pub data: T,
}

impl<T: Serialize> SignedApiData<T> {
    pub fn ok(signature_hash: String, data: T) -> Self {
        Self {
            ok: true,
            signature_hash,
            data,
        }
    }
}

/// Verifies an envelope for `scope` and records its nonce: the old
/// preflight's checks and answers, in its order.
pub async fn preflight<T: TypedMessage + DeserializeOwned>(
    state: &SharedState,
    headers: &HeaderMap,
    body: Value,
    scope: Scope,
) -> Result<(Signed<T>, String), ApiError> {
    let signed: Signed<T> = serde_json::from_value(body)
        .map_err(|e| ApiError::bad_request(format!("invalid Signed<{}>: {e}", T::PRIMARY_TYPE)))?;
    let signer = signed
        .signer()
        .map_err(|e| ApiError::unauthorized(format!("signature verify: {e}")))?;
    if let Some(addr) = crate::shared::signed_fetch::auth_chain_claimed_address(headers)
        && !addr.eq_ignore_ascii_case(&signer)
    {
        return Err(ApiError::unauthorized(
            "auth-chain signer != envelope signer",
        ));
    }
    signed
        .verify(&signer, chrono::Utc::now().timestamp())
        .map_err(|e| ApiError::unauthorized(format!("signature verify: {e}")))?;
    let domain = scope.domain();
    if !signed.domain.name.eq_ignore_ascii_case(&domain.name) {
        return Err(ApiError::bad_request(format!(
            "domain mismatch: expected {}",
            domain.name
        )));
    }
    catalyst_fed::check_and_record(
        &state.pool,
        NONCES,
        &signer,
        &signed.nonce,
        signed.signed_at,
    )
    .await
    .map_err(|e| ApiError::bad_request(format!("replay: {e}")))?;
    Ok((signed, signer.to_lowercase()))
}

pub struct Action<'a> {
    pub scope: Scope,
    pub signature_hash: &'a str,
    pub signer: &'a str,
    pub subject_id: Option<&'a str>,
    pub action_type: &'a str,
    pub payload: &'a Value,
    pub signed_at: i64,
}

/// Appends the action to the log; `false` when its digest was already there.
pub async fn record_action(conn: &mut PgConnection, action: &Action<'_>) -> Result<bool, ApiError> {
    let res = sqlx::query(
        "INSERT INTO explore.signed_actions (signature_hash, scope, signer, subject_id, action_type, message_payload, signed_at, received_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (signature_hash) DO NOTHING",
    )
    .bind(action.signature_hash)
    .bind(action.scope.as_str())
    .bind(action.signer.to_lowercase())
    .bind(action.subject_id)
    .bind(action.action_type)
    .bind(action.payload)
    .bind(action.signed_at)
    .bind(chrono::Utc::now().timestamp())
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceFavorite {
    pub place_id: String,
    pub action: PlaceFavoriteAction,
    pub signed_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlaceFavoriteAction {
    Add,
    Remove,
}

impl TypedMessage for PlaceFavorite {
    const PRIMARY_TYPE: &'static str = "PlaceFavorite";
    fn encode_struct(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.place_id.len() + 1 + 8);
        out.extend_from_slice(Self::PRIMARY_TYPE.as_bytes());
        out.push(0);
        out.extend_from_slice(self.place_id.as_bytes());
        out.push(match self.action {
            PlaceFavoriteAction::Add => 1,
            PlaceFavoriteAction::Remove => 0,
        });
        out.extend_from_slice(&self.signed_at.to_be_bytes());
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceVote {
    pub place_id: String,
    pub score: i8,
    pub signed_at: i64,
}

impl TypedMessage for PlaceVote {
    const PRIMARY_TYPE: &'static str = "PlaceVote";
    fn encode_struct(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.place_id.len() + 1 + 8);
        out.extend_from_slice(Self::PRIMARY_TYPE.as_bytes());
        out.push(0);
        out.extend_from_slice(self.place_id.as_bytes());
        out.push(self.score as u8);
        out.extend_from_slice(&self.signed_at.to_be_bytes());
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceReport {
    pub place_id: String,
    pub reason: PlaceReportReason,
    pub signed_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlaceReportReason {
    Spam,
    Harassment,
    Nsfw,
    Other,
}

impl TypedMessage for PlaceReport {
    const PRIMARY_TYPE: &'static str = "PlaceReport";
    fn encode_struct(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.place_id.len() + 1 + 8);
        out.extend_from_slice(Self::PRIMARY_TYPE.as_bytes());
        out.push(0);
        out.extend_from_slice(self.place_id.as_bytes());
        out.push(match self.reason {
            PlaceReportReason::Spam => 1,
            PlaceReportReason::Harassment => 2,
            PlaceReportReason::Nsfw => 3,
            PlaceReportReason::Other => 4,
        });
        out.extend_from_slice(&self.signed_at.to_be_bytes());
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSettingsUpdate {
    pub target: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: Option<bool>,
    #[serde(default)]
    pub use_local_time: Option<bool>,
    #[serde(default)]
    pub notify_by_email: Option<bool>,
    #[serde(default)]
    pub notify_by_browser: Option<bool>,
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
    pub signed_at: i64,
}

impl TypedMessage for ProfileSettingsUpdate {
    const PRIMARY_TYPE: &'static str = "ProfileSettingsUpdate";
    fn encode_struct(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.target.len() + 64);
        out.extend_from_slice(Self::PRIMARY_TYPE.as_bytes());
        out.push(0);
        out.extend_from_slice(self.target.as_bytes());
        if let Some(e) = &self.email {
            out.extend_from_slice(e.as_bytes());
        }
        out.push(self.email_verified.unwrap_or(false) as u8);
        out.push(self.use_local_time.unwrap_or(false) as u8);
        out.push(self.notify_by_email.unwrap_or(false) as u8);
        out.push(self.notify_by_browser.unwrap_or(false) as u8);
        for p in self.permissions.iter().flatten() {
            out.extend_from_slice(p.as_bytes());
            out.push(0);
        }
        out.extend_from_slice(&self.signed_at.to_be_bytes());
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleUpsert {
    #[serde(default)]
    pub schedule_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub theme: Option<String>,
    #[serde(default)]
    pub background: Vec<String>,
    pub active_since: i64,
    pub active_until: i64,
    #[serde(default = "default_true")]
    pub active: bool,
    pub signed_at: i64,
}

fn default_true() -> bool {
    true
}

impl TypedMessage for ScheduleUpsert {
    const PRIMARY_TYPE: &'static str = "ScheduleUpsert";
    fn encode_struct(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.name.len() + 64);
        out.extend_from_slice(Self::PRIMARY_TYPE.as_bytes());
        out.push(0);
        if let Some(id) = &self.schedule_id {
            out.extend_from_slice(id.as_bytes());
        }
        out.extend_from_slice(self.name.as_bytes());
        for field in [&self.description, &self.image, &self.theme]
            .into_iter()
            .flatten()
        {
            out.extend_from_slice(field.as_bytes());
        }
        for c in &self.background {
            out.extend_from_slice(c.as_bytes());
            out.push(0);
        }
        out.extend_from_slice(&self.active_since.to_be_bytes());
        out.extend_from_slice(&self.active_until.to_be_bytes());
        out.push(self.active as u8);
        out.extend_from_slice(&self.signed_at.to_be_bytes());
        out
    }
}

/// A schedule created without an id takes this one: stable for the signer,
/// the name and the nonce.
pub fn schedule_id(signer: &str, name: &str, nonce: &[u8; 16]) -> String {
    let mut h = Sha256::new();
    h.update(signer.to_ascii_lowercase().as_bytes());
    h.update([0u8]);
    h.update(name.as_bytes());
    h.update([0u8]);
    h.update(nonce);
    hex::encode(h.finalize())
}

const DEFAULT_LIMIT: i64 = 500;
const MAX_LIMIT: i64 = 5000;

fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

#[derive(Debug, Deserialize)]
pub struct ChangesQuery {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /federation/places/snapshot`: the log's watermark, its size by
/// action type, a digest of every action (sha256 of the ordered digests,
/// one per line) and the current fact counts.
pub async fn places_snapshot(State(state): State<SharedState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(snapshot_view(&state.pool).await?))
}

pub async fn snapshot_view(pool: &PgPool) -> Result<Value, ApiError> {
    let latest_seq: i64 = sqlx::query_scalar(
        "SELECT COALESCE(max(seq), 0) FROM explore.signed_actions WHERE scope = 'places'",
    )
    .fetch_one(pool)
    .await?;
    let by_type: Vec<(String, i64)> =
        sqlx::query_as("SELECT action_type, count(*) FROM explore.signed_actions WHERE scope = 'places' GROUP BY action_type ORDER BY action_type").fetch_all(pool).await?;
    let action_count: i64 = by_type.iter().map(|(_, c)| c).sum();
    let actions_by_type: serde_json::Map<String, Value> =
        by_type.into_iter().map(|(t, c)| (t, json!(c))).collect();
    let hashes: Vec<String> =
        sqlx::query_scalar("SELECT signature_hash FROM explore.signed_actions WHERE scope = 'places' ORDER BY signature_hash").fetch_all(pool).await?;
    let mut h = Sha256::new();
    for s in &hashes {
        h.update(s.as_bytes());
        h.update(b"\n");
    }
    let (favorites, votes, reports): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM explore.user_favorites), (SELECT count(*) FROM explore.user_likes), (SELECT count(*) FROM explore.place_reports)",
    )
    .fetch_one(pool)
    .await?;
    Ok(json!({
        "scope": "places",
        "latest_seq": latest_seq,
        "action_count": action_count,
        "actions_by_type": actions_by_type,
        "log_hash": hex::encode(h.finalize()),
        "current": { "favorites": favorites, "votes": votes, "reports": reports },
    }))
}

/// `GET /federation/places/changes?since=&limit=`: the actions after `since`, oldest first.
pub async fn places_changes(
    State(state): State<SharedState>,
    Query(q): Query<ChangesQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        changes_view(&state.pool, q.since, clamp_limit(q.limit)).await?,
    ))
}

pub async fn changes_view(pool: &PgPool, since: i64, limit: i64) -> Result<Value, ApiError> {
    let rows = sqlx::query(
        "SELECT seq, signature_hash, signer, subject_id, action_type, message_payload, signed_at, origin_peer
         FROM explore.signed_actions WHERE scope = 'places' AND seq > $1 ORDER BY seq ASC LIMIT $2",
    )
    .bind(since)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut max_seq = since;
    let actions: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let seq: i64 = r.get("seq");
            max_seq = max_seq.max(seq);
            json!({
                "seq": seq,
                "signature_hash": r.get::<String, _>("signature_hash"),
                "signer": r.get::<String, _>("signer"),
                "place_id": r.get::<Option<String>, _>("subject_id").unwrap_or_default(),
                "action_type": r.get::<String, _>("action_type"),
                "payload": r.get::<Value, _>("message_payload"),
                "signed_at": r.get::<i64, _>("signed_at"),
                "origin_peer": r.get::<Option<String>, _>("origin_peer"),
            })
        })
        .collect();
    Ok(json!({ "since": since, "limit": limit, "latest_seq": max_seq, "actions": actions }))
}

#[derive(Debug, Deserialize)]
pub struct FeedQuery {
    pub since: Option<i64>,
    pub limit: Option<i64>,
}

/// `GET /federation/v1/events/feed`: the old service answered it empty.
pub async fn events_feed(Query(_q): Query<FeedQuery>) -> Json<Value> {
    Json(json!({ "events": [], "partial": false }))
}

/// `GET /federation/v1/events/{event_id}/attendance`: likewise.
pub async fn events_attendance(
    Path(event_id): Path<String>,
    Query(_q): Query<FeedQuery>,
) -> Json<Value> {
    Json(json!({ "event_id": event_id, "attendances": [], "partial": false }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use alloy::signers::local::PrivateKeySigner;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::{ExploreState, Settings, router};
    use catalyst_fed::test_support::sign;

    async fn call(
        app: &axum::Router,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let req = Request::builder().method(method).uri(path);
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string()))
                .unwrap(),
            None => req.body(Body::empty()).unwrap(),
        };
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// Envelopes: a favorite and a vote logged once and applied, a replayed
    /// nonce refused, a foreign domain refused; a moderator's schedule and a
    /// wallet's own settings; the places feeds page the log.
    #[tokio::test]
    async fn envelopes_are_logged_once_applied_and_fed() {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let place = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO explore.place (id, base_position, title, favorites, raw) VALUES ($1, '4,4', 'p', 2, '{}')").bind(&place).execute(&pool).await.unwrap();
        let key = PrivateKeySigner::random();
        let wallet = format!("{:#x}", key.address());
        let settings = Settings {
            user_writes_enabled: true,
            operator_writes_enabled: true,
            ..Settings::for_tests()
        };
        let app = router(Arc::new(ExploreState::new(pool.clone(), settings, None)));
        let (_, before) = call(&app, "GET", "/federation/places/snapshot", None).await;
        let since = before["latest_seq"].as_i64().unwrap();

        let fav = sign(
            &key,
            catalyst_fed::domains::places(),
            PlaceFavorite {
                place_id: place.clone(),
                action: PlaceFavoriteAction::Add,
                signed_at: now(),
            },
            now(),
        );
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/places/{place}/favorites"),
            Some(serde_json::to_value(&fav).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            (body["signature_hash"].clone(), body["data"].clone()),
            (
                json!(fav.signature_hash()),
                json!({ "favorites": 3, "user_favorite": true })
            )
        );
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/places/{place}/favorites"),
            Some(serde_json::to_value(&fav).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "a replayed nonce: {body}");
        assert!(body["message"].as_str().unwrap().starts_with("replay:"));

        let vote = sign(
            &key,
            catalyst_fed::domains::places(),
            PlaceVote {
                place_id: place.clone(),
                score: -1,
                signed_at: now(),
            },
            now(),
        );
        let (_, body) = call(
            &app,
            "PATCH",
            &format!("/api/places/{place}/likes"),
            Some(serde_json::to_value(&vote).unwrap()),
        )
        .await;
        assert_eq!(body["data"]["user_dislike"], json!(true), "{body}");

        let foreign = sign(
            &key,
            catalyst_fed::domains::events(),
            PlaceVote {
                place_id: place.clone(),
                score: 1,
                signed_at: now(),
            },
            now(),
        );
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/places/{place}/likes"),
            Some(serde_json::to_value(&foreign).unwrap()),
        )
        .await;
        assert_eq!(
            (status, body["message"].clone()),
            (
                StatusCode::BAD_REQUEST,
                json!("domain mismatch: expected DecentralandPlaces")
            )
        );
        let stale = sign(
            &key,
            catalyst_fed::domains::places(),
            PlaceVote {
                place_id: place.clone(),
                score: 1,
                signed_at: now() - 3600,
            },
            now() - 3600,
        );
        let (status, _) = call(
            &app,
            "PATCH",
            &format!("/api/places/{place}/likes"),
            Some(serde_json::to_value(&stale).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "outside the skew window");

        let report = sign(
            &key,
            catalyst_fed::domains::places(),
            PlaceReport {
                place_id: place.clone(),
                reason: PlaceReportReason::Spam,
                signed_at: now(),
            },
            now(),
        );
        let (status, body) = call(
            &app,
            "POST",
            "/api/report",
            Some(serde_json::to_value(&report).unwrap()),
        )
        .await;
        assert_eq!(
            (status, body["data"]["place_id"].clone()),
            (StatusCode::OK, json!(place)),
            "{body}"
        );

        let (_, page) = call(
            &app,
            "GET",
            &format!("/federation/places/changes?since={since}"),
            None,
        )
        .await;
        let types: Vec<&str> = page["actions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["place_id"] == json!(place))
            .map(|a| a["action_type"].as_str().unwrap())
            .collect();
        assert_eq!(types, ["favorite", "vote_down", "report"]);
        let (_, snap) = call(&app, "GET", "/federation/places/snapshot", None).await;
        assert_eq!(snap["latest_seq"], page["latest_seq"]);
        assert!(snap["log_hash"].as_str().unwrap().len() == 64);

        // events: a schedule needs a moderator, settings are the signer's own
        let schedule = sign(
            &key,
            catalyst_fed::domains::events(),
            ScheduleUpsert {
                schedule_id: None,
                name: "MVMF".into(),
                description: None,
                image: None,
                theme: Some("pride".into()),
                background: vec!["#fff".into()],
                active_since: now(),
                active_until: now() + 86_400,
                active: true,
                signed_at: now(),
            },
            now(),
        );
        let (status, _) = call(
            &app,
            "POST",
            "/api/schedules",
            Some(serde_json::to_value(&schedule).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        sqlx::query("INSERT INTO explore.moderators (address, added_at) VALUES ($1, 0)")
            .bind(&wallet)
            .execute(&pool)
            .await
            .unwrap();
        let schedule = sign(
            &key,
            catalyst_fed::domains::events(),
            ScheduleUpsert {
                schedule_id: None,
                name: "MVMF".into(),
                description: None,
                image: None,
                theme: Some("pride".into()),
                background: vec!["#fff".into()],
                active_since: now(),
                active_until: now() + 86_400,
                active: true,
                signed_at: now(),
            },
            now(),
        );
        let (status, body) = call(
            &app,
            "POST",
            "/api/schedules",
            Some(serde_json::to_value(&schedule).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["data"]["id"],
            json!(schedule_id(&wallet, "MVMF", &schedule.nonce))
        );
        let (status, _) = call(
            &app,
            "POST",
            "/api/schedules",
            Some(json!({ "name": "unsigned" })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = call(
            &app,
            "POST",
            "/api/schedules",
            Some(json!({"domain": {}, "message": {}, "signature": "0x"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let mine = sign(
            &key,
            catalyst_fed::domains::events(),
            ProfileSettingsUpdate {
                target: wallet.to_uppercase().replace("0X", "0x"),
                email: Some("a@b.c".into()),
                email_verified: None,
                use_local_time: Some(false),
                notify_by_email: None,
                notify_by_browser: None,
                permissions: None,
                signed_at: now(),
            },
            now(),
        );
        let (status, body) = call(
            &app,
            "PATCH",
            "/api/profiles/me/settings",
            Some(serde_json::to_value(&mine).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            (
                body["data"]["email"].clone(),
                body["data"]["use_local_time"].clone(),
                body["data"]["user"].clone()
            ),
            (json!("a@b.c"), json!(false), json!(wallet))
        );
        let other = sign(
            &key,
            catalyst_fed::domains::events(),
            ProfileSettingsUpdate {
                target: "0x0000000000000000000000000000000000000001".into(),
                email: None,
                email_verified: None,
                use_local_time: None,
                notify_by_email: Some(true),
                notify_by_browser: None,
                permissions: None,
                signed_at: now(),
            },
            now(),
        );
        let (status, _) = call(
            &app,
            "PATCH",
            "/api/profiles/me/settings",
            Some(serde_json::to_value(&other).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let events_actions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM explore.signed_actions WHERE scope = 'events' AND signer = $1",
        )
        .bind(&wallet)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(events_actions, 2);
        let (_, feed) = call(&app, "GET", "/federation/v1/events/feed", None).await;
        assert_eq!(feed, json!({ "events": [], "partial": false }));
    }

    #[tokio::test]
    async fn changing_the_email_requires_verification_again_but_other_edits_preserve_it() {
        let Some(pool) = crate::db::scratch::pool(3).await else {
            return;
        };
        let key = PrivateKeySigner::random();
        let wallet = format!("{:#x}", key.address());
        sqlx::query("INSERT INTO explore.event_profile_settings (\"user\",email,email_verified) VALUES ($1,'verified@example.org',true)")
            .bind(&wallet).execute(&pool).await.unwrap();
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: true,
                ..Settings::for_tests()
            },
            None,
        )));
        for (email, expected) in [
            (None, true),
            (Some("verified@example.org"), true),
            (Some("new@example.org"), false),
        ] {
            let request = sign(
                &key,
                catalyst_fed::domains::events(),
                ProfileSettingsUpdate {
                    target: wallet.clone(),
                    email: email.map(str::to_owned),
                    email_verified: None,
                    use_local_time: Some(false),
                    notify_by_email: None,
                    notify_by_browser: None,
                    permissions: None,
                    signed_at: now(),
                },
                now(),
            );
            let (status, body) = call(
                &app,
                "PATCH",
                "/api/profiles/me/settings",
                Some(serde_json::to_value(request).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["data"]["email_verified"], expected, "{body}");
        }
    }

    /// A moderator's writes (a schedule, another wallet's settings) are the
    /// node's moderation, behind `OPS_WRITES_ENABLED`; a wallet's own
    /// settings are a user write, behind `DATA_WRITES_ENABLED`.
    #[tokio::test]
    async fn moderator_envelopes_ride_the_ops_switch_and_own_settings_the_data_switch() {
        let Some(pool) = crate::db::scratch::pool(2).await else {
            return;
        };
        let key = PrivateKeySigner::random();
        let wallet = format!("{:#x}", key.address());
        sqlx::query("INSERT INTO explore.moderators (address, added_at) VALUES ($1, 0)")
            .bind(&wallet)
            .execute(&pool)
            .await
            .unwrap();
        let app = |data: bool, ops: bool| {
            router(Arc::new(ExploreState::new(
                pool.clone(),
                Settings {
                    user_writes_enabled: data,
                    operator_writes_enabled: ops,
                    ..Settings::for_tests()
                },
                None,
            )))
        };
        let schedule = || {
            serde_json::to_value(sign(
                &key,
                catalyst_fed::domains::events(),
                ScheduleUpsert {
                    schedule_id: None,
                    name: "Gates".into(),
                    description: None,
                    image: None,
                    theme: Some("pride".into()),
                    background: vec!["#fff".into()],
                    active_since: now(),
                    active_until: now() + 86_400,
                    active: true,
                    signed_at: now(),
                },
                now(),
            ))
            .unwrap()
        };
        let settings_of = |target: &str| {
            serde_json::to_value(sign(
                &key,
                catalyst_fed::domains::events(),
                ProfileSettingsUpdate {
                    target: target.to_string(),
                    email: None,
                    email_verified: None,
                    use_local_time: Some(true),
                    notify_by_email: None,
                    notify_by_browser: None,
                    permissions: None,
                    signed_at: now(),
                },
                now(),
            ))
            .unwrap()
        };
        let other = "0x00000000000000000000000000000000000000c3";
        let other_path = format!("/api/profiles/{other}/settings");

        let data_only = app(true, false);
        for (method, path, body) in [
            ("POST", "/api/schedules", schedule()),
            ("PATCH", other_path.as_str(), settings_of(other)),
        ] {
            let (status, body) = call(&data_only, method, path, Some(body)).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {body}");
            assert!(
                body["message"]
                    .as_str()
                    .unwrap()
                    .contains("OPS_WRITES_ENABLED"),
                "{body}"
            );
        }
        let (status, body) = call(
            &data_only,
            "PATCH",
            "/api/profiles/me/settings",
            Some(settings_of(&wallet)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let ops_only = app(false, true);
        let (status, body) = call(&ops_only, "POST", "/api/schedules", Some(schedule())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(&ops_only, "PATCH", &other_path, Some(settings_of(other))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &ops_only,
            "PATCH",
            "/api/profiles/me/settings",
            Some(settings_of(&wallet)),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("DATA_WRITES_ENABLED"),
            "{body}"
        );
    }

    #[test]
    fn limit_clamps_to_bounds() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(-9)), 1);
        assert_eq!(clamp_limit(Some(42)), 42);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    #[test]
    fn schedule_ids_are_stable_per_signer_name_and_nonce() {
        let nonce = [7u8; 16];
        assert_eq!(
            schedule_id("0xABC", "MVMF", &nonce),
            schedule_id("0xabc", "MVMF", &nonce)
        );
        assert_ne!(
            schedule_id("0x1", "A", &nonce),
            schedule_id("0x1", "B", &nonce)
        );
        assert_ne!(
            schedule_id("0x1", "A", &[1; 16]),
            schedule_id("0x1", "A", &[2; 16])
        );
    }

    #[test]
    fn message_encodings_are_stable_and_distinct() {
        let create = ScheduleUpsert {
            schedule_id: None,
            name: "MVMF".into(),
            description: None,
            image: None,
            theme: None,
            background: vec![],
            active_since: 10,
            active_until: 20,
            active: true,
            signed_at: 5,
        };
        let mut update = create.clone();
        update.schedule_id = Some("sched-1".into());
        assert_ne!(create.encode_struct(), update.encode_struct());
        let v: ScheduleUpsert = serde_json::from_value(
            json!({ "name": "x", "active_since": 1, "active_until": 2, "signed_at": 3 }),
        )
        .unwrap();
        assert!(v.active && v.schedule_id.is_none());
        let p: ProfileSettingsUpdate =
            serde_json::from_value(json!({ "target": "0x1", "signed_at": 1 })).unwrap();
        assert!(p.email.is_none() && p.permissions.is_none());
        assert_eq!(
            serde_json::to_value(PlaceReportReason::Nsfw).unwrap(),
            json!("nsfw")
        );
    }
}
