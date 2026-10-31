//! The events' signed-envelope writes and the profile settings reads:
//!
//! - `PATCH /api/profiles/me/settings` (a `ProfileSettingsUpdate` envelope
//!   for the signer itself) and `PATCH /api/profiles/{id}/settings` (a
//!   moderator's for anyone); `GET` of both and the moderators'
//!   `GET /api/profiles/settings` behind a signed fetch;
//! - `POST /api/schedules` and `PATCH /api/schedules/{id}` (a moderator's
//!   `ScheduleUpsert` envelope);
//! - `/api/profiles/subscriptions` answers 410 on every verb: web-push was
//!   retired before the port.
//!
//! A moderator is a wallet in `explore.moderators`. Each envelope is logged
//! in `explore.signed_actions` and applied in one transaction: the signer's
//! own settings behind `DATA_WRITES_ENABLED`, the moderator-only writes (the
//! schedules, another profile's settings) behind `OPS_WRITES_ENABLED`, the
//! switch of the node's moderation.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Method};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool};

use super::ScheduleRecord;
use crate::SharedState;
use crate::fed::{self, ProfileSettingsUpdate, ScheduleUpsert};
use crate::shared::errors::{ApiData, ApiError};

/// Moderators may read everyone's settings; the list is capped (moderator
/// reads of a table with one row per wallet that saved settings).
const SETTINGS_LIST_CAP: i64 = 10_000;

pub async fn is_moderator(pool: &PgPool, address: &str) -> Result<bool, ApiError> {
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM explore.moderators WHERE address = $1")
        .bind(address.to_ascii_lowercase())
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

pub async fn require_moderator(pool: &PgPool, signer: &str) -> Result<(), ApiError> {
    if is_moderator(pool, signer).await? {
        Ok(())
    } else {
        Err(ApiError::forbidden("Forbidden"))
    }
}

fn ok(data: Value) -> Json<Value> {
    Json(json!({ "ok": true, "data": data }))
}

fn envelope(body: Value) -> Result<Value, ApiError> {
    if catalyst_fed::is_envelope(&body) {
        Ok(body)
    } else {
        Err(ApiError::bad_request("missing signed body"))
    }
}

#[derive(sqlx::FromRow)]
struct SettingsRow {
    email: Option<String>,
    email_verified: bool,
    use_local_time: bool,
    notify_by_email: bool,
    notify_by_browser: bool,
    permissions: Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

fn stamp(t: chrono::NaiveDateTime) -> String {
    t.and_utc()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// The wallet's settings, or the defaults for one that never saved any.
pub async fn load_settings(conn: &mut PgConnection, user: &str) -> Result<Value, ApiError> {
    let user = user.to_ascii_lowercase();
    let row: Option<SettingsRow> = sqlx::query_as(
        r#"SELECT email, email_verified, use_local_time, notify_by_email, notify_by_browser, permissions, created_at, updated_at
           FROM explore.event_profile_settings WHERE "user" = $1"#,
    )
    .bind(&user)
    .fetch_optional(conn)
    .await?;
    Ok(match row {
        Some(r) => json!({
            "user": user,
            "email": r.email,
            "email_verified": r.email_verified,
            "use_local_time": r.use_local_time,
            "notify_by_email": r.notify_by_email,
            "notify_by_browser": r.notify_by_browser,
            "permissions": r.permissions,
            "created_at": stamp(r.created_at),
            "updated_at": stamp(r.updated_at),
        }),
        None => json!({
            "user": user,
            "email": null,
            "email_verified": false,
            "use_local_time": true,
            "notify_by_email": false,
            "notify_by_browser": false,
            "permissions": [],
            "created_at": "1970-01-01T00:00:00.000Z",
            "updated_at": "1970-01-01T00:00:00.000Z",
        }),
    })
}

async fn apply_profile_settings(
    pool: &PgPool,
    signed: &catalyst_fed::Signed<ProfileSettingsUpdate>,
    signer: &str,
) -> Result<Value, ApiError> {
    let m = &signed.message;
    let target = m.target.to_ascii_lowercase();
    let hash = signed.signature_hash();
    let payload = serde_json::to_value(m).unwrap_or_else(|_| json!({}));
    let now = chrono::Utc::now().naive_utc();
    let mut tx = pool.begin().await?;
    fed::record_action(
        &mut tx,
        &fed::Action {
            scope: fed::Scope::Events,
            signature_hash: &hash,
            signer,
            subject_id: Some(&target),
            action_type: "ProfileSettingsUpdate",
            payload: &payload,
            signed_at: signed.signed_at,
        },
    )
    .await?;
    sqlx::query(
        r#"INSERT INTO explore.event_profile_settings
               ("user", email, email_verified, use_local_time, notify_by_email, notify_by_browser, permissions, created_at, updated_at)
           VALUES ($1, $2, COALESCE($3, false), COALESCE($4, true), COALESCE($5, false), COALESCE($6, false), COALESCE($7, '[]'::jsonb), $8, $8)
           ON CONFLICT ("user") DO UPDATE SET
               email = COALESCE($2, explore.event_profile_settings.email),
               email_verified = CASE WHEN $2 IS NOT NULL AND explore.event_profile_settings.email IS DISTINCT FROM $2
                   THEN COALESCE($3, false) ELSE COALESCE($3, explore.event_profile_settings.email_verified) END,
               use_local_time = COALESCE($4, explore.event_profile_settings.use_local_time),
               notify_by_email = COALESCE($5, explore.event_profile_settings.notify_by_email),
               notify_by_browser = COALESCE($6, explore.event_profile_settings.notify_by_browser),
               permissions = COALESCE($7, explore.event_profile_settings.permissions),
               updated_at = $8"#,
    )
    .bind(&target)
    .bind(m.email.as_deref())
    .bind(m.email_verified)
    .bind(m.use_local_time)
    .bind(m.notify_by_email)
    .bind(m.notify_by_browser)
    .bind(m.permissions.as_ref().map(|p| json!(p)))
    .bind(now)
    .execute(&mut *tx)
    .await?;
    let settings = load_settings(&mut tx, &target).await?;
    tx.commit().await?;
    Ok(settings)
}

/// `GET /api/profiles/settings`: every saved profile, for a moderator.
pub async fn list_profile_settings(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = state.signer(&headers, method.as_str(), uri.path()).await?;
    require_moderator(&state.pool, &user).await?;
    let users: Vec<String> = sqlx::query_scalar(
        r#"SELECT "user" FROM explore.event_profile_settings ORDER BY "user" ASC LIMIT $1"#,
    )
    .bind(SETTINGS_LIST_CAP)
    .fetch_all(&state.pool)
    .await?;
    let mut conn = state.pool.acquire().await?;
    let mut out = Vec::with_capacity(users.len());
    for user in users {
        out.push(load_settings(&mut conn, &user).await?);
    }
    Ok(ok(json!(out)))
}

/// `GET /api/profiles/me/settings`: the signer's own, with the retired `subscriptions` empty.
pub async fn get_my_profile_settings(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = state.signer(&headers, method.as_str(), uri.path()).await?;
    let mut conn = state.pool.acquire().await?;
    let mut settings = load_settings(&mut conn, &user).await?;
    if let Some(obj) = settings.as_object_mut() {
        obj.insert("subscriptions".into(), json!([]));
    }
    Ok(ok(settings))
}

/// `PATCH /api/profiles/me/settings`: a `ProfileSettingsUpdate` for the signer itself.
pub async fn update_my_profile_settings(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    state.user_writes()?;
    let body = envelope(body)?;
    let (signed, signer) =
        fed::preflight::<ProfileSettingsUpdate>(&state, &headers, body, fed::Scope::Events).await?;
    if !signed.message.target.eq_ignore_ascii_case(&signer) {
        return Err(ApiError::forbidden(
            "me/settings only edits the signer's own profile",
        ));
    }
    validate_own_settings(&signed.message)?;
    Ok(ok(
        apply_profile_settings(&state.pool, &signed, &signer).await?
    ))
}

fn validate_own_settings(message: &ProfileSettingsUpdate) -> Result<(), ApiError> {
    if message.permissions.is_some() {
        return Err(ApiError::forbidden(
            "Only a moderator may change profile permissions",
        ));
    }
    if message.email_verified == Some(true) {
        return Err(ApiError::bad_request(
            "Email verification cannot be self-assigned",
        ));
    }
    Ok(())
}

#[derive(Debug, Default)]
pub(super) struct EventPermissions {
    pub edit_any: bool,
    pub approve_any: bool,
    pub approve_own: bool,
}

/// Profile permissions are read from local authority, never request metadata.
pub(super) async fn event_permissions(
    pool: &PgPool,
    signer: &str,
) -> Result<EventPermissions, ApiError> {
    let (moderator, permissions): (bool, Value) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM explore.moderators WHERE address = $1), \
         COALESCE((SELECT permissions FROM explore.event_profile_settings WHERE \"user\" = $1), '[]'::jsonb)",
    ).bind(signer.to_ascii_lowercase()).fetch_one(pool).await?;
    let has = |permission: &str| {
        moderator
            || permissions
                .as_array()
                .is_some_and(|p| p.iter().any(|p| p.as_str() == Some(permission)))
    };
    Ok(EventPermissions {
        edit_any: has("edit_any_event"),
        approve_any: has("approve_any_event"),
        approve_own: has("approve_own_event"),
    })
}

/// `GET /api/profiles/{profile_id}/settings`: a moderator's read.
pub async fn get_profile_settings(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let user = state.signer(&headers, method.as_str(), uri.path()).await?;
    require_moderator(&state.pool, &user).await?;
    let mut conn = state.pool.acquire().await?;
    Ok(ok(load_settings(&mut conn, &profile_id).await?))
}

/// `PATCH /api/profiles/{profile_id}/settings`: a moderator's `ProfileSettingsUpdate`.
pub async fn update_profile_settings(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    state.operator_writes()?;
    let body = envelope(body)?;
    let (signed, signer) =
        fed::preflight::<ProfileSettingsUpdate>(&state, &headers, body, fed::Scope::Events).await?;
    if !signed.message.target.eq_ignore_ascii_case(&profile_id) {
        return Err(ApiError::bad_request(
            "target does not match path profile_id",
        ));
    }
    require_moderator(&state.pool, &signer).await?;
    Ok(ok(
        apply_profile_settings(&state.pool, &signed, &signer).await?
    ))
}

const SUBSCRIPTIONS_RETIRED: &str = "Web-push subscriptions are deprecated and no longer supported";

/// `GET`/`POST`/`DELETE /api/profiles/subscriptions`: 410 Gone.
pub async fn profile_subscriptions() -> ApiError {
    ApiError::http(410, SUBSCRIPTIONS_RETIRED)
}

async fn apply_schedule(
    state: &SharedState,
    headers: &HeaderMap,
    body: Value,
    raw_body: &[u8],
    path_id: Option<String>,
    method: &str,
    path: &str,
) -> Result<Json<ApiData<ScheduleRecord>>, ApiError> {
    state.operator_writes()?;
    if !catalyst_fed::is_envelope(&body) {
        return apply_legacy_schedule(state, headers, body, raw_body, path_id, method, path).await;
    }
    let body = envelope(body)?;
    let (signed, signer) =
        fed::preflight::<ScheduleUpsert>(state, headers, body, fed::Scope::Events).await?;
    if let Some(id) = &path_id
        && signed.message.schedule_id.as_ref() != Some(id)
    {
        return Err(ApiError::bad_request(
            "schedule_id in body does not match path",
        ));
    }
    require_moderator(&state.pool, &signer).await?;
    let m = &signed.message;
    let id = m
        .schedule_id
        .clone()
        .unwrap_or_else(|| fed::schedule_id(&signer, &m.name, &signed.nonce));
    let hash = signed.signature_hash();
    let payload = serde_json::to_value(m).unwrap_or_else(|_| json!({}));
    let at = |secs: i64| chrono::DateTime::from_timestamp(secs, 0).map(|t| t.naive_utc());
    let now = chrono::Utc::now().naive_utc();
    let mut tx = state.pool.begin().await?;
    fed::record_action(
        &mut tx,
        &fed::Action {
            scope: fed::Scope::Events,
            signature_hash: &hash,
            signer: &signer,
            subject_id: Some(&id),
            action_type: "ScheduleUpsert",
            payload: &payload,
            signed_at: signed.signed_at,
        },
    )
    .await?;
    sqlx::query(
        r#"INSERT INTO explore.schedules_local (id, name, description, image, theme, background, active_since, active_until, active, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name, description = EXCLUDED.description, image = EXCLUDED.image, theme = EXCLUDED.theme,
               background = EXCLUDED.background, active_since = EXCLUDED.active_since, active_until = EXCLUDED.active_until,
               active = EXCLUDED.active, updated_at = EXCLUDED.updated_at"#,
    )
    .bind(&id)
    .bind(&m.name)
    .bind(m.description.as_deref())
    .bind(m.image.as_deref())
    .bind(m.theme.as_deref())
    .bind(json!(m.background))
    .bind(at(m.active_since))
    .bind(at(m.active_until))
    .bind(m.active)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    state.schedules.invalidate();
    let schedule = state
        .schedules
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::internal("schedule vanished after write"))?;
    Ok(Json(ApiData::ok(schedule)))
}

/// The original signed-fetch JSON contract remains available beside the
/// federation envelope, with an exact-byte body hash and a unique UUID nonce
/// bound into its signed metadata. Audit insertion and application commit together.
/// A partial patch locks and merges the stored row,
/// and cannot silently create an absent schedule.
async fn apply_legacy_schedule(
    state: &SharedState,
    headers: &HeaderMap,
    body: Value,
    raw_body: &[u8],
    path_id: Option<String>,
    method: &str,
    path: &str,
) -> Result<Json<ApiData<ScheduleRecord>>, ApiError> {
    let signer = state.signer(headers, method, path).await?;
    let metadata: Value = headers
        .get("x-identity-metadata")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| serde_json::from_str(h).ok())
        .ok_or_else(|| ApiError::bad_request("Signed schedule metadata required"))?;
    let digest = hex::encode(Sha256::digest(raw_body));
    if metadata.get("body_sha256").and_then(Value::as_str) != Some(digest.as_str()) {
        return Err(ApiError::bad_request(
            "Schedule body must match signed body_sha256",
        ));
    }
    let nonce = metadata
        .get("nonce")
        .and_then(Value::as_str)
        .filter(|nonce| uuid::Uuid::parse_str(nonce).is_ok_and(|id| id.to_string() == *nonce))
        .ok_or_else(|| ApiError::bad_request("Signed schedule nonce must be a canonical UUID"))?;
    let action_hash = hex::encode(Sha256::digest(format!("legacy-schedule:{signer}:{nonce}")));
    let signed_at = headers
        .get("x-identity-timestamp")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<i64>().ok())
        .ok_or_else(|| ApiError::bad_request("Invalid timestamp"))?
        / 1000;
    let allowed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM explore.moderators WHERE address = $1) \
         OR EXISTS (SELECT 1 FROM explore.event_profile_settings WHERE \"user\" = $1 AND permissions ? 'edit_any_schedule')",
    ).bind(&signer).fetch_one(&state.pool).await?;
    if !allowed {
        return Err(ApiError::forbidden("Forbidden"));
    }
    let patch = body
        .as_object()
        .ok_or_else(|| ApiError::bad_request("invalid schedule body"))?;
    let fields = [
        "name",
        "description",
        "image",
        "theme",
        "background",
        "active_since",
        "active_until",
        "active",
    ];
    if patch.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(ApiError::bad_request("unknown schedule field"));
    }
    let mut tx = state.pool.begin().await?;
    let (id, mut doc) = match path_id {
        Some(id) => {
            let existing: Option<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(s) FROM explore.schedules_local s WHERE id = $1 FOR UPDATE",
            )
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?;
            (
                id,
                existing.ok_or_else(|| ApiError::not_found("Schedule not found"))?,
            )
        }
        None => (
            uuid::Uuid::new_v4().to_string(),
            json!({"active":true,"background":[]}),
        ),
    };
    let payload = json!({"body":body,"method":method,"path":path,"nonce":nonce});
    if !fed::record_action(
        &mut tx,
        &fed::Action {
            scope: fed::Scope::Events,
            signature_hash: &action_hash,
            signer: &signer,
            subject_id: Some(&id),
            action_type: "ScheduleUpsert",
            payload: &payload,
            signed_at,
        },
    )
    .await?
    {
        return Err(ApiError::http(409, "Schedule request already applied"));
    }
    for (key, value) in patch {
        doc[key] = value.clone();
    }
    let name = doc
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("schedule name required"))?;
    if name.chars().count() > 50 {
        return Err(ApiError::bad_request("schedule name too long"));
    }
    for field in ["description", "image", "theme"] {
        if doc
            .get(field)
            .is_some_and(|value| !value.is_null() && !value.is_string())
        {
            return Err(ApiError::bad_request("invalid schedule text"));
        }
    }
    if doc
        .get("description")
        .and_then(Value::as_str)
        .is_some_and(|value| value.chars().count() > 255)
    {
        return Err(ApiError::bad_request("schedule description too long"));
    }
    let background = doc
        .get("background")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("invalid schedule background"))?;
    if background.len() > 100
        || background.iter().any(|value| {
            value
                .as_str()
                .is_none_or(|value| value.chars().count() > 255)
        })
    {
        return Err(ApiError::bad_request("invalid schedule background"));
    }
    let active = doc
        .get("active")
        .and_then(Value::as_bool)
        .ok_or_else(|| ApiError::bad_request("invalid active flag"))?;
    let at = |field: &str| -> Result<Option<chrono::NaiveDateTime>, ApiError> {
        match doc.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => super::moderation::parse_rfc3339(value)
                .map(|value| value.naive_utc())
                .or_else(|| {
                    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok()
                })
                .map(Some)
                .ok_or_else(|| ApiError::bad_request(format!("invalid {field}"))),
            _ => Err(ApiError::bad_request(format!("invalid {field}"))),
        }
    };
    let since = at("active_since")?;
    let until = at("active_until")?;
    if let Some(image) = doc.get("image").and_then(Value::as_str) {
        crate::sanitize::sanitize_asset_url(image).map_err(ApiError::bad_request)?;
    }
    sqlx::query(
        "INSERT INTO explore.schedules_local (id,name,description,image,theme,background,active_since,active_until,active) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO UPDATE SET \
         name=EXCLUDED.name,description=EXCLUDED.description,image=EXCLUDED.image,theme=EXCLUDED.theme, \
         background=EXCLUDED.background,active_since=EXCLUDED.active_since,active_until=EXCLUDED.active_until,active=EXCLUDED.active,updated_at=timezone('UTC',now())",
    ).bind(&id).bind(name).bind(doc.get("description").and_then(Value::as_str))
        .bind(doc.get("image").and_then(Value::as_str)).bind(doc.get("theme").and_then(Value::as_str))
        .bind(json!(background)).bind(since).bind(until).bind(active).execute(&mut *tx).await?;
    tx.commit().await?;
    state.schedules.invalidate();
    let schedule = state
        .schedules
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::internal("schedule vanished after write"))?;
    Ok(Json(ApiData::ok(schedule)))
}

/// `POST /api/schedules`: a moderator's `ScheduleUpsert` (an id minted when absent).
pub async fn create_schedule(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    raw_body: Bytes,
) -> Result<Json<ApiData<ScheduleRecord>>, ApiError> {
    let body = serde_json::from_slice(&raw_body)
        .map_err(|_| ApiError::bad_request("Invalid schedule JSON"))?;
    apply_schedule(&state, &headers, body, &raw_body, None, "post", uri.path()).await
}

/// `PATCH /api/schedules/{schedule_id}`: the envelope must name the same id.
pub async fn patch_schedule(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(schedule_id): Path<String>,
    raw_body: Bytes,
) -> Result<Json<ApiData<ScheduleRecord>>, ApiError> {
    let body = serde_json::from_slice(&raw_body)
        .map_err(|_| ApiError::bad_request("Invalid schedule JSON"))?;
    apply_schedule(
        &state,
        &headers,
        body,
        &raw_body,
        Some(schedule_id),
        "patch",
        uri.path(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_schedule_signatures_bind_exact_bytes_reject_replay_and_audit_once() {
        use crate::shared::signed_fetch::test_support::signed_headers_by;
        use alloy::signers::local::PrivateKeySigner;
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let key = PrivateKeySigner::random();
        let signer = format!("{:#x}", key.address());
        sqlx::query(
            "INSERT INTO explore.moderators(address,added_at) VALUES($1,extract(epoch FROM now())::bigint)",
        )
        .bind(&signer)
        .execute(&pool)
        .await
        .unwrap();
        let app = crate::router(std::sync::Arc::new(crate::ExploreState::new(
            pool.clone(),
            crate::Settings {
                operator_writes_enabled: true,
                ..crate::Settings::for_tests()
            },
            None,
        )));
        let app = axum::Router::new().nest("/events", app);
        let body = r#"{"name":"Signed schedule"}"#;
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let metadata = json!({"body_sha256":hex::encode(Sha256::digest(body.as_bytes())),"nonce":uuid::Uuid::new_v4().to_string()}).to_string();
        let headers = signed_headers_by(
            &key,
            "post",
            "/events/api/schedules",
            &metadata,
            true,
            &timestamp,
        )
        .await;
        async fn call(app: &axum::Router, headers: &HeaderMap, body: &str) -> StatusCode {
            let mut request = Request::builder()
                .method("POST")
                .uri("/events/api/schedules")
                .header("content-type", "application/json");
            for (name, value) in headers {
                request = request.header(name, value);
            }
            app.clone()
                .oneshot(request.body(Body::from(body.to_string())).unwrap())
                .await
                .unwrap()
                .status()
        }
        assert_eq!(
            call(&app, &headers, r#"{"name":"Tampered"}"#).await,
            StatusCode::BAD_REQUEST
        );
        let unbound = signed_headers_by(
            &key,
            "post",
            "/events/api/schedules",
            "{}",
            true,
            &timestamp,
        )
        .await;
        assert_eq!(call(&app, &unbound, body).await, StatusCode::BAD_REQUEST);
        assert_eq!(call(&app, &headers, body).await, StatusCode::OK);
        assert_eq!(call(&app, &headers, body).await, StatusCode::CONFLICT);
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM explore.signed_actions WHERE signer=$1 AND action_type='ScheduleUpsert'").bind(&signer).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 1);
        let audit: Value = sqlx::query_scalar(
            "SELECT message_payload FROM explore.signed_actions WHERE signer=$1",
        )
        .bind(&signer)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(audit["body"]["name"], "Signed schedule");
    }

    #[test]
    fn a_profile_owner_cannot_self_assign_permissions_or_verification() {
        for body in [
            json!({"target":"0xalice","permissions":["approve_any_event"],"signed_at":1}),
            json!({"target":"0xalice","email_verified":true,"signed_at":1}),
        ] {
            let message: ProfileSettingsUpdate = serde_json::from_value(body).unwrap();
            assert!(validate_own_settings(&message).is_err());
        }
        let message: ProfileSettingsUpdate = serde_json::from_value(
            json!({"target":"0xalice","use_local_time":false,"signed_at":1}),
        )
        .unwrap();
        assert!(validate_own_settings(&message).is_ok());
    }

    #[tokio::test]
    async fn the_retired_subscriptions_answer_410() {
        use axum::response::IntoResponse;
        assert_eq!(profile_subscriptions().await.into_response().status(), 410);
    }

    #[test]
    fn a_body_without_the_envelope_keys_is_refused() {
        assert_eq!(
            envelope(json!({ "email": "a@b.c" }))
                .unwrap_err()
                .to_string(),
            "missing signed body"
        );
        assert!(envelope(json!({ "domain": {}, "message": {}, "signature": "0x" })).is_ok());
    }
}
