//! The events write routes: `POST /api/events`, `PATCH`/`DELETE
//! /api/events/{id}` and the RSVPs `POST`/`DELETE /api/events/{id}/attendees`.
//!
//! Two callers, as before: the events admin bearer
//! (`CATALYRST_EVENTS_ADMIN_TOKEN`) creates as `admin`, moderates with the
//! old admin body and soft-deletes as the admin, behind
//! `OPS_WRITES_ENABLED`; a signed wallet creates its own event (pending
//! approval), edits and deletes its own only (403 otherwise) and RSVPs,
//! behind `DATA_WRITES_ENABLED`. An owner's edit of an approved event's
//! content (or its featured item) sends it back to approval. Every write
//! lands in `explore.event` with its `explore.events_local` audit in one
//! transaction (`events::moderation::store`). Only events created here
//! (`origin = 'local'`) take their owner's edit and delete: on a mirrored
//! event those answer 409 naming the upstream (the mirror writes its
//! fields). The admin's moderation and delete of a mirrored event are local
//! policy (`explore.event_moderation`, re-applied by every pass; the delete
//! hides it from the public reads), and an RSVP to it is a local row the
//! count adds; the mirror deletes both with the event.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Method};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use super::moderation::{
    self, AdminPatch, CreateEvent, MAX_EVENT_DURATION_MS, PAST_FINISH_AT_MESSAGE,
    PAST_RECURRENT_UNTIL_MESSAGE, is_soft_deleted, max_duration_message, normalized_series,
    parse_rfc3339, series_may_have_future_dates, validate_featured_item,
};
use super::{EventAttendeeRecord, EventRecord};
use crate::SharedState;
use crate::shared::errors::{ApiData, ApiError};

const ADMIN_SIGNER: &str = "admin";
const DATE_FIELDS: [&str; 11] = [
    "start_at",
    "duration",
    "recurrent",
    "recurrent_frequency",
    "recurrent_interval",
    "recurrent_count",
    "recurrent_until",
    "recurrent_setpos",
    "recurrent_monthday",
    "recurrent_weekday_mask",
    "recurrent_month_mask",
];

fn deserialize_some<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    T::deserialize(d).map(Some)
}

/// The owner's edit body (unknown fields refused; the moderation fields are
/// accepted and ignored, as before).
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateEventBody {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub image_vertical: Option<String>,
    #[serde(default)]
    pub start_at: Option<String>,
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(default)]
    pub all_day: Option<bool>,
    #[serde(default)]
    pub recurrent: Option<bool>,
    #[serde(default)]
    pub recurrent_frequency: Option<String>,
    #[serde(default)]
    pub recurrent_setpos: Option<i64>,
    #[serde(default)]
    pub recurrent_monthday: Option<i64>,
    #[serde(default)]
    pub recurrent_weekday_mask: Option<i64>,
    #[serde(default)]
    pub recurrent_month_mask: Option<i64>,
    #[serde(default)]
    pub recurrent_interval: Option<i64>,
    #[serde(default)]
    pub recurrent_count: Option<i64>,
    #[serde(default)]
    pub recurrent_until: Option<String>,
    #[serde(default)]
    pub x: Option<i32>,
    #[serde(default)]
    pub y: Option<i32>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub server: Option<Option<String>>,
    #[serde(default)]
    pub contact: Option<String>,
    #[serde(default)]
    pub details: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub categories: Option<Vec<String>>,
    #[serde(default)]
    pub schedules: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub world: Option<Option<bool>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub community_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub featured_item: Option<Option<String>>,
    #[serde(default)]
    pub approved: Option<bool>,
    #[serde(default)]
    pub rejected: Option<bool>,
    #[serde(default)]
    pub rejection_reason: Option<String>,
    #[serde(default)]
    pub highlighted: Option<bool>,
    #[serde(default)]
    pub trending: Option<bool>,
}

#[derive(Debug, Deserialize, Default, Serialize)]
pub struct DeleteEventBody {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

fn normalize_featured_item(value: Option<&str>) -> Result<Option<String>, ApiError> {
    match value.filter(|v| !v.is_empty()) {
        Some(urn) => {
            validate_featured_item(urn).map_err(ApiError::bad_request)?;
            Ok(Some(urn.to_owned()))
        }
        None => Ok(None),
    }
}

fn dedup_schedules(schedules: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    schedules
        .into_iter()
        .filter(|s| seen.insert(s.clone()))
        .collect()
}

fn location_touched(body: &UpdateEventBody) -> bool {
    body.x.is_some() || body.y.is_some() || body.server.is_some() || body.world.is_some()
}

fn recurrence_touched(body: &UpdateEventBody) -> bool {
    body.recurrent.is_some()
        || body.recurrent_frequency.is_some()
        || body.recurrent_setpos.is_some()
        || body.recurrent_monthday.is_some()
        || body.recurrent_weekday_mask.is_some()
        || body.recurrent_month_mask.is_some()
        || body.recurrent_interval.is_some()
        || body.recurrent_count.is_some()
        || body.recurrent_until.is_some()
        || body.start_at.is_some()
        || body.duration.is_some()
}

fn content_touched(body: &UpdateEventBody) -> bool {
    body.name.is_some()
        || body.description.is_some()
        || body.image.is_some()
        || body.image_vertical.is_some()
        || body.url.is_some()
        || body.categories.is_some()
        || body.community_id.is_some()
        || location_touched(body)
}

fn moderated_content_changed(before: &Map<String, Value>, after: &Map<String, Value>) -> bool {
    [
        "name",
        "description",
        "image",
        "image_vertical",
        "url",
        "categories",
        "community_id",
        "x",
        "y",
        "server",
        "world",
    ]
    .iter()
    .any(|key| {
        if *key == "description" {
            let description = |raw: &Map<String, Value>| {
                crate::sanitize::sanitize_event_description(
                    raw.get(*key).and_then(Value::as_str).unwrap_or_default(),
                )
            };
            description(before) != description(after)
        } else if *key == "categories" {
            let categories = |raw: &Map<String, Value>| {
                let mut categories = raw
                    .get(*key)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                categories.sort_by_key(Value::to_string);
                categories
            };
            categories(before) != categories(after)
        } else {
            before.get(*key).unwrap_or(&Value::Null) != after.get(*key).unwrap_or(&Value::Null)
        }
    })
}

fn featured_item_changed(raw: &Map<String, Value>, body: &UpdateEventBody) -> bool {
    let Some(next) = &body.featured_item else {
        return false;
    };
    next.as_deref().filter(|v| !v.is_empty()) != raw.get("featured_item").and_then(Value::as_str)
}

fn date_key_present(body: &[u8]) -> bool {
    serde_json::from_slice::<Map<String, Value>>(body)
        .map(|m| DATE_FIELDS.iter().any(|k| m.contains_key(*k)))
        .unwrap_or(false)
}

fn set_opt<T: Serialize>(map: &mut Map<String, Value>, key: &str, val: &Option<T>) {
    if let Some(v) = val {
        map.insert(key.into(), json!(v));
    }
}

/// The old owner edit onto the stored document.
pub fn apply_owner_edit(
    raw: &mut Map<String, Value>,
    body: &UpdateEventBody,
    date_key_present: bool,
) -> Result<(), ApiError> {
    if let Some(name) = &body.name {
        if name.chars().count() > 150 {
            return Err(ApiError::bad_request("name must be at most 150 characters"));
        }
        raw.insert("name".into(), json!(name));
    }
    set_opt(raw, "description", &body.description);
    if body
        .description
        .as_ref()
        .is_some_and(|value| value.chars().count() > 5000)
        || body
            .details
            .as_ref()
            .is_some_and(|value| value.chars().count() > 5000)
        || body
            .contact
            .as_ref()
            .is_some_and(|value| value.chars().count() > 100)
    {
        return Err(ApiError::bad_request(
            "event text exceeds the maximum length",
        ));
    }
    for (key, img) in [
        ("image", &body.image),
        ("image_vertical", &body.image_vertical),
    ] {
        if let Some(img) = img {
            let img = crate::sanitize::sanitize_asset_url(img).map_err(ApiError::bad_request)?;
            raw.insert(key.into(), json!(img));
        }
    }
    if let Some(server) = &body.server {
        raw.insert("server".into(), json!(server));
    }
    set_opt(raw, "contact", &body.contact);
    set_opt(raw, "details", &body.details);
    set_opt(raw, "recurrent_frequency", &body.recurrent_frequency);
    set_opt(raw, "recurrent_until", &body.recurrent_until);
    set_opt(raw, "community_id", &body.community_id);
    if let Some(item) = &body.featured_item {
        let item = normalize_featured_item(item.as_deref())?;
        raw.insert("featured_item".into(), json!(item));
    }
    set_opt(raw, "all_day", &body.all_day);
    set_opt(raw, "recurrent", &body.recurrent);
    if let Some(world) = body.world {
        raw.insert("world".into(), json!(world.unwrap_or(false)));
    }
    set_opt(raw, "recurrent_setpos", &body.recurrent_setpos);
    set_opt(raw, "recurrent_monthday", &body.recurrent_monthday);
    set_opt(raw, "recurrent_weekday_mask", &body.recurrent_weekday_mask);
    set_opt(raw, "recurrent_month_mask", &body.recurrent_month_mask);
    set_opt(raw, "recurrent_interval", &body.recurrent_interval);
    set_opt(raw, "recurrent_count", &body.recurrent_count);
    if let Some(duration) = body.duration {
        if duration < 0 {
            return Err(ApiError::bad_request("duration must be positive"));
        }
        let stored = raw.get("duration").and_then(Value::as_i64).unwrap_or(0);
        if duration > stored.max(MAX_EVENT_DURATION_MS) {
            return Err(ApiError::bad_request(max_duration_message()));
        }
        raw.insert("duration".into(), json!(duration));
    }
    if let Some(url) = &body.url {
        let url = crate::sanitize::validate_event_url(url).map_err(ApiError::bad_request)?;
        raw.insert("url".into(), json!(url));
    }
    if let Some(cats) = &body.categories {
        raw.insert("categories".into(), json!(cats));
    }
    if let Some(scheds) = &body.schedules {
        raw.insert("schedules".into(), json!(dedup_schedules(scheds.clone())));
    }
    for (key, v) in [("x", body.x), ("y", body.y)] {
        if let Some(v) = v {
            if !(-170..=170).contains(&v) {
                return Err(ApiError::bad_request("coordinates out of range"));
            }
            raw.insert(key.into(), json!(v));
        }
    }
    if let Some(start_at) = &body.start_at {
        let parsed =
            parse_rfc3339(start_at).ok_or_else(|| ApiError::bad_request("invalid start_at"))?;
        raw.insert("start_at".into(), json!(parsed.to_rfc3339()));
    }
    if recurrence_touched(body) || date_key_present {
        recompute_occurrences(raw)?;
        reject_past_dates(raw)?;
    }
    let x = raw.get("x").and_then(Value::as_i64).unwrap_or(0) as i32;
    let y = raw.get("y").and_then(Value::as_i64).unwrap_or(0) as i32;
    raw.insert("position".into(), json!([x, y]));
    raw.insert("coordinates".into(), json!([x, y]));
    Ok(())
}

fn recompute_occurrences(raw: &mut Map<String, Value>) -> Result<(), ApiError> {
    super::recurrence::recompute(raw, Utc::now())
}

fn reject_past_dates(raw: &Map<String, Value>) -> Result<(), ApiError> {
    let now = Utc::now();
    let recurrent = raw
        .get("recurrent")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let frequency = raw.get("recurrent_frequency").and_then(Value::as_str);
    let count = raw.get("recurrent_count").and_then(Value::as_i64);
    let until = raw
        .get("recurrent_until")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339);
    if !series_may_have_future_dates(recurrent, frequency, count, until, now)
        && raw
            .get("finish_at")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339)
            .is_some_and(|f| f <= now)
    {
        return Err(ApiError::bad_request(PAST_FINISH_AT_MESSAGE));
    }
    if normalized_series(recurrent, frequency, count, until) && until.is_some_and(|u| u <= now) {
        return Err(ApiError::bad_request(PAST_RECURRENT_UNTIL_MESSAGE));
    }
    Ok(())
}

/// The stored document, its owner (the document's `user`, else the column)
/// and whether it is mirrored. The owner never writes a mirrored event: that
/// answers 409 naming the upstream (`owner_write`); the operator's delete of
/// one is local moderation ([`moderation::hide_mirrored`]).
async fn load_raw(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<Option<(Map<String, Value>, String, bool)>, ApiError> {
    let row: Option<(Value, Option<String>, String)> = sqlx::query_as(
        "SELECT raw, user_creator, origin FROM explore.event WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let mirrored = row
        .as_ref()
        .is_some_and(|(_, _, origin)| origin != moderation::LOCAL);
    Ok(row.map(|(raw, creator, _)| {
        let owner = raw
            .get("user")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(creator)
            .unwrap_or_default()
            .to_lowercase();
        let map = match raw {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        (map, owner, mirrored)
    }))
}

async fn answer(state: &SharedState, id: &str) -> Result<Json<ApiData<EventRecord>>, ApiError> {
    let record = state
        .events
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Not found event \"{id}\"")))?;
    Ok(Json(ApiData::ok(record)))
}

fn not_found(id: &str) -> ApiError {
    ApiError::not_found(format!("Not found event \"{id}\""))
}

/// `POST /api/events`.
pub async fn create_event(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ApiData<EventRecord>>, ApiError> {
    let admin = super::handlers::is_admin_request(&headers, &state);
    let signer = if admin {
        state.operator_writes()?;
        ADMIN_SIGNER.to_string()
    } else {
        state.user_writes()?;
        state.signer(&headers, method.as_str(), uri.path()).await?
    };
    let body: CreateEvent = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid event body: {e}")))?;
    let id = moderation::create_as(&state.pool, &body, &signer, &signer).await?;
    answer(&state, &id).await
}

/// `PATCH /api/events/{id}`: the admin's moderation or the owner's edit.
pub async fn patch_event(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(event_id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiData<EventRecord>>, ApiError> {
    if super::handlers::is_admin_request(&headers, &state) {
        state.operator_writes()?;
        let patch: AdminPatch = serde_json::from_slice(&body)
            .map_err(|e| ApiError::bad_request(format!("invalid patch body: {e}")))?;
        if patch.actor.as_ref().is_some_and(|a| a.chars().count() > 42) {
            return Err(ApiError::bad_request("actor too long"));
        }
        let actor = patch
            .actor
            .clone()
            .unwrap_or_else(|| ADMIN_SIGNER.to_string());
        if !moderation::moderate(&state.pool, &event_id, &patch, &actor).await? {
            return Err(not_found(&event_id));
        }
        return answer(&state, &event_id).await;
    }
    state.user_writes()?;
    let signer = state.signer(&headers, method.as_str(), uri.path()).await?;
    let dates_present = date_key_present(&body);
    let mut edit: UpdateEventBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid patch body: {e}")))?;
    let permissions = super::profiles::event_permissions(&state.pool, &signer).await?;
    super::mirror::reclaim_local(&state.pool).await?;
    let mut tx = state.pool.begin().await?;
    let Some((mut raw, owner, mirrored)) = load_raw(&mut tx, &event_id).await? else {
        return Err(not_found(&event_id));
    };
    if mirrored {
        tx.rollback().await?;
        return Err(moderation::mirrored_event(&state.pool, &event_id).await);
    }
    if is_soft_deleted(&raw) {
        return Err(not_found(&event_id));
    }
    let is_owner = owner == signer;
    if !is_owner && !permissions.edit_any {
        return Err(ApiError::forbidden(
            "You don't have permission to edit this event",
        ));
    }
    if !is_owner {
        state.operator_writes()?;
        edit.contact = None;
        edit.details = None;
        edit.community_id = None;
    }
    if !permissions.edit_any {
        edit.schedules = None;
        edit.url = None;
    }
    let was_approved = raw
        .get("approved")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let featured_changed = featured_item_changed(&raw, &edit);
    if let Some(community) = &edit.community_id {
        // Detaching still requires management of the existing community.
        if let Some(community) = community
            .as_deref()
            .or_else(|| raw.get("community_id").and_then(Value::as_str))
        {
            moderation::validate_community(&mut *tx, community, &signer).await?;
        }
    }
    let before = raw.clone();
    apply_owner_edit(&mut raw, &edit, dates_present)?;
    moderation::validate_references(&mut *tx, &raw).await?;
    if location_touched(&edit) {
        let world = raw.get("world").and_then(Value::as_bool).unwrap_or(false);
        let server = raw
            .get("server")
            .and_then(Value::as_str)
            .map(str::to_string);
        let x = raw.get("x").and_then(Value::as_i64).unwrap_or(0) as i32;
        let y = raw.get("y").and_then(Value::as_i64).unwrap_or(0) as i32;
        let (place_id, world) =
            moderation::resolve_location(&mut *tx, world, server.as_deref(), x, y).await?;
        raw.insert(
            "place_id".into(),
            place_id.map(Value::String).unwrap_or(Value::Null),
        );
        raw.insert("world".into(), json!(world));
    }
    // Another reviewer's rejection is a moderation hold: self-approval cannot remove it.
    let held = moderation::flag(&before, "rejected")
        && !before
            .get("rejected_by")
            .and_then(Value::as_str)
            .is_some_and(|by| by.eq_ignore_ascii_case(&signer));
    let can_approve = permissions.approve_any || (is_owner && permissions.approve_own && !held);
    if can_approve {
        if let Some(approved) = edit.approved {
            raw.insert("approved".into(), json!(approved));
            if approved {
                raw.insert("approved_by".into(), json!(signer));
                raw.insert("rejected".into(), json!(false));
                raw.insert("rejected_by".into(), Value::Null);
                raw.insert("rejection_reason".into(), Value::Null);
            } else {
                raw.insert("approved_by".into(), Value::Null);
            }
        }
    }
    if permissions.edit_any {
        set_opt(&mut raw, "highlighted", &edit.highlighted);
        set_opt(&mut raw, "trending", &edit.trending);
    }
    if can_approve && edit.approved != Some(true) {
        if let Some(rejected) = edit.rejected {
            raw.insert("rejected".into(), json!(rejected));
            if rejected && !moderation::flag(&before, "rejected") {
                raw.insert("rejected_by".into(), json!(signer));
            } else if !rejected {
                raw.insert("rejected_by".into(), Value::Null);
                raw.insert("rejection_reason".into(), Value::Null);
            }
        }
        if moderation::flag(&raw, "rejected") {
            set_opt(&mut raw, "rejection_reason", &edit.rejection_reason);
        }
    }
    if moderation::flag(&raw, "rejected") {
        raw.insert("approved".into(), json!(false));
        raw.insert("approved_by".into(), Value::Null);
        raw.insert("highlighted".into(), json!(false));
        raw.insert("trending".into(), json!(false));
    }
    if was_approved
        && !can_approve
        && ((content_touched(&edit) && moderated_content_changed(&before, &raw))
            || featured_changed)
    {
        raw.insert("approved".into(), json!(false));
        raw.insert("approved_by".into(), Value::Null);
        raw.insert("highlighted".into(), json!(false));
        raw.insert("trending".into(), json!(false));
    }
    raw.insert("updated_at".into(), json!(Utc::now().to_rfc3339()));
    moderation::store_locked(&mut tx, &event_id, raw, &signer, json!({ "edit": edit })).await?;
    tx.commit().await?;
    answer(&state, &event_id).await
}

/// `DELETE /api/events/{id}`: a soft delete, by the admin or the owner.
pub async fn delete_event(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(event_id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiData<EventRecord>>, ApiError> {
    let admin = super::handlers::is_admin_request(&headers, &state);
    let signer = if admin {
        state.operator_writes()?;
        None
    } else {
        state.user_writes()?;
        Some(state.signer(&headers, method.as_str(), uri.path()).await?)
    };
    let permissions = if let Some(signer) = signer.as_ref() {
        super::profiles::event_permissions(&state.pool, signer).await?
    } else {
        Default::default()
    };
    super::mirror::reclaim_local(&state.pool).await?;
    let mut tx = state.pool.begin().await?;
    let Some((mut raw, owner, mirrored)) = load_raw(&mut tx, &event_id).await? else {
        return Err(not_found(&event_id));
    };
    if mirrored && signer.is_some() {
        tx.rollback().await?;
        return Err(moderation::mirrored_event(&state.pool, &event_id).await);
    }
    if is_soft_deleted(&raw) {
        tx.rollback().await?;
        return answer(&state, &event_id).await;
    }
    let now = Utc::now().to_rfc3339();
    let (provenance, payload) = match signer {
        None => {
            let body: DeleteEventBody = if body.is_empty() {
                DeleteEventBody::default()
            } else {
                serde_json::from_slice(&body)
                    .map_err(|e| ApiError::bad_request(format!("invalid delete body: {e}")))?
            };
            raw.insert("deleted_by_admin".into(), json!(true));
            raw.insert("deleted_by".into(), json!(body.actor));
            raw.insert("deleted_reason".into(), json!(body.reason));
            (
                body.actor.clone().unwrap_or_else(|| ADMIN_SIGNER.into()),
                json!({ "delete": body }),
            )
        }
        Some(signer) => {
            if owner != signer && !permissions.edit_any {
                return Err(ApiError::forbidden(
                    "You don't have permission to delete this event",
                ));
            }
            if owner != signer {
                state.operator_writes()?;
            }
            raw.insert(
                if owner == signer {
                    "deleted_by_user"
                } else {
                    "deleted_by_admin"
                }
                .into(),
                json!(true),
            );
            raw.insert("deleted_by".into(), json!(signer));
            (signer, json!({ "delete": {} }))
        }
    };
    raw.insert("deleted_at".into(), json!(now));
    raw.insert("updated_at".into(), json!(now));
    if mirrored {
        moderation::hide_mirrored_locked(&mut tx, &event_id, raw, &provenance).await?;
    } else {
        moderation::store_locked(&mut tx, &event_id, raw, &provenance, payload).await?;
    }
    tx.commit().await?;
    answer(&state, &event_id).await
}

/// Visible to `signer`: approved and not rejected, or theirs; never a deleted one.
async fn exists_visible(
    state: &SharedState,
    event_id: &str,
    signer: &str,
) -> Result<bool, ApiError> {
    let row: Option<(bool, Option<String>, Value)> =
        sqlx::query_as("SELECT approved, user_creator, raw FROM explore.event WHERE id = $1")
            .bind(event_id)
            .fetch_optional(&state.pool)
            .await?;
    let Some((approved, creator, raw)) = row else {
        return Ok(false);
    };
    let map = raw.as_object().cloned().unwrap_or_default();
    if is_soft_deleted(&map) {
        return Ok(false);
    }
    let rejected = map
        .get("rejected")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let owner = map
        .get("user")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or(creator)
        .map(|u| u.to_lowercase());
    Ok((approved && !rejected) || owner.as_deref() == Some(signer))
}

async fn rsvp(
    state: SharedState,
    method: Method,
    path: &str,
    headers: HeaderMap,
    event_id: String,
    going: bool,
) -> Result<Json<ApiData<Vec<EventAttendeeRecord>>>, ApiError> {
    state.user_writes()?;
    let signer = state.signer(&headers, method.as_str(), path).await?;
    if !exists_visible(&state, &event_id, &signer).await? {
        return Err(not_found(&event_id));
    }
    let list = if going {
        let user_name = headers
            .get(crate::shared::signed_fetch::AUTH_METADATA_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|m| m.get("name").and_then(Value::as_str).map(str::to_string));
        super::attendees::rsvp_going(&state.pool, &event_id, &signer, user_name.as_deref()).await?
    } else {
        super::attendees::rsvp_cancel(&state.pool, &event_id, &signer).await?
    };
    Ok(Json(ApiData::ok(list)))
}

/// `POST /api/events/{id}/attendees`: the signer is going.
pub async fn create_event_attendee(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Json<ApiData<Vec<EventAttendeeRecord>>>, ApiError> {
    rsvp(state, method, uri.path(), headers, event_id, true).await
}

/// `DELETE /api/events/{id}/attendees`: the signer is not.
pub async fn delete_event_attendee(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Json<ApiData<Vec<EventAttendeeRecord>>>, ApiError> {
    rsvp(state, method, uri.path(), headers, event_id, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> Map<String, Value> {
        json!({ "name": "e", "x": 1, "y": 2, "duration": 3_600_000, "start_at": "2099-01-01T00:00:00+00:00", "approved": true }).as_object().unwrap().clone()
    }

    #[test]
    fn an_owner_edit_rewrites_the_fields_it_names_and_the_position() {
        let mut r = raw();
        let body: UpdateEventBody = serde_json::from_value(json!({ "name": "renamed", "x": 10, "categories": ["art"], "schedules": ["a", "a", "b"] })).unwrap();
        apply_owner_edit(&mut r, &body, false).unwrap();
        assert_eq!(
            (
                r["name"].clone(),
                r["position"].clone(),
                r["schedules"].clone()
            ),
            (json!("renamed"), json!([10, 2]), json!(["a", "b"]))
        );
        assert!(content_touched(&body) && location_touched(&body));
    }

    #[test]
    fn an_owner_edit_is_validated() {
        let too_long: UpdateEventBody =
            serde_json::from_value(json!({ "name": "x".repeat(151) })).unwrap();
        assert!(apply_owner_edit(&mut raw(), &too_long, false).is_err());
        let off_map: UpdateEventBody = serde_json::from_value(json!({ "y": 171 })).unwrap();
        assert!(apply_owner_edit(&mut raw(), &off_map, false).is_err());
        let past: UpdateEventBody =
            serde_json::from_value(json!({ "start_at": "2000-01-01T00:00:00Z" })).unwrap();
        assert_eq!(
            apply_owner_edit(&mut raw(), &past, true)
                .unwrap_err()
                .to_string(),
            PAST_FINISH_AT_MESSAGE
        );
        let longer: UpdateEventBody =
            serde_json::from_value(json!({ "duration": MAX_EVENT_DURATION_MS + 1 })).unwrap();
        assert!(apply_owner_edit(&mut raw(), &longer, false).is_err());
        assert!(
            serde_json::from_value::<UpdateEventBody>(json!({ "nope": 1 })).is_err(),
            "unknown fields are refused"
        );
    }

    #[test]
    fn a_rescheduled_event_recomputes_its_occurrence() {
        let mut r = raw();
        let body: UpdateEventBody = serde_json::from_value(
            json!({ "start_at": "2099-02-01T10:00:00Z", "duration": 1_800_000 }),
        )
        .unwrap();
        apply_owner_edit(&mut r, &body, true).unwrap();
        assert_eq!(r["next_start_at"], json!("2099-02-01T10:00:00+00:00"));
        assert_eq!(r["finish_at"], json!("2099-02-01T10:30:00+00:00"));
        assert_eq!(r["recurrent_dates"], json!(["2099-02-01T10:00:00+00:00"]));
        assert!(
            date_key_present(br#"{"recurrent_count": 2}"#)
                && !date_key_present(br#"{"name": "x"}"#)
        );
    }

    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::shared::signed_fetch::test_support::{address, signed_headers};
    use crate::{ExploreState, Settings, router};

    async fn call(
        app: &axum::Router,
        method: &str,
        path: &str,
        auth: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(path);
        match auth {
            Some("signed") => {
                let signed = path.split('?').next().unwrap();
                let metadata = if signed.starts_with("/api/schedules") {
                    use sha2::{Digest, Sha256};
                    json!({"name":"Alice","body_sha256":hex::encode(Sha256::digest(body.as_ref().map(Value::to_string).unwrap_or_default().as_bytes())),"nonce":uuid::Uuid::new_v4().to_string()}).to_string()
                } else {
                    r#"{"name":"Alice"}"#.to_string()
                };
                for (k, v) in signed_headers(&method.to_lowercase(), signed, &metadata, true)
                    .await
                    .iter()
                {
                    req = req.header(k, v);
                }
            }
            Some(token) => req = req.header("authorization", format!("Bearer {token}")),
            None => {}
        }
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
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())),
        )
    }

    fn new_event() -> Value {
        json!({ "name": "Launch party", "start_at": "2099-06-01T18:00:00Z", "duration": 3_600_000, "x": 10, "y": -20, "description": "<b>come</b>" })
    }

    async fn call_as(
        app: &axum::Router,
        method: &str,
        path: &str,
        wallet: &alloy::signers::local::PrivateKeySigner,
        body: Value,
    ) -> (StatusCode, Value) {
        let headers = crate::shared::signed_fetch::test_support::signed_headers_by(
            wallet,
            &method.to_lowercase(),
            path,
            "{}",
            true,
            &Utc::now().timestamp_millis().to_string(),
        )
        .await;
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        for (key, value) in headers.iter() {
            req = req.header(key, value)
        }
        let response = app
            .clone()
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn http_patch_permissions_preserve_rejection_provenance_and_remoderate_changed_communities()
     {
        let Some(pool) = crate::db::scratch::pool(1).await else {
            return;
        };
        let owner = alloy::signers::local::PrivateKeySigner::random();
        let editor = alloy::signers::local::PrivateKeySigner::random();
        let owner_address = format!("{:#x}", owner.address());
        let editor_address = format!("{:#x}", editor.address());
        let event = moderation::create_as(
            &pool,
            &serde_json::from_value(new_event()).unwrap(),
            &owner_address,
            "test",
        )
        .await
        .unwrap();
        let path = format!("/api/events/{event}");
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: true,
                operator_writes_enabled: true,
                events_admin_token: Some("patch-review-admin".into()),
                ..Settings::for_tests()
            },
            None,
        )));
        let (status, _) = call(
            &app,
            "PATCH",
            &path,
            Some("patch-review-admin"),
            Some(json!({"approved":true})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, date) = call_as(
            &app,
            "PATCH",
            &path,
            &owner,
            json!({"start_at":"2099-07-01T18:00:00Z"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{date}");
        assert_eq!(date["data"]["approved"], true);
        let (_, renamed) = call_as(
            &app,
            "PATCH",
            &path,
            &owner,
            json!({"name":"Changed public title"}),
        )
        .await;
        assert_eq!(renamed["data"]["approved"], false);
        let (_, rejected) = call(
            &app,
            "PATCH",
            &path,
            Some("patch-review-admin"),
            Some(json!({"rejected":true,"reason":"Moderator decision"})),
        )
        .await;
        let rejecting_actor = rejected["data"]["rejected_by"].clone();
        let (status, owner_edit) = call_as(
            &app,
            "PATCH",
            &path,
            &owner,
            json!({"rejected":false,"rejection_reason":"Forged","details":"Owner instructions"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{owner_edit}");
        assert_eq!(owner_edit["data"]["rejected"], true);
        assert_eq!(owner_edit["data"]["rejected_by"], rejecting_actor);
        assert_eq!(owner_edit["data"]["rejection_reason"], "Moderator decision");
        sqlx::query("INSERT INTO explore.event_profile_settings (\"user\",permissions) VALUES ($1,'[\"edit_any_event\"]')").bind(&editor_address).execute(&pool).await.unwrap();
        let (status, edited) = call_as(
            &app,
            "PATCH",
            &path,
            &editor,
            json!({"name":"Editor title","rejected":false,"rejection_reason":"Also forged"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        assert_eq!(edited["data"]["rejected_by"], rejecting_actor);
        assert_eq!(edited["data"]["rejection_reason"], "Moderator decision");
        sqlx::query("UPDATE explore.event_profile_settings SET permissions='[\"edit_any_event\",\"approve_any_event\"]' WHERE \"user\"=$1").bind(&editor_address).execute(&pool).await.unwrap();
        let (status, approved) =
            call_as(&app, "PATCH", &path, &editor, json!({"approved":true})).await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(approved["data"]["approved"], true);
        assert_eq!(approved["data"]["rejected"], false);
        assert!(approved["data"]["rejected_by"].is_null());
        assert!(approved["data"]["rejection_reason"].is_null());
        let community = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO social.communities (id,name,owner_address) VALUES ($1,'Managed community',$2)").bind(community).bind(&owner_address).execute(&pool).await.unwrap();
        let (status, linked) = call_as(
            &app,
            "PATCH",
            &path,
            &owner,
            json!({"community_id":community.to_string()}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{linked}");
        assert_eq!(
            linked["data"]["approved"], false,
            "a changed public community association requires moderation"
        );
        call(
            &app,
            "PATCH",
            &path,
            Some("patch-review-admin"),
            Some(json!({"approved":true})),
        )
        .await;
        let (_, unchanged) = call_as(
            &app,
            "PATCH",
            &path,
            &owner,
            json!({"community_id":community.to_string()}),
        )
        .await;
        assert_eq!(
            unchanged["data"]["approved"], true,
            "unchanged community does not reopen moderation"
        );
    }

    #[tokio::test]
    async fn a_waiting_owner_patch_reads_the_committed_moderation_under_the_same_row_lock() {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let wallet = alloy::signers::local::PrivateKeySigner::random();
        let owner = format!("{:#x}", wallet.address());
        let event = moderation::create_as(
            &pool,
            &serde_json::from_value(new_event()).unwrap(),
            &owner,
            "test",
        )
        .await
        .unwrap();
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: true,
                ..Settings::for_tests()
            },
            None,
        )));
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM explore.event WHERE id=$1 FOR UPDATE")
            .bind(&event)
            .execute(&mut *tx)
            .await
            .unwrap();
        let path = format!("/api/events/{event}");
        let mut patch = tokio::spawn(async move {
            call_as(
                &app,
                "PATCH",
                &path,
                &wallet,
                json!({"name":"Concurrent owner name"}),
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut patch)
                .await
                .is_err()
        );
        sqlx::query("UPDATE explore.event SET approved=false,raw=raw || '{\"approved\":false,\"rejected\":true,\"rejected_by\":\"concurrent-reviewer\",\"rejection_reason\":\"Locked decision\"}'::jsonb WHERE id=$1")
            .bind(&event).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let (status, result) = patch.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["data"]["name"], "Concurrent owner name");
        assert_eq!(result["data"]["rejected_by"], "concurrent-reviewer");
        assert_eq!(result["data"]["rejection_reason"], "Locked decision");
        assert_eq!(result["data"]["approved"], false);
    }

    #[tokio::test]
    async fn lists_preserve_a_bounded_future_window_and_ignore_invalid_legacy_dates() {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let id = uuid::Uuid::new_v4().to_string();
        let creator = format!(
            "{:#x}",
            alloy::signers::local::PrivateKeySigner::random().address()
        );
        let now = chrono::DateTime::<Utc>::from_timestamp(Utc::now().timestamp(), 0).unwrap();
        let expired = now - chrono::Duration::hours(2);
        let active = now - chrono::Duration::minutes(15);
        let next = now + chrono::Duration::hours(1);
        let later = next + chrono::Duration::hours(1);
        let dates = json!([
            later.to_rfc3339(),
            "not-a-date",
            expired.to_rfc3339(),
            null,
            active.to_rfc3339(),
            "infinity",
            next.to_rfc3339()
        ]);
        super::super::mirror::upsert_event(&pool, &json!({"id":id,"name":"Projection fixture","user":creator,"approved":true,"recurrent":true,"start_at":expired.to_rfc3339(),"next_start_at":expired.to_rfc3339(),"duration":3600000,"finish_at":(later+chrono::Duration::hours(1)).to_rfc3339(),"recurrent_dates":dates})).await.unwrap();
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings::for_tests(),
            None,
        )));
        let path = format!("/api/events?list=all&creator={creator}");
        let (status, list) = call(&app, "GET", &path, None, None).await;
        assert_eq!(status, StatusCode::OK, "{list}");
        let actual: Vec<i64> = list["data"][0]["recurrent_dates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|date| {
                chrono::DateTime::parse_from_rfc3339(date.as_str().unwrap())
                    .unwrap()
                    .timestamp()
            })
            .collect();
        assert_eq!(
            actual,
            vec![active.timestamp(), next.timestamp(), later.timestamp()],
            "include an in-progress occurrence, exclude expired/invalid dates, and sort future dates"
        );
        for date in list["data"][0]["recurrent_dates"].as_array().unwrap() {
            assert!(
                date.as_str().unwrap().ends_with('Z'),
                "UTC dates use the Z suffix: {date}"
            );
        }
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(
                list["data"][0]["next_start_at"].as_str().unwrap()
            )
            .unwrap()
            .timestamp(),
            active.timestamp(),
            "stale snapshot rollover still uses the complete series"
        );
        let (status, live) = call(
            &app,
            "GET",
            &format!("/api/events?list=live&creator={creator}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{live}");
        assert_eq!(live["data"][0]["id"], id);
        let (_, detail) = call(&app, "GET", &format!("/api/events/{id}"), None, None).await;
        assert_eq!(
            detail["data"]["recurrent_dates"].as_array().unwrap().len(),
            4,
            "details retain valid past dates as well"
        );
        // The first thousand stored entries are all past. Both list windows
        // must select upcoming dates before applying their output bound.
        let historical: Vec<String> = (0..1000)
            .chain((1000..2300).rev())
            .map(|index| (now + chrono::Duration::hours(index - 1000)).to_rfc3339())
            .collect();
        sqlx::query(
            "UPDATE explore.event SET raw=jsonb_set(raw,'{recurrent_dates}',$2) WHERE id=$1",
        )
        .bind(&id)
        .bind(json!(historical))
        .execute(&pool)
        .await
        .unwrap();
        let (_, standard) = call(&app, "GET", &path, None, None).await;
        let window = standard["data"][0]["recurrent_dates"].as_array().unwrap();
        assert_eq!(window.len(), 100);
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(window[0].as_str().unwrap())
                .unwrap()
                .timestamp(),
            now.timestamp()
        );
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(window[99].as_str().unwrap())
                .unwrap()
                .timestamp(),
            (now + chrono::Duration::hours(99)).timestamp()
        );
        let (_, expanded) = call(
            &app,
            "GET",
            &format!("{path}&with_recurrent_dates=true"),
            None,
            None,
        )
        .await;
        let window = expanded["data"][0]["recurrent_dates"].as_array().unwrap();
        assert_eq!(window.len(), 1000);
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(window[0].as_str().unwrap())
                .unwrap()
                .timestamp(),
            now.timestamp()
        );
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(window[999].as_str().unwrap())
                .unwrap()
                .timestamp(),
            (now + chrono::Duration::hours(999)).timestamp()
        );
        for ended_or_invalid in [
            json!([expired.to_rfc3339()]),
            json!({"invalid":"legacy recurrence document"}),
        ] {
            sqlx::query(
                "UPDATE explore.event SET raw=jsonb_set(raw,'{recurrent_dates}',$2) WHERE id=$1",
            )
            .bind(&id)
            .bind(ended_or_invalid)
            .execute(&pool)
            .await
            .unwrap();
            let (status, empty) = call(&app, "GET", &path, None, None).await;
            assert_eq!(status, StatusCode::OK, "{empty}");
            assert_eq!(
                empty["data"][0]["recurrent_dates"],
                json!([]),
                "only genuinely ended or invalid series have no upcoming dates"
            );
        }
    }

    #[tokio::test]
    async fn self_approval_cannot_clear_a_moderator_rejection_but_approve_any_can() {
        let Some(pool) = crate::db::scratch::pool(2).await else {
            return;
        };
        let wallet = alloy::signers::local::PrivateKeySigner::random();
        let owner = format!("{:#x}", wallet.address());
        let event = moderation::create_as(
            &pool,
            &serde_json::from_value(new_event()).unwrap(),
            &owner,
            "test",
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO explore.event_profile_settings (\"user\",permissions) VALUES ($1,'[\"approve_own_event\"]')").bind(&owner).execute(&pool).await.unwrap();
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: true,
                operator_writes_enabled: true,
                events_admin_token: Some("rejection-reviewer".into()),
                ..Settings::for_tests()
            },
            None,
        )));
        let path = format!("/api/events/{event}");
        let (status, approved) =
            call_as(&app, "PATCH", &path, &wallet, json!({"approved":true})).await;
        assert_eq!(status, StatusCode::OK, "{approved}");
        assert_eq!(
            approved["data"]["approved"], true,
            "self-approval still works before moderation rejection"
        );
        let (_, own) = call_as(&app, "PATCH", &path, &wallet, json!({"rejected":true})).await;
        assert_eq!(own["data"]["rejected"], true, "{own}");
        let (status, undone) =
            call_as(&app, "PATCH", &path, &wallet, json!({"approved":true})).await;
        assert_eq!(status, StatusCode::OK, "{undone}");
        assert_eq!(
            undone["data"]["rejected"], false,
            "an owner can undo their own rejection"
        );
        let (_, rejected) = call(
            &app,
            "PATCH",
            &path,
            Some("rejection-reviewer"),
            Some(json!({"rejected":true,"reason":"Moderator hold"})),
        )
        .await;
        let actor = rejected["data"]["rejected_by"].clone();
        for body in [
            json!({"approved":true}),
            json!({"rejected":false,"rejection_reason":"Forged reason"}),
        ] {
            let (status, blocked) = call_as(&app, "PATCH", &path, &wallet, body).await;
            assert_eq!(status, StatusCode::OK, "{blocked}");
            assert_eq!(blocked["data"]["approved"], false);
            assert_eq!(blocked["data"]["rejected"], true);
            assert_eq!(blocked["data"]["rejected_by"], actor);
            assert_eq!(blocked["data"]["rejection_reason"], "Moderator hold");
        }
        sqlx::query("UPDATE explore.event_profile_settings SET permissions='[\"approve_any_event\"]' WHERE \"user\"=$1").bind(&owner).execute(&pool).await.unwrap();
        let (status, cleared) =
            call_as(&app, "PATCH", &path, &wallet, json!({"approved":true})).await;
        assert_eq!(status, StatusCode::OK, "{cleared}");
        assert_eq!(cleared["data"]["approved"], true);
        assert_eq!(cleared["data"]["rejected"], false);
        assert!(cleared["data"]["rejected_by"].is_null());
        assert!(cleared["data"]["rejection_reason"].is_null());
    }

    #[tokio::test]
    async fn legacy_schedule_patches_preserve_fields_and_event_lists_apply_schedule_filter() {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        sqlx::query("INSERT INTO explore.event_profile_settings (\"user\", permissions) VALUES ($1, '[\"edit_any_schedule\"]'::jsonb) ON CONFLICT (\"user\") DO UPDATE SET permissions = EXCLUDED.permissions")
            .bind(address()).execute(&pool).await.unwrap();
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: true,
                operator_writes_enabled: true,
                events_admin_token: Some("schedule-test-admin".into()),
                ..Settings::for_tests()
            },
            None,
        )));
        let (status, schedule) = call(&app, "POST", "/api/schedules", Some("signed"), Some(json!({
            "name":"Calendar", "description":"Retained", "active_since":"2099-01-01T00:00:00Z", "active_until":"2099-12-31T00:00:00Z"
        }))).await;
        assert_eq!(status, StatusCode::OK, "{schedule}");
        let schedule_id = schedule["data"]["id"].as_str().unwrap();
        let (status, changed) = call(
            &app,
            "PATCH",
            &format!("/api/schedules/{schedule_id}"),
            Some("signed"),
            Some(json!({"name":"Renamed"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{changed}");
        assert_eq!(changed["data"]["description"], "Retained");
        assert_eq!(
            changed["data"]["active_since"],
            schedule["data"]["active_since"]
        );
        let mut chosen = new_event();
        chosen["schedules"] = json!([schedule_id]);
        let (status, chosen) =
            call(&app, "POST", "/api/events", Some("signed"), Some(chosen)).await;
        assert_eq!(status, StatusCode::OK, "{chosen}");
        let chosen_id = chosen["data"]["id"].as_str().unwrap();
        let (status, other) = call(
            &app,
            "POST",
            "/api/events",
            Some("signed"),
            Some(new_event()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{other}");
        for id in [chosen_id, other["data"]["id"].as_str().unwrap()] {
            let (status, body) = call(
                &app,
                "PATCH",
                &format!("/api/events/{id}"),
                Some("schedule-test-admin"),
                Some(json!({"approved":true})),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (status, body) = call(
            &app,
            "GET",
            &format!("/api/events?list=all&schedule={schedule_id}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"][0]["id"], chosen_id);
        let (status, body) = call(
            &app,
            "PATCH",
            "/api/schedules/missing",
            Some("signed"),
            Some(json!({"name":"No"})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[test]
    fn schedule_edits_and_content_roundtrips_do_not_require_new_moderation() {
        let before = json!({"name":"Event", "description":"text", "categories":["art","music"],"start_at":"2099-01-01T00:00:00Z"}).as_object().unwrap().clone();
        let mut after = before.clone();
        after.insert("start_at".into(), json!("2099-01-02T00:00:00Z"));
        after.insert("categories".into(), json!(["music", "art"]));
        assert!(!moderated_content_changed(&before, &after));
        after.insert("name".into(), json!("Changed"));
        assert!(moderated_content_changed(&before, &after));
    }

    /// The events write surface over the real schema: a wallet creates,
    /// edits and deletes its own event and RSVPs; another wallet may not;
    /// the admin bearer moderates; the switches answer 503; the sitemaps and
    /// the v1 reads see the result.
    #[tokio::test]
    async fn events_are_written_by_their_owner_and_moderated_by_the_admin() {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let settings = Settings {
            user_writes_enabled: true,
            operator_writes_enabled: true,
            events_admin_token: Some("events-admin".into()),
            ..Settings::for_tests()
        };
        let app = router(Arc::new(ExploreState::new(
            pool.clone(),
            settings.clone(),
            None,
        )));
        let closed = router(Arc::new(ExploreState::new(
            pool.clone(),
            Settings {
                user_writes_enabled: false,
                ..settings
            },
            None,
        )));

        let (status, _) = call(
            &closed,
            "POST",
            "/api/events",
            Some("signed"),
            Some(new_event()),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let (status, _) = call(&app, "POST", "/api/events", None, Some(new_event())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, body) = call(
            &app,
            "POST",
            "/api/events",
            Some("signed"),
            Some(json!({ "name": "x", "nope": 1 })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = call(
            &app,
            "POST",
            "/api/events",
            Some("signed"),
            Some(new_event()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let id = body["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(
            (
                body["data"]["user"].clone(),
                body["data"]["approved"].clone()
            ),
            (json!(address()), json!(false))
        );

        // the admin approves, the owner's content edit sends it back to approval
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/events/{id}"),
            Some("events-admin"),
            Some(json!({ "action": "approve", "actor": "0xmod" })),
        )
        .await;
        assert_eq!(
            (status, body["data"]["approved"].clone()),
            (StatusCode::OK, json!(true)),
            "{body}"
        );
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/events/{id}"),
            Some("signed"),
            Some(json!({ "name": "Launch party II" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            (
                body["data"]["name"].clone(),
                body["data"]["approved"].clone()
            ),
            (json!("Launch party II"), json!(false))
        );
        let (status, _) = call(
            &app,
            "PATCH",
            &format!("/api/events/{id}"),
            Some("events-admin"),
            Some(json!({ "approved": true })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // another owner's event is not editable; an unknown one is a 404
        sqlx::query("UPDATE explore.event SET raw = raw || '{\"user\": \"0x0000000000000000000000000000000000000abc\"}', user_creator = '0x0000000000000000000000000000000000000abc' WHERE id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        let (status, body) = call(
            &app,
            "PATCH",
            &format!("/api/events/{id}"),
            Some("signed"),
            Some(json!({ "name": "hijack" })),
        )
        .await;
        assert_eq!(
            (status, body["message"].clone()),
            (
                StatusCode::FORBIDDEN,
                json!("You don't have permission to edit this event")
            )
        );
        sqlx::query("UPDATE explore.event SET raw = raw || jsonb_build_object('user', $2::text), user_creator = $2 WHERE id = $1").bind(&id).bind(address()).execute(&pool).await.unwrap();
        let (status, _) = call(
            &app,
            "PATCH",
            "/api/events/00000000-0000-0000-0000-000000000000",
            Some("signed"),
            Some(json!({ "name": "x" })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, body) = call(
            &app,
            "POST",
            &format!("/api/events/{id}/attendees"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            (
                body["data"][0]["user"].clone(),
                body["data"][0]["user_name"].clone()
            ),
            (json!(address()), json!("Alice"))
        );
        let (_, body) = call(
            &app,
            "DELETE",
            &format!("/api/events/{id}/attendees"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(body["data"], json!([]));

        let (status, body) = call(&app, "GET", &format!("/v1/events/{id}"), None, None).await;
        assert_eq!(
            (status, body["data"]["id"].clone()),
            (StatusCode::OK, json!(id))
        );
        let (status, body) = call(&app, "GET", "/v1/events/not-a-uuid", None, None).await;
        assert_eq!(
            (status, body["message"].clone()),
            (StatusCode::NOT_FOUND, json!("Event not found: not-a-uuid"))
        );
        let (status, body) = call(&app, "GET", "/events/sitemap.xml", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.as_str().unwrap().contains("sitemap.events.xml?page=0"),
            "{body}"
        );
        let (_, body) = call(&app, "GET", "/events/sitemap.events.xml?page=0", None, None).await;
        assert!(
            body.as_str().unwrap().contains(&format!("/event/?id={id}")),
            "{body}"
        );
        let (_, body) = call(
            &app,
            "GET",
            "/events/sitemap.events.xml?page=00",
            None,
            None,
        )
        .await;
        assert!(
            !body.as_str().unwrap().contains("<url>"),
            "a non-canonical page number answers an empty set"
        );

        // the owner deletes; a delete again answers the deleted record
        let (status, body) = call(
            &app,
            "DELETE",
            &format!("/api/events/{id}"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(
            (status, body["data"]["deleted_by_user"].clone()),
            (StatusCode::OK, json!(true)),
            "{body}"
        );
        let (status, _) = call(
            &app,
            "DELETE",
            &format!("/api/events/{id}"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(
            &app,
            "POST",
            &format!("/api/events/{id}/attendees"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no RSVP to a deleted event");
        let overlay: String =
            sqlx::query_scalar("SELECT signer FROM explore.events_local WHERE id = $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(overlay, address());
        let (status, body) = call(&app, "GET", "/api/profiles/subscriptions", None, None).await;
        assert_eq!(status, StatusCode::GONE, "{body}");
    }

    /// A mirrored event is the upstream's: its owner's edit and delete (and
    /// an admin patch of its name) answer 409 and write nothing, so the next
    /// pass still writes it. The admin's moderation and delete are local
    /// policy the pass keeps: a rejected or deleted event leaves the public
    /// list and detail while the admin still reads it. An RSVP is a local row
    /// the count adds to the upstream's.
    #[tokio::test]
    async fn a_mirrored_event_refuses_its_owner_keeps_the_admins_moderation_and_counts_local_rsvps()
    {
        let Some(pool) = crate::db::scratch::pool(4).await else {
            return;
        };
        let settings = Settings {
            user_writes_enabled: true,
            operator_writes_enabled: true,
            events_admin_token: Some("events-admin".into()),
            ..Settings::for_tests()
        };
        let app = router(Arc::new(ExploreState::new(pool.clone(), settings, None)));
        let id = uuid::Uuid::new_v4().to_string();
        let community = uuid::Uuid::new_v4().to_string();
        let upstream = |name: &str| {
            json!({ "id": id, "name": name, "user": address(), "approved": true,
                    "community_id": community,
                    "start_at": "2099-06-01T18:00:00Z", "finish_at": "2099-06-01T19:00:00Z",
                    "duration": 3_600_000, "total_attendees": 7,
                    "latest_attendees": ["0x00000000000000000000000000000000000000a7"] })
        };
        let pass = |name: &'static str| {
            let pool = pool.clone();
            let doc = upstream(name);
            async move {
                assert!(
                    super::super::mirror::upsert_event(&pool, &doc)
                        .await
                        .unwrap()
                );
            }
        };
        pass("Upstream party").await;

        let path = format!("/api/events/{id}");
        let listed = || {
            let app = app.clone();
            let community = community.clone();
            async move {
                let (_, body) = call(
                    &app,
                    "GET",
                    &format!("/api/events?list=all&community_id={community}"),
                    None,
                    None,
                )
                .await;
                let shown = body["data"]["events"].as_array().map_or(0, Vec::len);
                assert_eq!(body["data"]["total"], json!(shown), "{body}");
                shown
            }
        };
        assert_eq!(listed().await, 1);
        let edit = json!({ "name": "renamed here" });
        let refusals = [
            call(&app, "PATCH", &path, Some("signed"), Some(edit.clone())).await,
            call(&app, "DELETE", &path, Some("signed"), None).await,
            call(
                &app,
                "PATCH",
                &path,
                Some("events-admin"),
                Some(edit.clone()),
            )
            .await,
        ];
        for (status, body) in refusals {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["code"], json!("conflict"));
            assert!(
                body["message"].as_str().unwrap().contains("mirrored from"),
                "{body}"
            );
        }
        let (name, raw, audits, overlays): (String, Value, i64, i64) = sqlx::query_as(
            "SELECT name, raw, (SELECT count(*) FROM explore.events_local WHERE id = $1), \
                    (SELECT count(*) FROM explore.event_moderation WHERE event_id = $1) \
             FROM explore.event WHERE id = $1",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            (
                name.as_str(),
                raw.get("deleted_by_user"),
                raw.get("rejected"),
                audits,
                overlays
            ),
            ("Upstream party", None, None, 0, 0)
        );

        // the admin rejects it: gone from the public reads, kept by a pass
        let (status, body) = call(
            &app,
            "PATCH",
            &path,
            Some("events-admin"),
            Some(json!({ "action": "reject" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        pass("Renamed upstream").await;
        assert_eq!(listed().await, 0, "a rejected event is not listed");
        let (status, _) = call(&app, "GET", &path, None, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = call(&app, "GET", &path, Some("events-admin"), None).await;
        assert_eq!(
            (
                status,
                body["data"]["name"].clone(),
                body["data"]["rejected"].clone()
            ),
            (StatusCode::OK, json!("Renamed upstream"), json!(true)),
            "{body}"
        );
        let (status, body) = call(
            &app,
            "PATCH",
            &path,
            Some("events-admin"),
            Some(json!({ "action": "approve" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(listed().await, 1);

        let (status, body) = call(
            &app,
            "POST",
            &format!("{path}/attendees"),
            Some("signed"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, body) = call(&app, "GET", &path, None, None).await;
        assert_eq!(
            body["data"]["total_attendees"],
            json!(8),
            "the upstream's 7 and this node's 1: {body}"
        );
        pass("Renamed again").await;
        let (_, body) = call(&app, "GET", &path, None, None).await;
        assert_eq!(
            (
                body["data"]["name"].clone(),
                body["data"]["total_attendees"].clone()
            ),
            (json!("Renamed again"), json!(8)),
            "neither the RSVP nor the moderation freezes the upstream's fields"
        );

        // the admin deletes it: hidden from every public read across passes
        let operator = "0x00000000000000000000000000000000000000ad";
        let (status, body) = call(
            &app,
            "DELETE",
            &path,
            Some("events-admin"),
            Some(json!({ "reason": "duplicate", "actor": operator })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        pass("Renamed once more").await;
        assert_eq!(listed().await, 0, "a hidden event is not listed");
        let (status, _) = call(&app, "GET", &path, None, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = call(&app, "GET", &path, Some("events-admin"), None).await;
        assert_eq!(
            (
                status,
                body["data"]["name"].clone(),
                body["data"]["deleted_by_admin"].clone(),
                body["data"]["deleted_by"].clone(),
                body["data"]["deleted_reason"].clone()
            ),
            (
                StatusCode::OK,
                json!("Renamed once more"),
                json!(true),
                json!(operator),
                json!("duplicate")
            ),
            "the console still reads it, with the delete's actor and reason: {body}"
        );
        let (hidden, approved, audits): (Option<bool>, Option<bool>, i64) = sqlx::query_as(
            "SELECT hidden, approved, (SELECT count(*) FROM explore.events_local WHERE id = $1) \
             FROM explore.event_moderation WHERE event_id = $1",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((hidden, approved, audits), (Some(true), Some(true), 0));

        sqlx::query("DELETE FROM explore.event_attendance_local WHERE event_id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM explore.event_moderation WHERE event_id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM explore.event WHERE id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[test]
    fn the_featured_item_change_is_detected() {
        let mut r = raw();
        r.insert(
            "featured_item".into(),
            json!(
                "urn:decentraland:matic:collections-v2:0x0000000000000000000000000000000000000001:1"
            ),
        );
        let same: UpdateEventBody = serde_json::from_value(json!({ "featured_item": "urn:decentraland:matic:collections-v2:0x0000000000000000000000000000000000000001:1" })).unwrap();
        let cleared: UpdateEventBody =
            serde_json::from_value(json!({ "featured_item": null })).unwrap();
        assert!(!featured_item_changed(&r, &same));
        assert!(featured_item_changed(&r, &cleared));
        assert!(!featured_item_changed(&r, &UpdateEventBody::default()));
    }
}
