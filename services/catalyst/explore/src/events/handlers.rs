//! The events read routes (`/api/events...`, `/api/schedules...`).

use axum::Json;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    EventAttendeeRecord, EventCategoryRecord, EventListFilters, EventListType, EventRecord,
    ScheduleRecord, SortOrder,
};
use crate::SharedState;
use crate::shared::errors::{ApiData, ApiError};

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub struct EventListWithTotal {
    pub events: Vec<EventRecord>,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub total: i64,
}

/// A places_ids/community_id query answers `{events, total}`; the plain list
/// answers a bare array. The Unity client parses each shape by query.
#[derive(Debug, Serialize)]
#[serde(untagged)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub enum EventListData {
    WithTotal(EventListWithTotal),
    Events(Vec<EventRecord>),
}

#[derive(Debug, Deserialize, Default)]
pub struct EventListQuery {
    pub limit: Option<String>,
    pub offset: Option<String>,
    pub list: Option<String>,
    pub order: Option<String>,
    pub highlighted: Option<String>,
    pub creator: Option<String>,
    pub world: Option<String>,
    pub world_names: Option<Vec<String>>,
    pub position: Option<String>,
    pub positions: Option<Vec<String>>,
    pub estate_id: Option<String>,
    pub community_id: Option<String>,
    pub places_ids: Option<Vec<String>>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub search: Option<String>,
    pub only_attendee: Option<String>,
    pub schedule: Option<String>,
    pub owner: Option<String>,
    pub with_connected_users: Option<String>,
    pub with_recurrent_dates: Option<String>,
    pub approved: Option<String>,
    pub rejected: Option<String>,
    pub deleted: Option<String>,
    pub allow_deleted: Option<String>,
}

impl EventListQuery {
    fn from_pairs(pairs: &[(String, String)]) -> Self {
        let mut q = EventListQuery::default();
        let (mut positions, mut world_names, mut places_ids) = (Vec::new(), Vec::new(), Vec::new());
        for (k, v) in pairs {
            match k.as_str() {
                "limit" => q.limit = Some(v.clone()),
                "offset" => q.offset = Some(v.clone()),
                "list" => q.list = Some(v.clone()),
                "order" => q.order = Some(v.clone()),
                "highlighted" => q.highlighted = Some(v.clone()),
                "creator" => q.creator = Some(v.clone()),
                "world" => q.world = Some(v.clone()),
                "position" => q.position = Some(v.clone()),
                "estate_id" => q.estate_id = Some(v.clone()),
                "community_id" => q.community_id = Some(v.clone()),
                "from" => q.from = Some(v.clone()),
                "to" => q.to = Some(v.clone()),
                "search" => q.search = Some(v.clone()),
                "only_attendee" => q.only_attendee = Some(v.clone()),
                "schedule" => q.schedule = Some(v.clone()),
                "owner" => q.owner = Some(v.clone()),
                "with_connected_users" => q.with_connected_users = Some(v.clone()),
                "with_recurrent_dates" => q.with_recurrent_dates = Some(v.clone()),
                "approved" => q.approved = Some(v.clone()),
                "rejected" => q.rejected = Some(v.clone()),
                "deleted" => q.deleted = Some(v.clone()),
                "allow_deleted" => q.allow_deleted = Some(v.clone()),
                "positions" | "positions[]" => positions.push(v.clone()),
                "world_names" | "world_names[]" => world_names.push(v.clone()),
                "places_ids" | "places_ids[]" => places_ids.push(v.clone()),
                _ => {}
            }
        }
        if !positions.is_empty() {
            q.positions = Some(positions);
        }
        if !world_names.is_empty() {
            q.world_names = Some(world_names);
        }
        if !places_ids.is_empty() {
            q.places_ids = Some(places_ids);
        }
        q
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

fn parse_position(s: &str) -> Option<(i32, i32)> {
    let (x, y) = crate::auth::parse_pointer(s)?;
    Some((i32::try_from(x).ok()?, i32::try_from(y).ok()?))
}

fn parse_filters(
    q: &EventListQuery,
    body_place_ids: Vec<String>,
    body_community_id: Option<String>,
    user: Option<String>,
    admin: bool,
    include_unapproved: bool,
) -> Result<Option<EventListFilters>, ApiError> {
    let limit = q
        .limit
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|n| n.clamp(0, 500))
        .unwrap_or(500);
    if limit == 0 {
        return Ok(None);
    }
    let offset = q
        .offset
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|n| n.max(0))
        .unwrap_or(0);
    let mut list = match q.list.as_deref() {
        Some("all") => EventListType::All,
        Some("live") => EventListType::Live,
        Some("upcoming") => EventListType::Upcoming,
        Some("relevance") => EventListType::Relevance,
        _ => EventListType::Active,
    };
    let order = match q.order.as_deref() {
        Some("desc") => SortOrder::Desc,
        Some("asc") => SortOrder::Asc,
        _ if q.search.is_some() => SortOrder::Desc,
        _ => SortOrder::Asc,
    };
    let mut highlighted = q.highlighted.as_deref().and_then(parse_bool);
    if matches!(q.list.as_deref(), Some("highlight")) {
        highlighted = Some(true);
        list = EventListType::Active;
    }

    let mut positions: Vec<(i32, i32)> = Vec::new();
    if let Some(p) = &q.position {
        match parse_position(p) {
            Some(pos) if is_inside_world_limits(pos.0, pos.1) => positions.push(pos),
            Some(_) => return Ok(None),
            None => return Err(ApiError::bad_request("invalid position")),
        }
    }
    if let Some(ps) = &q.positions {
        for s in ps {
            match parse_position(s) {
                Some(pos) if is_inside_world_limits(pos.0, pos.1) => positions.push(pos),
                Some(_) => return Ok(None),
                None => return Err(ApiError::bad_request("invalid position in positions[]")),
            }
        }
    }

    let mut places_ids = q.places_ids.clone().unwrap_or_default();
    places_ids.extend(body_place_ids);
    let community_id = q.community_id.clone().or(body_community_id);

    let from = q
        .from
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));
    let to =
        q.to.as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc));

    let search = match q.search.as_deref() {
        Some(s) if !has_three_word_chars(s) => return Ok(None),
        Some(s) => Some(s.to_string()),
        None => None,
    };

    let owner = resolve_owner(q);
    let creator = if owner {
        None
    } else {
        match q.creator.as_deref() {
            Some(c) if is_ethereum_address(c) => Some(c.to_lowercase()),
            Some(_) => return Ok(None),
            None => None,
        }
    };

    let estate_id = match q.estate_id.as_deref() {
        Some(e) if !e.is_empty() => {
            if e.parse::<f64>().map(|n| n.is_finite()).unwrap_or(false) {
                Some(e.to_string())
            } else {
                return Ok(None);
            }
        }
        _ => None,
    };

    Ok(Some(EventListFilters {
        limit,
        offset,
        list,
        order,
        highlighted,
        creator,
        world: q.world.as_deref().and_then(parse_bool),
        world_names: q.world_names.clone().unwrap_or_default(),
        positions,
        estate_id,
        community_id,
        schedule: q.schedule.clone(),
        with_recurrent_dates: q
            .with_recurrent_dates
            .as_deref()
            .and_then(parse_bool)
            .unwrap_or(false),
        places_ids,
        destination_ids: Vec::new(),
        from,
        to,
        search,
        user: user.clone(),
        rejected: if admin {
            q.rejected.as_deref().and_then(parse_bool)
        } else {
            None
        },
        approved: if admin {
            q.approved.as_deref().and_then(parse_bool)
        } else {
            None
        },
        deleted: if admin {
            q.deleted.as_deref().and_then(parse_bool)
        } else {
            None
        },
        admin,
        only_attendee: resolve_only_attendee(q) && user.is_some(),
        owner,
        include_own: true,
        include_deleted: admin
            && q.allow_deleted
                .as_deref()
                .and_then(parse_bool)
                .unwrap_or(false),
        include_unapproved,
    }))
}

fn has_three_word_chars(s: &str) -> bool {
    let mut run = 0;
    for c in s.chars() {
        if c.is_alphanumeric() || c == '_' {
            run += 1;
            if run >= 3 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

fn is_ethereum_address(s: &str) -> bool {
    let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) else {
        return false;
    };
    hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) fn is_inside_world_limits(x: i32, y: i32) -> bool {
    const RANGES: [(i32, i32, i32, i32); 4] = [
        (-150, -150, 150, 150),
        (62, 151, 162, 158),
        (151, 144, 162, 150),
        (151, 59, 163, 143),
    ];
    RANGES.iter().any(|(x_min, y_min, x_max, y_max)| {
        x >= *x_min && x <= *x_max && y >= *y_min && y <= *y_max
    })
}

fn resolve_only_attendee(q: &EventListQuery) -> bool {
    match q.only_attendee.as_deref() {
        Some(v) => parse_bool(v).unwrap_or(true),
        None => false,
    }
}

fn resolve_owner(q: &EventListQuery) -> bool {
    q.owner.as_deref().and_then(parse_bool).unwrap_or(false)
}

pub(crate) fn is_admin_request(headers: &HeaderMap, state: &SharedState) -> bool {
    crate::auth::admin_bearer::matches(state.settings.events_admin_token.as_deref(), headers)
}

pub(crate) async fn viewer_is_admin(
    state: &SharedState,
    headers: &HeaderMap,
    viewer: Option<&str>,
) -> Result<bool, ApiError> {
    if is_admin_request(headers, state) {
        return Ok(true);
    }
    match viewer {
        Some(v) => state.events.viewer_is_moderator(v).await,
        None => Ok(false),
    }
}

async fn list_with(
    state: &SharedState,
    headers: &HeaderMap,
    method: &str,
    signed_path: &str,
    q: &EventListQuery,
    body_place_ids: Vec<String>,
    body_community_id: Option<String>,
) -> Result<Json<ApiData<EventListData>>, ApiError> {
    let user = state.viewer(headers, method, signed_path).await;
    if q.only_attendee.is_some() && user.is_none() {
        return Err(ApiError::unauthorized(
            "only_attendee filter requieres autentication",
        ));
    }
    if resolve_owner(q) && user.is_none() {
        return Err(ApiError::unauthorized(
            "owner filter requires authentication",
        ));
    }
    let admin = is_admin_request(headers, state);
    let include_unapproved = viewer_is_admin(state, headers, user.as_deref()).await?;
    let Some(filters) = parse_filters(
        q,
        body_place_ids,
        body_community_id,
        user,
        admin,
        include_unapproved,
    )?
    else {
        return Ok(Json(ApiData::ok(EventListData::Events(Vec::new()))));
    };
    let envelope_with_total = !filters.places_ids.is_empty() || filters.community_id.is_some();
    let (mut events, total) = state.events.query(&filters, envelope_with_total).await?;
    if q.with_connected_users
        .as_deref()
        .and_then(parse_bool)
        .unwrap_or(false)
    {
        fill_connected(&mut events, &state.live).await;
    }
    let data = if envelope_with_total {
        EventListData::WithTotal(EventListWithTotal { events, total })
    } else {
        EventListData::Events(events)
    };
    Ok(Json(ApiData::ok(data)))
}

pub(super) async fn fill_connected(
    events: &mut [EventRecord],
    live: &std::sync::Arc<crate::places::live::Live>,
) {
    let keys = events
        .iter()
        .map(|event| connected_key(event.world, event.server.as_deref(), event.x, event.y))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let connected = live.connected_by_key(keys).await;
    for e in events {
        let key = connected_key(e.world, e.server.as_deref(), e.x, e.y);
        // Legacy events clients expect an array; a failed/unknown room
        // lookup keeps the upstream's empty-array fallback.
        e.connected_addresses = Some(connected.get(&key).cloned().flatten().unwrap_or_default());
    }
}

fn connected_key(world: bool, server: Option<&str>, x: i32, y: i32) -> (bool, String) {
    match server.filter(|server| world && !server.is_empty()) {
        Some(server) => (true, server.to_ascii_lowercase()),
        None => (false, format!("{x},{y}")),
    }
}

pub async fn get_event_list(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiData<EventListData>>, ApiError> {
    let q = EventListQuery::from_pairs(&pairs);
    list_with(&state, &headers, "get", uri.path(), &q, Vec::new(), None).await
}

#[derive(Debug, Deserialize, Default)]
pub struct EventSearchBody {
    #[serde(default, rename = "placeIds")]
    pub place_ids: Vec<String>,
    #[serde(default, rename = "communityId")]
    pub community_id: Option<String>,
}

pub async fn post_event_search(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
    Json(body): Json<EventSearchBody>,
) -> Result<Json<ApiData<EventListData>>, ApiError> {
    let q = EventListQuery::from_pairs(&pairs);
    list_with(
        &state,
        &headers,
        "post",
        uri.path(),
        &q,
        body.place_ids,
        body.community_id,
    )
    .await
}

fn is_soft_deleted(evt: &EventRecord) -> bool {
    evt.deleted_by_user || evt.deleted_by_admin || evt.deleted_at.is_some()
}

pub(crate) fn visible_to_viewer(evt: &EventRecord, viewer: Option<&str>, admin: bool) -> bool {
    if admin {
        return true;
    }
    let owner =
        matches!((viewer, evt.user.as_deref()), (Some(v), Some(u)) if v.eq_ignore_ascii_case(u));
    if owner {
        return true;
    }
    !is_soft_deleted(evt) && evt.approved && !evt.rejected
}

pub async fn get_event(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Json<ApiData<EventRecord>>, ApiError> {
    let viewer = state.viewer(&headers, "get", uri.path()).await;
    let admin = viewer_is_admin(&state, &headers, viewer.as_deref()).await?;
    let evt = state
        .events
        .get_for_viewer(&event_id, viewer.as_deref())
        .await?
        .filter(|evt| visible_to_viewer(evt, viewer.as_deref(), admin))
        .ok_or_else(|| ApiError::not_found(format!("Not found event \"{event_id}\"")))?;
    Ok(Json(ApiData::ok(evt)))
}

pub async fn get_attending_event_list(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<ApiData<Vec<EventRecord>>>, ApiError> {
    let user = state
        .viewer(&headers, "get", uri.path())
        .await
        .ok_or_else(|| ApiError::unauthorized("Unauthorized"))?;
    Ok(Json(ApiData::ok(state.events.attending(&user).await?)))
}

pub async fn get_moderation_list(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiData<Vec<EventRecord>>>, ApiError> {
    if !is_admin_request(&headers, &state) {
        return Err(ApiError::forbidden(
            if state.settings.events_admin_token.is_none() {
                "Admin token not configured"
            } else {
                "Invalid admin credentials"
            },
        ));
    }
    let limit = pairs
        .iter()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<i64>().ok())
        .map(|n| n.clamp(0, 500))
        .unwrap_or(24);
    Ok(Json(ApiData::ok(
        state.events.moderation_pending(limit).await?,
    )))
}

pub async fn get_event_category_list() -> Json<ApiData<Vec<EventCategoryRecord>>> {
    Json(ApiData::ok(super::categories::list()))
}

pub async fn get_event_attendees(
    State(state): State<SharedState>,
    Path(event_id): Path<String>,
) -> Result<Json<ApiData<Vec<EventAttendeeRecord>>>, ApiError> {
    let list = super::attendees::list_for_event(&state.pool, &event_id).await?;
    Ok(Json(ApiData::ok(list)))
}

pub async fn get_schedule_list(
    State(state): State<SharedState>,
) -> Result<Json<ApiData<Vec<ScheduleRecord>>>, ApiError> {
    Ok(Json(ApiData::ok(state.schedules.list().await?)))
}

pub async fn get_schedule_by_id(
    State(state): State<SharedState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<ApiData<ScheduleRecord>>, ApiError> {
    let s = state
        .schedules
        .get(&schedule_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Schedule \"{schedule_id}\" not found")))?;
    Ok(Json(ApiData::ok(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A community_id query selects the WithTotal envelope. The Unity client
    /// (EventWithPlaceIdDTOListResponse) parses exactly
    /// `{"ok":true,"data":{"events":[...],"total":n}}` for this query, so the
    /// empty case must serialize to that byte sequence -- the untagged
    /// EventListData must not collapse to a bare array.
    #[test]
    fn envelopes_follow_the_query_shape() {
        let pairs = vec![
            (
                "community_id".to_string(),
                "11111111-2222-3333-4444-555555555555".to_string(),
            ),
            ("limit".to_string(), "10".to_string()),
        ];
        let q = EventListQuery::from_pairs(&pairs);
        let filters = parse_filters(
            &q,
            Vec::new(),
            None,
            Some("0xabc".to_string()),
            false,
            false,
        )
        .unwrap()
        .unwrap();
        assert!(filters.community_id.is_some());
        let body = ApiData::ok(EventListData::WithTotal(EventListWithTotal {
            events: Vec::new(),
            total: 0,
        }));
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"ok":true,"data":{"events":[],"total":0}}"#
        );

        let q = EventListQuery::from_pairs(&[("limit".to_string(), "10".to_string())]);
        let filters = parse_filters(&q, Vec::new(), None, None, false, false)
            .unwrap()
            .unwrap();
        assert!(filters.places_ids.is_empty() && filters.community_id.is_none());
        let body = ApiData::ok(EventListData::Events(Vec::new()));
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"ok":true,"data":[]}"#
        );
    }

    #[test]
    fn filters_follow_upstream_parsing_rules() {
        let q = EventListQuery::from_pairs(&[
            ("list".to_string(), "highlight".to_string()),
            ("position".to_string(), "10,-20".to_string()),
            ("positions[]".to_string(), "1,1".to_string()),
            (
                "creator".to_string(),
                "0xF39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
            ),
            ("estate_id".to_string(), "6112".to_string()),
            ("search".to_string(), "party".to_string()),
            ("approved".to_string(), "false".to_string()),
        ]);
        let f = parse_filters(&q, vec!["p1".into()], None, None, false, false)
            .unwrap()
            .unwrap();
        assert_eq!(f.list, EventListType::Active);
        assert_eq!(f.highlighted, Some(true));
        assert_eq!(f.positions, vec![(10, -20), (1, 1)]);
        assert_eq!(
            f.creator.as_deref(),
            Some("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266")
        );
        assert_eq!(f.estate_id.as_deref(), Some("6112"));
        assert_eq!(
            f.order,
            SortOrder::Desc,
            "a search sorts by rank descending"
        );
        assert_eq!(f.places_ids, vec!["p1"]);
        assert_eq!(f.approved, None, "status filters are admin-only");
        assert!(!f.only_attendee);

        let q = EventListQuery::from_pairs(&[("limit".to_string(), "0".to_string())]);
        assert!(
            parse_filters(&q, Vec::new(), None, None, false, false)
                .unwrap()
                .is_none()
        );
        let q = EventListQuery::from_pairs(&[("position".to_string(), "9999,0".to_string())]);
        assert!(
            parse_filters(&q, Vec::new(), None, None, false, false)
                .unwrap()
                .is_none()
        );
        let q = EventListQuery::from_pairs(&[("position".to_string(), "abc".to_string())]);
        assert!(parse_filters(&q, Vec::new(), None, None, false, false).is_err());
        let q = EventListQuery::from_pairs(&[("search".to_string(), "ab".to_string())]);
        assert!(
            parse_filters(&q, Vec::new(), None, None, false, false)
                .unwrap()
                .is_none()
        );
        let q =
            EventListQuery::from_pairs(&[("creator".to_string(), "not-an-address".to_string())]);
        assert!(
            parse_filters(&q, Vec::new(), None, None, false, false)
                .unwrap()
                .is_none()
        );

        let q = EventListQuery::from_pairs(&[
            ("owner".to_string(), "true".to_string()),
            (
                "creator".to_string(),
                "0xF39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
            ),
            ("only_attendee".to_string(), "".to_string()),
            ("allow_deleted".to_string(), "true".to_string()),
        ]);
        let f = parse_filters(&q, Vec::new(), None, Some("0xabc".into()), true, true)
            .unwrap()
            .unwrap();
        assert!(f.owner && f.creator.is_none() && f.only_attendee && f.include_deleted && f.admin);
    }

    #[test]
    fn world_limits_and_admin_bearer_comparison() {
        assert!(is_inside_world_limits(0, 0));
        assert!(is_inside_world_limits(-150, 150));
        assert!(is_inside_world_limits(160, 155));
        assert!(is_inside_world_limits(151, 151));
        assert!(!is_inside_world_limits(151, 159));
        assert!(!is_inside_world_limits(200, 0));
    }
}
