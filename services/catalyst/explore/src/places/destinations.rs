//! `/api/destinations`: the explorer's unified places + worlds feed, with the
//! content gate, the curated ordering and the optional live-event / next-event
//! decorations. The old service asked the events service over HTTP for those;
//! here they are one query over `explore.event` (`EventsComponent`).
//!
//! Live occupancy and connected wallets (`places::live`): `most_active`
//! orders by the presence collector's latest headcounts, `user_count` is
//! that headcount (the mirrored figure while no snapshot exists), and
//! `with_connected_users` lists the wallets in the room from the comms
//! module (`null`, "unknown", without it; an empty array is an empty room).

use std::collections::HashMap;

use super::live;
use axum::Json;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, Method};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use super::{PlaceListFilters, PlaceOrderBy, PlaceRow};
use crate::events::NextEvent;
use crate::shared::errors::{ApiDataTotal, ApiError};
use crate::{ExploreState, SharedState};

pub const MAX_BATCH_ITEMS: usize = 1000;
/// places.decentraland.org's default and cap: `limit=500` still answers 100.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub enum EntityType {
    Place,
    World,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct DestinationNextEvent {
    pub id: String,
    pub name: String,
    pub next_start_at: String,
}

impl From<NextEvent> for DestinationNextEvent {
    fn from(e: NextEvent) -> Self {
        Self {
            id: e.id,
            name: e.name,
            next_start_at: e.next_start_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct Destination {
    pub id: String,
    pub kind: EntityType,
    pub title: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub owner: Option<String>,
    pub positions: Vec<String>,
    pub base_position: String,
    pub contact_name: Option<String>,
    pub contact_email: Option<String>,
    pub content_rating: Option<String>,
    pub disabled: bool,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub disabled_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub created_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub updated_at: Option<DateTime<Utc>>,
    pub favorites: i32,
    pub likes: i32,
    pub dislikes: i32,
    pub categories: Vec<String>,
    pub highlighted: bool,
    pub highlighted_image: Option<String>,
    pub ranking: Option<f64>,
    pub sdk: Option<String>,
    pub creator_address: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub deployed_at: Option<DateTime<Utc>>,
    pub world: bool,
    pub world_name: Option<String>,
    pub is_private: bool,
    pub user_favorite: bool,
    pub user_like: bool,
    pub user_dislike: bool,
    pub user_count: Option<i32>,
    pub user_visits: i32,
    pub like_rate: Option<f64>,
    pub like_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub live: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub live_event: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional, type = "string | null"))]
    pub live_event_name: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "ts",
        ts(
            optional,
            type = "{ id: string, name: string, next_start_at: string } | null"
        )
    )]
    pub next_event: Option<Option<DestinationNextEvent>>,
    /// Wallet addresses connected here, present only when with_connected_users=true:
    /// an empty array means nobody is connected, null means the comms service could
    /// not be reached and the connected users are unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional, type = "Array<string> | null"))]
    pub connected_addresses: Option<Option<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional, type = "Array<Record<string, unknown>>"))]
    pub realms_detail: Option<Vec<serde_json::Value>>,
}

impl From<PlaceRow> for Destination {
    fn from(p: PlaceRow) -> Self {
        let kind = if p.world && !p.scene_place {
            EntityType::World
        } else {
            EntityType::Place
        };
        Self {
            id: p.id,
            kind,
            title: p.title,
            description: p.description,
            image: p.image,
            owner: p.owner,
            positions: p.positions,
            base_position: p.base_position,
            contact_name: p.contact_name,
            contact_email: p.contact_email,
            content_rating: p.content_rating,
            disabled: p.disabled,
            disabled_at: p.disabled_at,
            created_at: p.created_at,
            updated_at: p.updated_at,
            favorites: p.favorites,
            likes: p.likes,
            dislikes: p.dislikes,
            categories: p.categories,
            highlighted: p.highlighted,
            highlighted_image: p.highlighted_image,
            ranking: p.ranking,
            sdk: p.sdk,
            creator_address: p.creator_address,
            deployed_at: p.deployed_at,
            world: p.world,
            world_name: p.world_name,
            is_private: p.is_private,
            user_favorite: p.user_favorite,
            user_like: p.user_like,
            user_dislike: p.user_dislike,
            user_count: p.user_count,
            user_visits: p.user_visits,
            like_rate: p.like_rate,
            like_score: p.like_score,
            live: p.live,
            live_event: p.live,
            live_event_name: p.live_event_name,
            next_event: None,
            connected_addresses: p.connected_addresses,
            realms_detail: p.realms_detail,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DestinationFlags {
    pub with_realms_detail: bool,
    pub with_connected_users: bool,
    pub with_live_events: bool,
    pub with_next_event: bool,
}

fn present(s: &Option<String>) -> bool {
    s.as_deref().is_some_and(|v| !v.is_empty())
}

fn is_lookup_query(f: &PlaceListFilters) -> bool {
    present(&f.search)
        || !f.ids.is_empty()
        || f.viewer_favorites_only
        || f.owner_filtered
        || present(&f.creator_address)
        || f.only_highlighted
}

fn owner_param(pairs: &[(String, String)]) -> Option<&str> {
    pairs
        .iter()
        .find(|(k, _)| k == "owner")
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

fn require_feed_content(f: &mut PlaceListFilters) {
    let lookup = is_lookup_query(f);
    let names_parcels = f.positions.iter().any(|p| !p.is_empty());
    let names_worlds = f.names.iter().any(|n| !n.is_empty());
    f.require_content_places = !lookup && !names_parcels;
    f.require_content_worlds = !lookup && !names_worlds;
}

fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn truthy(pairs: &[(String, String)], key: &str) -> bool {
    matches!(get(pairs, key), Some("true") | Some("1"))
}

fn multi(pairs: &[(String, String)], key: &str) -> Result<Vec<String>, ApiError> {
    let values: Vec<String> = pairs
        .iter()
        .filter(|(k, _)| k == key)
        .flat_map(|(_, v)| v.split(','))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect();
    if values.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(format!(
            "Too many {key} (max {MAX_BATCH_ITEMS})"
        )));
    }
    Ok(values)
}

fn positions_param(pairs: &[(String, String)], key: &str) -> Result<Vec<String>, ApiError> {
    let values: Vec<String> = pairs
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
        .collect();
    if values.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(format!(
            "Too many {key} (max {MAX_BATCH_ITEMS})"
        )));
    }
    Ok(values)
}

fn int_param(pairs: &[(String, String)], key: &str) -> Option<i64> {
    get(pairs, key)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n >= 0)
}

pub fn parse_with_options(pairs: &[(String, String)]) -> Result<DestinationFlags, ApiError> {
    let with = multi(pairs, "with")?;
    let has = |name: &str| with.iter().any(|w| w == name);
    Ok(DestinationFlags {
        with_live_events: truthy(pairs, "with_live_events") || has("live_events"),
        with_connected_users: truthy(pairs, "with_connected_users") || has("connected_users"),
        with_next_event: has("next_event"),
        with_realms_detail: truthy(pairs, "with_realms_detail") || has("realms_detail"),
    })
}

fn select_branches(f: &mut PlaceListFilters, kinds: &[String]) {
    let wants_place = kinds.is_empty() || kinds.iter().any(|k| k == "place");
    let wants_world = kinds.is_empty() || kinds.iter().any(|k| k == "world");
    let excludes_worlds = !f.positions.is_empty() && f.names.is_empty();
    let excludes_places = !f.names.is_empty() && f.positions.is_empty();
    let place_branch = wants_place && !excludes_places;
    let world_branch = wants_world && !excludes_worlds;
    f.only_places = !world_branch;
    f.only_worlds = !place_branch;
}

fn no_branch(f: &PlaceListFilters) -> bool {
    f.only_places && f.only_worlds
}

pub(crate) fn parse_filters(
    pairs: &[(String, String)],
) -> Result<(PlaceListFilters, bool, DestinationFlags), ApiError> {
    let only_favorites = truthy(pairs, "only_favorites");
    let limit = int_param(pairs, "limit")
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);
    let offset = int_param(pairs, "offset").unwrap_or(0);
    let mut kinds: Vec<String> = multi(pairs, "kinds")?
        .into_iter()
        .filter(|k| k == "place" || k == "world")
        .collect();
    if truthy(pairs, "only_worlds") {
        kinds = vec!["world".to_string()];
    } else if truthy(pairs, "only_places") {
        kinds = vec!["place".to_string()];
    }
    let mut positions = positions_param(pairs, "positions")?;
    if positions.is_empty() {
        positions = positions_param(pairs, "pointer")?;
    }
    let mut names = multi(pairs, "world_names")?;
    if names.is_empty() {
        names = multi(pairs, "names")?;
    }
    let sdk = if truthy(pairs, "only_sdk7") {
        Some("7".to_string())
    } else {
        get(pairs, "sdk").map(str::to_string)
    };
    let mut f = PlaceListFilters {
        limit,
        offset,
        positions,
        categories: multi(pairs, "categories")?,
        names,
        ids: multi(pairs, "ids")?
            .into_iter()
            .map(|id| id.to_lowercase())
            .collect(),
        only_highlighted: truthy(pairs, "only_highlighted"),
        search: get(pairs, "search").map(str::to_string),
        creator_address: get(pairs, "creator_address").map(|s| s.to_lowercase()),
        sdk,
        order_by: PlaceOrderBy::parse(get(pairs, "order_by")),
        order_desc: !matches!(get(pairs, "order"), Some("asc")),
        destinations_mode: true,
        ..Default::default()
    };
    select_branches(&mut f, &kinds);
    Ok((f, only_favorites, parse_with_options(pairs)?))
}

/// The key the events catalogue knows a destination by: its world name for a
/// world, its place id otherwise.
fn event_key(d: &PlaceRow) -> String {
    if d.world {
        d.world_name.clone().unwrap_or_else(|| d.id.clone())
    } else {
        d.id.clone()
    }
}

pub(crate) async fn enrich(state: &SharedState, data: &mut [PlaceRow], flags: &DestinationFlags) {
    enrich_with_counts(state, data, flags, None).await
}

async fn enrich_with_counts(
    state: &ExploreState,
    data: &mut [PlaceRow],
    flags: &DestinationFlags,
    counts: Option<live::LiveUserCounts>,
) {
    if data.is_empty() {
        return;
    }
    let counts = match counts {
        Some(c) => c,
        None => state.live.live_user_counts().await,
    };
    live::fill_user_counts(data, &counts);
    if flags.with_connected_users {
        live::fill_connected(&state.live, data).await;
    }
    if flags.with_live_events {
        let ids: Vec<String> = data.iter().map(event_key).collect();
        let live_map: HashMap<String, Option<String>> =
            match state.events.live_event_names(&ids).await {
                Ok(map) => map,
                Err(e) => {
                    tracing::warn!(error = %e, "destinations: live event lookup failed");
                    HashMap::new()
                }
            };
        for d in data.iter_mut() {
            let event_name = live_map.get(&event_key(d)).cloned().unwrap_or(None);
            d.live = Some(event_name.is_some());
            d.live_event_name = Some(event_name);
        }
    }
    if flags.with_realms_detail {
        for d in data.iter_mut() {
            d.apply_realms_detail(Some(state.settings.realm_name.as_str()));
        }
    }
}

fn destination_event_key(d: &Destination) -> String {
    if d.world {
        d.world_name.clone().unwrap_or_else(|| d.id.clone())
    } else {
        d.id.clone()
    }
}

pub(crate) async fn decorate_next_event(
    state: &ExploreState,
    data: &mut [Destination],
    flags: &DestinationFlags,
) {
    if !flags.with_next_event || data.is_empty() {
        return;
    }
    let ids: Vec<String> = data.iter().map(destination_event_key).collect();
    let next = match state.events.next_events(&ids).await {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!(error = %e, "destinations: next event lookup failed");
            HashMap::new()
        }
    };
    for d in data.iter_mut() {
        d.next_event = Some(
            next.get(&destination_event_key(d))
                .cloned()
                .map(DestinationNextEvent::from),
        );
    }
}

fn empty() -> Json<ApiDataTotal<Destination>> {
    Json(ApiDataTotal::ok(vec![], 0))
}

async fn run_list(
    state: &ExploreState,
    filters: PlaceListFilters,
    flags: &DestinationFlags,
) -> Result<Json<ApiDataTotal<Destination>>, ApiError> {
    if no_branch(&filters) {
        return Ok(empty());
    }
    let (out, total) = list_page(state, filters, flags).await?;
    Ok(Json(ApiDataTotal::ok(out, total)))
}

async fn list_page(
    state: &ExploreState,
    mut filters: PlaceListFilters,
    flags: &DestinationFlags,
) -> Result<(Vec<Destination>, i64), ApiError> {
    require_feed_content(&mut filters);
    let counts = live::inject_live_user_counts(&state.live, &mut filters).await;
    let (mut data, total) = state.places.list_page(&filters).await?;
    enrich_with_counts(state, &mut data, flags, counts).await;
    let mut out: Vec<Destination> = data.into_iter().map(Destination::from).collect();
    decorate_next_event(state, &mut out, flags).await;
    Ok((out, total))
}

pub(crate) async fn list_destinations(
    state: &SharedState,
    headers: &HeaderMap,
    method: &str,
    signed_path: &str,
    pairs: &[(String, String)],
) -> Result<Json<ApiDataTotal<Destination>>, ApiError> {
    let (mut filters, only_favorites, flags) = parse_filters(pairs)?;
    let user = state.viewer(headers, method, signed_path).await;
    if !state
        .places
        .scope_to_viewer(&mut filters, user.as_deref(), only_favorites)
    {
        return Ok(empty());
    }
    if let Some(owner) = owner_param(pairs) {
        filters.owner_filtered = true;
        filters.operated_positions = state.places.operated_positions(owner).await?;
    }
    run_list(state, filters, &flags).await
}

pub async fn get_destinations_list(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiDataTotal<Destination>>, ApiError> {
    list_destinations(&state, &headers, method.as_str(), uri.path(), &pairs).await
}

fn bounded_list(value: Option<&Value>) -> Result<Option<Vec<String>>, ApiError> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Ok(None);
    };
    if items.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(format!(
            "Too many items (max {MAX_BATCH_ITEMS})"
        )));
    }
    Ok(Some(
        items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
    ))
}

struct BatchSelectors {
    ids: Vec<String>,
    positions: Vec<String>,
    world_names: Vec<String>,
}

fn parse_batch_body(body: &Value) -> Result<Option<BatchSelectors>, ApiError> {
    let object;
    let body = if body.is_array() {
        object = serde_json::json!({ "ids": body });
        &object
    } else {
        body
    };
    let ids = bounded_list(body.get("ids"))?;
    let positions = bounded_list(body.get("positions"))?;
    let world_names = bounded_list(body.get("world_names"))?;
    let provided = ids.is_some() || positions.is_some() || world_names.is_some();
    let ids = ids.unwrap_or_default();
    let positions = positions.unwrap_or_default();
    let world_names = world_names.unwrap_or_default();
    if provided && ids.is_empty() && positions.is_empty() && world_names.is_empty() {
        return Ok(None);
    }
    Ok(Some(BatchSelectors {
        ids: ids.into_iter().map(|id| id.to_lowercase()).collect(),
        positions,
        world_names,
    }))
}

pub async fn post_destinations_list_by_id(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
    body: Option<Json<Value>>,
) -> Result<Json<ApiDataTotal<Destination>>, ApiError> {
    let body = body
        .map(|Json(v)| v)
        .unwrap_or_else(|| Value::Object(Default::default()));
    let Some(selectors) = parse_batch_body(&body)? else {
        return Ok(empty());
    };
    let flags = parse_with_options(&pairs)?;
    let user = state.viewer(&headers, method.as_str(), uri.path()).await;
    run_list(&state, by_selectors(selectors, user), &flags).await
}

fn by_selectors(selectors: BatchSelectors, viewer: Option<String>) -> PlaceListFilters {
    let mut filters = PlaceListFilters {
        limit: MAX_LIMIT,
        ids: selectors.ids,
        positions: selectors.positions,
        names: selectors.world_names,
        destinations_mode: true,
        ..Default::default()
    };
    select_branches(&mut filters, &[]);
    filters.viewer = viewer.map(|u| u.to_lowercase());
    filters
}

/// `POST /api/destinations` with an array body, for an in-process caller:
/// the ids lowercased and matched by id only, the default flags, an
/// anonymous viewer and at most `MAX_LIMIT` rows. As that route answers:
/// no ids, no rows (not every destination); more than `MAX_BATCH_ITEMS`, 400.
pub async fn destinations_by_ids(
    state: &ExploreState,
    ids: Vec<String>,
) -> Result<Vec<Destination>, ApiError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    if ids.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(format!(
            "Too many items (max {MAX_BATCH_ITEMS})"
        )));
    }
    let selectors = BatchSelectors {
        ids: ids.into_iter().map(|id| id.to_lowercase()).collect(),
        positions: Vec::new(),
        world_names: Vec::new(),
    };
    let filters = by_selectors(selectors, None);
    Ok(list_page(state, filters, &DestinationFlags::default())
        .await?
        .0)
}

#[cfg(test)]
mod feed_gate_tests {
    use super::*;

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn gated(list: &[(&str, &str)]) -> (bool, bool) {
        let pairs = pairs(list);
        let (mut f, _, _) = parse_filters(&pairs).unwrap();
        f.owner_filtered = owner_param(&pairs).is_some();
        require_feed_content(&mut f);
        (f.require_content_places, f.require_content_worlds)
    }

    #[test]
    fn the_generic_feed_gates_both_branches_and_lookups_exempt_them() {
        assert_eq!(gated(&[]), (true, true));
        assert_eq!(gated(&[("only_places", "true")]), (true, true));
        assert_eq!(gated(&[("order_by", "most_active")]), (true, true));
        assert_eq!(gated(&[("categories", "art")]), (true, true));
        assert_eq!(gated(&[("sdk", "7")]), (true, true));
        assert_eq!(gated(&[("search", "junk")]), (false, false));
        assert_eq!(gated(&[("creator_address", "0xABC")]), (false, false));
        assert_eq!(gated(&[("only_highlighted", "true")]), (false, false));
        assert_eq!(gated(&[("owner", "0xABC")]), (false, false));
        assert_eq!(gated(&[("ids", "uuid-1")]), (false, false));
    }

    #[test]
    fn empty_lookup_values_do_not_count_and_selectors_exempt_their_own_branch() {
        assert_eq!(gated(&[("search", "")]), (true, true));
        assert_eq!(gated(&[("creator_address", "")]), (true, true));
        assert_eq!(gated(&[("pointer", "")]), (true, true));
        assert_eq!(gated(&[("names", "")]), (true, true));
        assert_eq!(gated(&[("pointer", ""), ("pointer", "1,1")]), (false, true));
        assert_eq!(gated(&[("pointer", "1,1")]), (false, true));
        assert_eq!(gated(&[("positions", "1,1")]), (false, true));
        assert_eq!(gated(&[("names", "junk")]), (true, false));
        assert_eq!(gated(&[("world_names", "junk")]), (true, false));
        assert_eq!(
            gated(&[("pointer", "1,1"), ("names", "junk")]),
            (false, false)
        );
        assert_eq!(gated(&[("owner", "")]), (true, true));
    }
}

#[cfg(test)]
mod param_parity_tests {
    use super::*;

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn filters(list: &[(&str, &str)]) -> PlaceListFilters {
        parse_filters(&pairs(list)).unwrap().0
    }

    /// places.decentraland.org: no `limit` and `limit=500` both answer 100 rows.
    #[test]
    fn pagination_defaults_and_bounds_follow_upstream() {
        let f = filters(&[]);
        assert_eq!((f.limit, f.offset), (100, 0));
        let f = filters(&[("limit", "500"), ("offset", "7")]);
        assert_eq!((f.limit, f.offset), (100, 7));
        let f = filters(&[("limit", "-3"), ("offset", "abc")]);
        assert_eq!((f.limit, f.offset), (100, 0));
        assert_eq!(filters(&[("limit", "0")]).limit, 0);
    }

    #[test]
    fn multi_params_merge_parcels_never_split_and_world_names_win() {
        let f = filters(&[("categories", "art, game"), ("categories", "music")]);
        assert_eq!(f.categories, vec!["art", "game", "music"]);
        assert_eq!(filters(&[("ids", "AAA,bbb")]).ids, vec!["aaa", "bbb"]);
        assert_eq!(
            filters(&[("positions", "10,20"), ("positions", "-3,4")]).positions,
            vec!["10,20", "-3,4"]
        );
        assert_eq!(filters(&[("pointer", "10,20")]).positions, vec!["10,20"]);
        assert_eq!(
            filters(&[("positions", "1,1"), ("pointer", "2,2")]).positions,
            vec!["1,1"]
        );
        assert_eq!(filters(&[("names", "a.dcl.eth")]).names, vec!["a.dcl.eth"]);
        assert_eq!(
            filters(&[("world_names", "b.dcl.eth"), ("names", "a.dcl.eth")]).names,
            vec!["b.dcl.eth"]
        );
    }

    #[test]
    fn kinds_legacy_flags_and_lone_selectors_narrow_the_branches() {
        let f = filters(&[("kinds", "world")]);
        assert!(f.only_worlds && !f.only_places);
        let f = filters(&[("kinds", "place")]);
        assert!(f.only_places && !f.only_worlds);
        let f = filters(&[("kinds", "place,world")]);
        assert!(!f.only_places && !f.only_worlds);
        let f = filters(&[("kinds", "junk")]);
        assert!(!f.only_places && !f.only_worlds);
        let f = filters(&[("kinds", "place"), ("only_worlds", "true")]);
        assert!(f.only_worlds && !f.only_places);
        let f = filters(&[("positions", "1,1")]);
        assert!(f.only_places && !f.only_worlds);
        let f = filters(&[("world_names", "a.dcl.eth")]);
        assert!(f.only_worlds && !f.only_places);
        let f = filters(&[("positions", "1,1"), ("world_names", "a.dcl.eth")]);
        assert!(!f.only_places && !f.only_worlds);
        assert!(no_branch(&filters(&[
            ("positions", "1,1"),
            ("kinds", "world")
        ])));
        let f = filters(&[("positions", "1,1"), ("kinds", "place,world")]);
        assert!(f.only_places && !f.only_worlds);
    }

    #[test]
    fn only_sdk7_and_with_options() {
        assert_eq!(filters(&[("only_sdk7", "true")]).sdk.as_deref(), Some("7"));
        assert_eq!(filters(&[("sdk", "6")]).sdk.as_deref(), Some("6"));
        assert_eq!(filters(&[("only_sdk7", "false")]).sdk, None);
        let flags = parse_with_options(&pairs(&[(
            "with",
            "live_events,connected_users,next_event,realms_detail",
        )]))
        .unwrap();
        assert!(
            flags.with_live_events
                && flags.with_connected_users
                && flags.with_next_event
                && flags.with_realms_detail
        );
        let flags = parse_with_options(&pairs(&[
            ("with_live_events", "true"),
            ("with_realms_detail", "true"),
        ]))
        .unwrap();
        assert!(
            flags.with_live_events
                && !flags.with_connected_users
                && !flags.with_next_event
                && flags.with_realms_detail
        );
    }

    #[test]
    fn oversized_selector_lists_are_rejected_and_batch_bodies_parse() {
        let ids = (0..=MAX_BATCH_ITEMS)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_filters(&pairs(&[("ids", ids.as_str())])).is_err());
        let bare = parse_batch_body(&serde_json::json!(["A", 7]))
            .unwrap()
            .expect("ids selected");
        assert_eq!(bare.ids, vec!["a"]);
        let object = parse_batch_body(
            &serde_json::json!({"positions": ["3,3"], "world_names": ["w.dcl.eth"]}),
        )
        .unwrap()
        .expect("selectors present");
        assert!(object.ids.is_empty());
        assert_eq!(object.positions, vec!["3,3"]);
        assert_eq!(object.world_names, vec!["w.dcl.eth"]);
        assert!(
            parse_batch_body(&serde_json::json!({ "ids": [42] }))
                .unwrap()
                .is_none()
        );
        assert!(parse_batch_body(&serde_json::json!([])).unwrap().is_none());
        let unfiltered = parse_batch_body(&serde_json::json!({}))
            .unwrap()
            .expect("no selector means the open list");
        assert!(unfiltered.ids.is_empty() && unfiltered.positions.is_empty());
        let too_many: Vec<String> = (0..=MAX_BATCH_ITEMS).map(|i| i.to_string()).collect();
        assert!(parse_batch_body(&serde_json::json!({ "ids": too_many })).is_err());
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    fn destination() -> Destination {
        Destination::from(super::super::rows::sample_row())
    }

    #[test]
    fn live_and_next_event_fields_are_omitted_until_requested() {
        let v = serde_json::to_value(destination()).unwrap();
        assert!(v.get("live").is_none());
        assert!(v.get("live_event").is_none());
        assert!(v.get("live_event_name").is_none());
        assert!(v.get("next_event").is_none());
        assert_eq!(v["kind"], serde_json::json!("place"));

        let mut d = destination();
        d.live = Some(false);
        d.live_event = Some(false);
        d.live_event_name = Some(None);
        d.next_event = Some(None);
        let v = serde_json::to_value(d).unwrap();
        assert_eq!(v["live"], serde_json::json!(false));
        assert_eq!(v["live_event_name"], serde_json::Value::Null);
        assert_eq!(v["next_event"], serde_json::Value::Null);

        let mut d = destination();
        d.next_event = Some(Some(DestinationNextEvent {
            id: "ev-1".to_string(),
            name: "Upcoming".to_string(),
            next_start_at: "2030-01-01T00:00:00Z".to_string(),
        }));
        assert_eq!(
            serde_json::to_value(d).unwrap()["next_event"]["name"],
            serde_json::json!("Upcoming")
        );
    }

    #[test]
    fn kind_and_live_event_mirror_the_row() {
        let mut row = super::super::rows::sample_row();
        row.world = true;
        row.world_name = Some("My-World.dcl.eth".into());
        row.live = Some(true);
        assert_eq!(event_key(&row), "My-World.dcl.eth");
        let d = Destination::from(row);
        assert_eq!(d.kind, EntityType::World);
        assert_eq!(d.live_event, Some(true));
        assert_eq!(destination_event_key(&d), "My-World.dcl.eth");
        assert_eq!(
            serde_json::to_value(d).unwrap()["kind"],
            serde_json::json!("world")
        );
    }
}
