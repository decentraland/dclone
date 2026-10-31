//! The places, map, worlds and categories read routes (`/api/...`).

use std::collections::{BTreeMap, HashMap};

use axum::Json;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, Method};
use serde::Serialize;

use super::{CategoryTarget, PlaceListFilters, PlaceOrderBy, PlaceRow, PlaceStatusRow, WorldRow};
use crate::SharedState;
use crate::shared::errors::{ApiData, ApiDataTotal, ApiDataTotalMap, ApiError};

/// Explicit favorites requests must not disguise a failed signed login as an empty list.
/// Keep the legacy unsigned read empty; public optional viewer reads remain anonymous.
pub(crate) async fn favorites_viewer(
    state: &SharedState,
    headers: &HeaderMap,
    method: &str,
    path: &str,
    only_favorites: bool,
) -> Result<Option<String>, ApiError> {
    let has_auth = headers
        .keys()
        .any(|name| name.as_str().starts_with("x-identity-"));
    if only_favorites && has_auth {
        return state.signer(headers, method, path).await.map(Some);
    }
    Ok(state.viewer(headers, method, path).await)
}

pub async fn get_place(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
    Path(place_id): Path<String>,
) -> Result<Json<ApiData<PlaceRow>>, ApiError> {
    let user = state.viewer(&headers, method.as_str(), uri.path()).await;
    match state
        .places
        .find_by_id_for(&place_id, user.as_deref())
        .await?
    {
        Some(mut p) => {
            p.apply_realms_detail(realms_detail_for(&state, &pairs));
            Ok(Json(ApiData::ok(p)))
        }
        None => Err(ApiError::not_found(format!(
            "Not found place \"{place_id}\""
        ))),
    }
}

pub async fn get_place_list(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiDataTotal<PlaceRow>>, ApiError> {
    let mut filters = parse_filters(&pairs);
    let only_favorites = pairs
        .iter()
        .any(|(k, v)| k == "only_favorites" && matches!(v.as_str(), "true" | "1"));
    let user = favorites_viewer(
        &state,
        &headers,
        method.as_str(),
        uri.path(),
        only_favorites,
    )
    .await?;
    if !state
        .places
        .scope_to_viewer(&mut filters, user.as_deref(), only_favorites)
    {
        return Ok(Json(ApiDataTotal::ok(vec![], 0)));
    }
    if let Some(owner) = pairs.iter().find(|(k, _)| k == "owner").map(|(_, v)| v) {
        filters.owner_filtered = true;
        filters.operated_positions = state.places.operated_positions(owner).await?;
    }
    let (mut data, total) = state.places.list_page(&filters).await?;
    let realms = realms_detail_for(&state, &pairs);
    for row in &mut data {
        row.apply_realms_detail(realms);
    }
    Ok(Json(ApiDataTotal::ok(data, total)))
}

pub const MAX_BATCH_ITEMS: usize = 100;

fn ids_body(ids: serde_json::Value) -> Result<Vec<String>, ApiError> {
    let ids = ids
        .as_array()
        .ok_or_else(|| {
            ApiError::bad_request("Invalid request body. Expected an array of place IDs.")
        })?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    if ids.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(
            "Cannot request more than 100 places at once",
        ));
    }
    Ok(ids)
}

pub async fn post_place_list_by_id(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(ids): Json<serde_json::Value>,
) -> Result<Json<ApiDataTotal<PlaceRow>>, ApiError> {
    let ids = ids_body(ids)?;
    let user = state.viewer(&headers, method.as_str(), uri.path()).await;
    // No LIMIT on an id lookup, so the page is the whole match set.
    let data = state.places.find_by_ids_for(&ids, user.as_deref()).await?;
    let total = data.len() as i64;
    Ok(Json(ApiDataTotal::ok(data, total)))
}

pub async fn post_place_status_list_by_id(
    State(state): State<SharedState>,
    Json(ids): Json<serde_json::Value>,
) -> Result<Json<ApiDataTotal<PlaceStatusRow>>, ApiError> {
    let ids = ids_body(ids)?;
    let data = state.places.find_by_ids_status(&ids).await?;
    let total = data.len() as i64;
    Ok(Json(ApiDataTotal::ok(data, total)))
}

pub fn with_realms_detail(pairs: &[(String, String)]) -> bool {
    pairs
        .iter()
        .any(|(k, v)| k == "with_realms_detail" && matches!(v.as_str(), "true" | "1"))
}

/// This node's realm name when the request asks for `realms_detail`.
fn realms_detail_for<'a>(state: &'a SharedState, pairs: &[(String, String)]) -> Option<&'a str> {
    with_realms_detail(pairs).then_some(state.settings.realm_name.as_str())
}

fn parse_filters(pairs: &[(String, String)]) -> PlaceListFilters {
    let get = |k: &str| pairs.iter().find(|(p, _)| p == k).map(|(_, v)| v.clone());
    let get_all = |k: &str| {
        pairs
            .iter()
            .filter(|(p, _)| p == k)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let bool_q = |k: &str| {
        get(k)
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false)
    };
    let limit = get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(0, 100);
    let offset = get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    PlaceListFilters {
        limit,
        offset,
        positions: get_all("positions"),
        names: get_all("names"),
        categories: get_all("categories"),
        only_highlighted: bool_q("only_highlighted"),
        only_excluded_from_ranking: bool_q("only_excluded_from_ranking"),
        search: get("search"),
        creator_address: get("creator_address").map(|s| s.to_lowercase()),
        sdk: get("sdk"),
        order_by: PlaceOrderBy::parse(get("order_by").as_deref()),
        order_desc: !matches!(get("order").as_deref(), Some("asc")),
        only_places: true,
        ..Default::default()
    }
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct MapEntry {
    pub id: String,
    pub base_position: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub contact_name: Option<String>,
    pub categories: Vec<String>,
    pub user_favorite: bool,
    pub user_like: bool,
    pub user_dislike: bool,
    pub user_visits: i32,
    pub user_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional, type = "Array<Record<string, unknown>>"))]
    pub realms_detail: Option<Vec<serde_json::Value>>,
}

impl From<&PlaceRow> for MapEntry {
    fn from(p: &PlaceRow) -> Self {
        Self {
            id: p.id.clone(),
            base_position: p.base_position.clone(),
            title: p.title.clone(),
            description: p.description.clone(),
            image: p.image.clone(),
            contact_name: p.contact_name.clone(),
            categories: p.categories.clone(),
            user_favorite: p.user_favorite,
            user_like: p.user_like,
            user_dislike: p.user_dislike,
            user_visits: p.user_visits,
            user_count: p.user_count.unwrap_or(0),
            realms_detail: p.realms_detail.clone(),
        }
    }
}

fn map_filters(pairs: &[(String, String)]) -> (PlaceListFilters, bool) {
    let get = |k: &str| pairs.iter().find(|(p, _)| p == k).map(|(_, v)| v.clone());
    let get_all = |k: &str| {
        pairs
            .iter()
            .filter(|(p, _)| p == k)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let only_favorites = get("only_favorites")
        .map(|v| matches!(v.as_str(), "true" | "1"))
        .unwrap_or(false);
    // places.decentraland.org answers 500 map entries by default and clamps there.
    let limit = get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(500)
        .clamp(0, 500);
    let offset = get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    let f = PlaceListFilters {
        limit,
        offset,
        positions: get_all("positions"),
        categories: get_all("categories"),
        only_highlighted: get("only_highlighted")
            .map(|v| matches!(v.as_str(), "true" | "1"))
            .unwrap_or(false),
        search: get("search"),
        creator_address: get("creator_address").map(|s| s.to_lowercase()),
        sdk: get("sdk"),
        order_by: PlaceOrderBy::parse(get("order_by").as_deref()),
        order_desc: !matches!(get("order").as_deref(), Some("asc")),
        ..Default::default()
    };
    (f, only_favorites)
}

pub async fn get_map_places(
    State(state): State<SharedState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiDataTotalMap<MapEntry>>, ApiError> {
    let (mut filters, only_favorites) = map_filters(&pairs);
    if only_favorites {
        return Ok(Json(ApiDataTotalMap::ok(BTreeMap::new(), 0)));
    }
    filters.only_places = !filters.only_highlighted;
    filters.only_categorized = true;
    let (mut data, total) = state.places.list_page(&filters).await?;
    let realms = realms_detail_for(&state, &pairs);
    let mut map: BTreeMap<String, MapEntry> = BTreeMap::new();
    for place in &mut data {
        place.apply_realms_detail(realms);
        map.insert(place.base_position.clone(), MapEntry::from(&*place));
    }
    Ok(Json(ApiDataTotalMap::ok(map, total)))
}

pub async fn get_all_places_list(
    State(state): State<SharedState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiDataTotal<PlaceRow>>, ApiError> {
    let (filters, only_favorites) = map_filters(&pairs);
    if only_favorites {
        return Ok(Json(ApiDataTotal::ok(vec![], 0)));
    }
    let (mut data, total) = state.places.list_page(&filters).await?;
    let realms = realms_detail_for(&state, &pairs);
    for place in &mut data {
        place.apply_realms_detail(realms);
    }
    Ok(Json(ApiDataTotal::ok(data, total)))
}

pub async fn get_world(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(world_id): Path<String>,
) -> Result<Json<ApiData<WorldRow>>, ApiError> {
    let user = state.viewer(&headers, method.as_str(), uri.path()).await;
    match state
        .places
        .find_world_by_id_for(&world_id, user.as_deref())
        .await?
    {
        Some(w) => Ok(Json(ApiData::ok(WorldRow::from(w)))),
        None => Err(ApiError::not_found(format!(
            "Not found world \"{world_id}\""
        ))),
    }
}

pub async fn get_world_list(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ApiDataTotal<WorldRow>>, ApiError> {
    let get = |k: &str| pairs.iter().find(|(p, _)| p == k).map(|(_, v)| v.clone());
    let get_all = |k: &str| {
        pairs
            .iter()
            .filter(|(p, _)| p == k)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let bool_q = |k: &str| {
        get(k)
            .map(|v| matches!(v.as_str(), "true" | "1"))
            .unwrap_or(false)
    };
    let only_favorites = bool_q("only_favorites");
    let user = favorites_viewer(
        &state,
        &headers,
        method.as_str(),
        uri.path(),
        only_favorites,
    )
    .await?;
    let limit = get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(0, 100);
    let offset = get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    let mut filters = PlaceListFilters {
        limit,
        offset,
        names: get_all("names"),
        categories: get_all("categories"),
        search: get("search"),
        order_by: PlaceOrderBy::parse(get("order_by").as_deref()),
        order_desc: !matches!(get("order").as_deref(), Some("asc")),
        only_highlighted: bool_q("only_highlighted"),
        only_excluded_from_ranking: bool_q("only_excluded_from_ranking"),
        only_worlds: true,
        creator_address: get("owner").map(|s| s.to_lowercase()),
        ..Default::default()
    };
    if !state
        .places
        .scope_to_viewer(&mut filters, user.as_deref(), only_favorites)
    {
        return Ok(Json(ApiDataTotal::ok(vec![], 0)));
    }
    let (data, total) = state.places.list_page(&filters).await?;
    let worlds: Vec<WorldRow> = data.into_iter().map(WorldRow::from).collect();
    Ok(Json(ApiDataTotal::ok(worlds, total)))
}

pub async fn get_world_names_list(
    State(state): State<SharedState>,
) -> Result<Json<ApiDataTotal<String>>, ApiError> {
    let names = state.places.world_names().await?;
    let total = names.len() as i64;
    Ok(Json(ApiDataTotal::ok(names, total)))
}

pub(crate) fn category_i18n_en(name: &str) -> Option<&'static str> {
    Some(match name {
        "poi" => "\u{1F4CD} Point of Interest",
        "featured" => "\u{2728} Featured",
        "art" => "\u{1F3A8} Art",
        "game" => "\u{1F3AE} Game",
        "casino" => "\u{2663}\u{FE0F} Casino",
        "social" => "\u{1F465} Social",
        "music" => "\u{1F3B5} Music",
        "fashion" => "\u{1F460} Fashion",
        "crypto" => "\u{1FA99} Crypto",
        "education" => "\u{1F4DA} Education",
        "shop" => "\u{1F6CD}\u{FE0F} Shop",
        "business" => "\u{1F3E2} Business",
        "sports" => "\u{26BD}\u{FE0F} Sports",
        "parkour" => "\u{1F3C3} Parkour",
        _ => return None,
    })
}

pub(crate) fn curated_counts(counts: Vec<(String, i64)>) -> Vec<(String, i64)> {
    counts
        .into_iter()
        .filter(|(name, _)| category_i18n_en(name).is_some())
        .collect()
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct CategoryOut {
    pub name: String,
    pub active: bool,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub count: i64,
    pub i18n: I18n,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "places/"))]
pub struct I18n {
    pub en: Option<String>,
}

pub async fn get_categories(
    State(state): State<SharedState>,
    Query(pairs): Query<HashMap<String, String>>,
) -> Result<Json<ApiData<Vec<CategoryOut>>>, ApiError> {
    let target = CategoryTarget::parse(pairs.get("target").map(|s| s.as_str()));
    let counts = curated_counts(state.places.category_counts(target).await?);
    let data = counts
        .into_iter()
        .map(|(name, count)| CategoryOut {
            i18n: I18n {
                en: category_i18n_en(&name).map(|s| s.to_string()),
            },
            active: true,
            count,
            name,
        })
        .collect();
    Ok(Json(ApiData::ok(data)))
}

#[derive(Debug, Serialize)]
pub struct PlaceCategoriesOut {
    pub categories: Vec<String>,
}

pub async fn get_place_categories(
    State(state): State<SharedState>,
    Path(place_id): Path<String>,
) -> Result<Json<ApiData<PlaceCategoriesOut>>, ApiError> {
    let categories = state.places.categories_for_place(&place_id).await?;
    Ok(Json(ApiData::ok(PlaceCategoriesOut { categories })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(kvs: &[(&str, &str)]) -> Vec<(String, String)> {
        kvs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn with_realms_detail_is_truthy_only() {
        assert!(with_realms_detail(&pairs(&[(
            "with_realms_detail",
            "true"
        )])));
        assert!(with_realms_detail(&pairs(&[("with_realms_detail", "1")])));
        assert!(!with_realms_detail(&pairs(&[(
            "with_realms_detail",
            "false"
        )])));
        assert!(!with_realms_detail(&pairs(&[("with_realms_detail", "0")])));
        assert!(!with_realms_detail(&pairs(&[])));
    }

    #[test]
    fn parse_filters_reads_residual_filters() {
        let f = parse_filters(&pairs(&[
            ("names", "Foo.dcl.eth"),
            ("names", "Bar.dcl.eth"),
            ("sdk", "7"),
            ("creator_address", "0xABC"),
            ("only_highlighted", "true"),
            ("only_excluded_from_ranking", "1"),
            ("positions", "1,2"),
            ("limit", "500"),
        ]));
        assert_eq!(f.names, vec!["Foo.dcl.eth", "Bar.dcl.eth"]);
        assert_eq!(f.sdk.as_deref(), Some("7"));
        assert_eq!(f.creator_address.as_deref(), Some("0xabc"));
        assert!(f.only_highlighted);
        assert!(f.only_excluded_from_ranking);
        assert_eq!(f.positions, vec!["1,2"]);
        assert_eq!(f.limit, 100);
        assert!(f.only_places && !f.only_worlds);
        assert!(!parse_filters(&pairs(&[])).only_excluded_from_ranking);
        assert!(
            !parse_filters(&pairs(&[("only_excluded_from_ranking", "false")]))
                .only_excluded_from_ranking
        );
    }

    #[test]
    fn the_category_list_keeps_curated_names_in_order_and_drops_raw_scene_tags() {
        let counts = vec![
            ("art".to_string(), 889),
            ("arcade".to_string(), 40),
            ("game".to_string(), 509),
            ("atlas:*".to_string(), 12),
            ("authoritative-server".to_string(), 7),
            ("parkour".to_string(), 14),
        ];
        assert_eq!(
            curated_counts(counts),
            vec![
                ("art".to_string(), 889),
                ("game".to_string(), 509),
                ("parkour".to_string(), 14)
            ]
        );
        for name in [
            "poi",
            "featured",
            "game",
            "casino",
            "social",
            "music",
            "art",
            "fashion",
            "crypto",
            "education",
            "shop",
            "sports",
            "business",
            "parkour",
        ] {
            assert!(category_i18n_en(name).is_some(), "{name} must stay listed");
        }
    }

    #[test]
    fn a_map_entry_reads_the_row_and_an_unknown_count_is_zero() {
        let mut row = super::super::rows::sample_row();
        row.user_count = None;
        row.realms_detail = Some(vec![]);
        let entry = MapEntry::from(&row);
        assert_eq!(entry.user_count, 0);
        assert_eq!(entry.base_position, "1,2");
        assert_eq!(
            serde_json::to_value(&entry).unwrap()["realms_detail"],
            serde_json::json!([])
        );
    }
    // The client signFetch + personalSign shape, for the fixture's own wallet.
    async fn hud_client_headers(
        owner: &alloy_signer_local::PrivateKeySigner,
        path: &str,
        timestamp: &str,
    ) -> HeaderMap {
        use alloy_signer::Signer;
        use alloy_signer_local::PrivateKeySigner;
        use axum::http::{HeaderName, HeaderValue};
        let ephemeral: PrivateKeySigner = format!("0x{}", "11".repeat(32)).parse().unwrap();
        let delegation = format!(
            "Decentraland Login\nEphemeral address: {:#x}\nExpiration: 2099-01-01T00:00:00.000Z",
            ephemeral.address()
        );
        let delegation_signature = owner
            .sign_message(delegation.as_bytes())
            .await
            .unwrap()
            .to_string();
        let payload = format!("GET:{path}:{timestamp}:{{}}").to_lowercase();
        let signature = ephemeral
            .sign_message(payload.as_bytes())
            .await
            .unwrap()
            .to_string();
        let links = [
            serde_json::json!({"type":"SIGNER","payload":format!("{:#x}",owner.address()),"signature":""}),
            serde_json::json!({"type":"ECDSA_EPHEMERAL","payload":delegation,"signature":delegation_signature}),
            serde_json::json!({"type":"ECDSA_SIGNED_ENTITY","payload":payload,"signature":signature}),
        ];
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-identity-timestamp",
            HeaderValue::from_str(timestamp).unwrap(),
        );
        headers.insert("x-identity-metadata", HeaderValue::from_static("{}"));
        for (index, link) in links.iter().enumerate() {
            headers.insert(
                HeaderName::from_bytes(format!("x-identity-auth-chain-{index}").as_bytes())
                    .unwrap(),
                HeaderValue::from_str(&link.to_string()).unwrap(),
            );
        }
        headers
    }

    #[tokio::test]
    async fn hud_favorites_signature_matches_the_actual_client_vector() {
        let owner = crate::shared::signed_fetch::test_support::KEY
            .parse()
            .unwrap();
        let headers = hud_client_headers(&owner, "/places/api/destinations", "1793217600000").await;
        let delegation: serde_json::Value =
            serde_json::from_str(headers["x-identity-auth-chain-1"].to_str().unwrap()).unwrap();
        assert_eq!(
            delegation["signature"],
            "0x9163f5d66c4554b3c3a4c0027a37ec781b89726320742ab6ffac9e368927d5e46642c1c14669675237fe96b6064d072e7f34352a8f61e2c827c3044a386728891c"
        );
        let link: serde_json::Value =
            serde_json::from_str(headers["x-identity-auth-chain-2"].to_str().unwrap()).unwrap();
        assert_eq!(
            link["payload"],
            "get:/places/api/destinations:1793217600000:{}"
        );
        assert_eq!(
            link["signature"],
            "0x30ad979298e9191c99c1f8de0580beefa79f73cd878f1524b494a59f074a90147dbf2912bda2ced98dd8f8efe9fb4ebcfee52105018c847b9db670c3a71d529b1b"
        );
    }

    /// Full gateway path verifies through nesting; a stripping proxy needs its
    /// original-path header. Failed authentication must not mimic an empty wallet.
    #[tokio::test]
    async fn hud_signed_favorites_roundtrip_preserves_gateway_path_and_reports_auth_failure() {
        use alloy_signer_local::PrivateKeySigner;
        use axum::{
            Router,
            body::{Body, to_bytes},
            http::{Request, StatusCode},
        };
        use std::sync::Arc;
        use tower::ServiceExt;
        let Some(pool) = crate::db::scratch::pool(3).await else {
            return;
        };
        let state = Arc::new(crate::ExploreState::new(
            pool.clone(),
            crate::Settings::for_tests(),
            None,
        ));
        let local = crate::router(state);
        let prefixed = Router::new().nest("/places", local.clone());
        let owner = PrivateKeySigner::random();
        let user = format!("{:#x}", owner.address());
        for (id, position, world) in [
            ("favorite-native", "23,24", false),
            ("favorite-other", "25,26", false),
            ("favorite-world", "27,28", true),
        ] {
            let name = world.then_some("favorite-world.dcl.eth");
            sqlx::query("INSERT INTO explore.place (id,base_position,title,highlighted,raw) VALUES ($1,$2,$1,true,$3)")
                .bind(id).bind(position).bind(serde_json::json!({"image":"https://native.example/cover.png","owner":user,"positions":[position],"world":world,"world_name":name}))
                .execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO explore.user_favorites (\"user\",entity_id) VALUES ($1,'favorite-native'),($1,'favorite-world'),('0xother','favorite-other')")
            .bind(&user).execute(&pool).await.unwrap();
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        for (endpoint, expected_ids) in [
            (
                "api/destinations",
                vec!["favorite-native", "favorite-world"],
            ),
            ("v1/destinations", vec!["favorite-native", "favorite-world"]),
            ("api/places", vec!["favorite-native"]),
            ("api/worlds", vec!["favorite-world"]),
        ] {
            let signed_path = format!("/places/{endpoint}");
            let gateway_uri = format!("{signed_path}?only_favorites=true");
            let local_uri = format!("/{endpoint}?only_favorites=true");
            let signed = hud_client_headers(&owner, &signed_path, &timestamp).await;
            for (app, uri, original) in [
                (&prefixed, gateway_uri.as_str(), None),
                (&local, local_uri.as_str(), Some(gateway_uri.as_str())),
            ] {
                let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
                *request.headers_mut() = signed.clone();
                if let Some(original) = original {
                    request
                        .headers_mut()
                        .insert("x-original-path", original.parse().unwrap());
                }
                let response = app.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
                let body: serde_json::Value = serde_json::from_slice(
                    &to_bytes(response.into_body(), 1024 * 1024).await.unwrap(),
                )
                .unwrap();
                let mut ids: Vec<_> = body["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| {
                        assert_eq!(row["user_favorite"], true, "{endpoint}: {row}");
                        row["id"].as_str().unwrap()
                    })
                    .collect();
                ids.sort_unstable();
                assert_eq!(ids, expected_ids, "{endpoint}: {body}");
                assert_eq!(body["total"], expected_ids.len(), "{endpoint}: {body}");
            }
            let mut corrupt = signed.clone();
            let mut link: serde_json::Value =
                serde_json::from_str(corrupt["x-identity-auth-chain-2"].to_str().unwrap()).unwrap();
            link["signature"] = serde_json::json!(format!("0x{}", "00".repeat(65)));
            corrupt.insert("x-identity-auth-chain-2", link.to_string().parse().unwrap());
            for headers in [
                corrupt.clone(),
                hud_client_headers(
                    &owner,
                    &format!("/{}", endpoint.rsplit('/').next().unwrap()),
                    &timestamp,
                )
                .await,
                hud_client_headers(
                    &owner,
                    &signed_path,
                    &(chrono::Utc::now().timestamp_millis() - 600_000).to_string(),
                )
                .await,
                HeaderMap::from_iter([(
                    axum::http::HeaderName::from_static("x-identity-timestamp"),
                    "broken".parse().unwrap(),
                )]),
            ] {
                let mut request = Request::builder()
                    .uri(&gateway_uri)
                    .body(Body::empty())
                    .unwrap();
                *request.headers_mut() = headers;
                let response = prefixed.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{endpoint}");
            }
            let response = prefixed
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(&gateway_uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap())
                    .unwrap();
            assert_eq!(body["total"], 0, "{endpoint}: {body}");
            assert_eq!(body["data"], serde_json::json!([]), "{endpoint}: {body}");
            // Optional viewer authentication remains anonymous for public reads.
            let mut request = Request::builder()
                .uri(&signed_path)
                .body(Body::empty())
                .unwrap();
            *request.headers_mut() = corrupt;
            assert_eq!(
                prefixed.clone().oneshot(request).await.unwrap().status(),
                StatusCode::OK,
                "{endpoint}"
            );
        }
    }
}
