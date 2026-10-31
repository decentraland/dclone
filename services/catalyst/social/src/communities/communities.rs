//! The community listing, detail, thumbnail and the owner-side writes.
//! Anonymous readers see public listed communities; a signed reader also
//! sees the unlisted ones it belongs to, its role, visibility, friends in
//! each community (v1 resolves them to profiles, v2 sends the addresses)
//! and may filter by its own membership. Deletion is the soft `active =
//! false` (members keep their rows, grant nothing: `standing`).

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::{Address, CommunityId, CommunityPermission, CommunityStanding};

use super::dto::{CommunityDetail, CommunityListItem};
use super::friendship::community_friends;
use super::http::{
    CommError, EnvelopeData, Paginated, Pagination, err_json, get_all, get_bool, get_first,
    get_pagination_params,
};
use super::profiles::ProfileInfo;
use super::thumbnail::{detect_image_mime_type, store_thumbnail, validate_thumbnail};
use super::write_support::{
    ClientCommunityWriteAuthority, Refusal, boundary, map_db, parse_community_id, parse_multipart,
    refuse,
};
use super::{SharedState, queries, signed_path, validate};

const MIN_SEARCH_LENGTH_FOR_MINIMAL_RESPONSE: usize = 3;
const MAX_LIMIT_FOR_MINIMAL_RESPONSE: i64 = 50;

#[derive(Debug, PartialEq, Eq)]
enum MinimalError {
    Unauthorized,
    TooShort,
}

fn plan_minimal_search(
    search: Option<&str>,
    signer_present: bool,
    requested_limit: i64,
) -> Result<(&str, i64), MinimalError> {
    if !signer_present {
        return Err(MinimalError::Unauthorized);
    }
    let Some(s) = search else {
        return Err(MinimalError::TooShort);
    };
    if s.chars().count() < MIN_SEARCH_LENGTH_FOR_MINIMAL_RESPONSE {
        return Err(MinimalError::TooShort);
    }
    Ok((s, requested_limit.min(MAX_LIMIT_FOR_MINIMAL_RESPONSE)))
}

const VALID_MEMBERSHIP_ROLES: &[&str] = &["owner", "moderator", "member", "none"];

#[derive(Debug, PartialEq, Eq)]
enum MembershipFilterError {
    Unauthorized,
    AllRolesUnknown(Vec<String>),
}

fn plan_membership_filters(
    raw_roles: Vec<String>,
    only_member_of: bool,
    signer_present: bool,
) -> Result<Vec<String>, MembershipFilterError> {
    let requested: Vec<String> = raw_roles
        .into_iter()
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
        .collect();
    let roles: Vec<String> = requested
        .iter()
        .filter(|r| VALID_MEMBERSHIP_ROLES.contains(&r.as_str()))
        .cloned()
        .collect();
    if !requested.is_empty() && roles.is_empty() {
        return Err(MembershipFilterError::AllRolesUnknown(requested));
    }
    if !signer_present && (!roles.is_empty() || only_member_of) {
        return Err(MembershipFilterError::Unauthorized);
    }
    Ok(roles)
}

fn membership_filter_error(e: MembershipFilterError) -> CommError {
    match e {
        MembershipFilterError::Unauthorized => CommError::not_authorized(
            "Authentication required to filter communities by your own membership",
        ),
        MembershipFilterError::AllRolesUnknown(requested) => CommError::bad_request(format!(
            "Unknown community role: {}. Valid roles are {}",
            requested.join(", "),
            VALID_MEMBERSHIP_ROLES.join(", ")
        )),
    }
}

/// A local thumbnail is served here; otherwise the query's value stands (a
/// mirrored community's upstream thumbnail URL), else no key: production's
/// rows carry no thumbnail field, and ui3 reads a missing one as none.
fn thumbnail_for(
    cdn_url: &str,
    id: &uuid::Uuid,
    has_thumbnail: bool,
    mirrored: Option<String>,
) -> Option<String> {
    if has_thumbnail {
        Some(super::thumbnail_url(cdn_url, &id.to_string()))
    } else {
        mirrored
    }
}

fn set_thumbnail_list(cdn_url: &str, item: &mut CommunityListItem) {
    item.thumbnail_url = thumbnail_for(
        cdn_url,
        &item.id,
        item.has_thumbnail,
        item.thumbnail_url.take(),
    );
}

fn set_thumbnail_detail(cdn_url: &str, item: &mut CommunityDetail) {
    item.thumbnail_url = thumbnail_for(
        cdn_url,
        &item.id,
        item.has_thumbnail,
        item.thumbnail_url.take(),
    );
}

fn enrich_list_v1(
    cdn_url: &str,
    items: &mut [CommunityListItem],
    owner_names: &HashMap<String, String>,
) {
    for item in items.iter_mut() {
        set_thumbnail_list(cdn_url, item);
        item.owner_name = Some(
            owner_names
                .get(&item.owner_address.to_lowercase())
                .cloned()
                .unwrap_or_default(),
        );
    }
}

fn owner_addresses(items: &[CommunityListItem]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for o in items {
        let lc = o.owner_address.to_lowercase();
        if !seen.contains(&lc) {
            seen.push(lc);
        }
    }
    seen
}

fn friend_profile(address: &str, info: &ProfileInfo) -> serde_json::Value {
    json!({
        "address": address,
        "name": info.name,
        "hasClaimedName": info.has_claimed_name,
        "profilePictureUrl": info.profile_picture_url,
    })
}

async fn friends_by_community(
    state: &SharedState,
    viewer: &Address,
    items: &[CommunityListItem],
) -> HashMap<CommunityId, Vec<String>> {
    let ids: Vec<CommunityId> = items.iter().map(|o| CommunityId::from_uuid(o.id)).collect();
    if ids.is_empty() {
        return HashMap::new();
    }
    community_friends(&state.pool, viewer, &ids).await
}

async fn enrich_with_friends(
    state: &SharedState,
    by_community: HashMap<CommunityId, Vec<String>>,
    items: &mut [CommunityListItem],
) {
    if by_community.is_empty() {
        return;
    }
    let all_addresses: Vec<String> = by_community.values().flatten().cloned().collect();
    let profiles = state.profiles.get_profiles(&all_addresses).await;
    for item in items.iter_mut() {
        let Some(addresses) = by_community.get(&CommunityId::from_uuid(item.id)) else {
            continue;
        };
        let friends: Vec<serde_json::Value> = addresses
            .iter()
            .filter_map(|addr| {
                profiles
                    .get(&addr.to_lowercase())
                    .map(|info| friend_profile(addr, info))
            })
            .collect();
        if !friends.is_empty() {
            item.friends = Some(friends);
        }
    }
}

pub async fn get_raw_thumbnail(
    State(state): State<SharedState>,
    Path(id_str): Path<String>,
) -> Response {
    let Ok(id) = CommunityId::parse(&id_str) else {
        return (StatusCode::NOT_FOUND, "thumbnail not found").into_response();
    };
    let hash = match queries::thumbnail_hash(&state.pool, id).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "raw-thumbnail: db error");
            return (StatusCode::INTERNAL_SERVER_ERROR, "database error").into_response();
        }
    };
    let Some(hash) = hash else {
        return (StatusCode::NOT_FOUND, "thumbnail not found").into_response();
    };
    match state.storage.retrieve(&hash).await {
        Ok(Some(bytes)) => {
            let mut headers = HeaderMap::new();
            let content_type = detect_image_mime_type(&bytes)
                .map(|m| m.as_str())
                .unwrap_or("image/png");
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
            if let Ok(v) = HeaderValue::from_str(&bytes.len().to_string()) {
                headers.insert(header::CONTENT_LENGTH, v);
            }
            (StatusCode::OK, headers, bytes).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "thumbnail not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, hash = %hash, "raw-thumbnail: blob store error");
            (StatusCode::INTERNAL_SERVER_ERROR, "content store error").into_response()
        }
    }
}

fn selected_community(pairs: &[(String, String)]) -> Result<Option<CommunityId>, CommError> {
    let id = get_first(pairs, "communityId")
        .map(|raw| {
            CommunityId::parse(&raw).map_err(|_| CommError::bad_request("invalid communityId"))
        })
        .transpose()?;
    if id.is_some() && get_bool(pairs, "minimal") {
        return Err(CommError::bad_request(
            "communityId cannot be combined with minimal",
        ));
    }
    Ok(id)
}

pub async fn get_communities(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Response, CommError> {
    let pagination = get_pagination_params(&pairs);
    let search = get_first(&pairs, "search")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let minimal = get_bool(&pairs, "minimal");
    let community_id = selected_community(&pairs)?;
    let signer = state
        .optional_signer(&headers, "get", &signed_path(&original, "/v1/communities"))
        .await;

    if minimal {
        return minimal_search(&state, &pagination, search.as_deref(), signer.as_ref()).await;
    }
    let only_member_of = get_bool(&pairs, "onlyMemberOf");
    let only_with_active_voice_chat = get_bool(&pairs, "onlyWithActiveVoiceChat");
    let roles = plan_membership_filters(get_all(&pairs, "roles"), only_member_of, signer.is_some())
        .map_err(membership_filter_error)?;
    let (mut results, total) = queries::list(
        &state.pool,
        &pagination,
        search.as_deref(),
        signer.as_ref(),
        only_member_of,
        only_with_active_voice_chat,
        &roles,
        community_id,
    )
    .await?;

    let owners = owner_addresses(&results);
    let (owner_names, by_community) =
        tokio::join!(state.profiles.get_owner_names(&owners), async {
            match signer.as_ref() {
                Some(user) => friends_by_community(&state, user, &results).await,
                None => HashMap::new(),
            }
        });
    enrich_list_v1(&state.settings.cdn_url, &mut results, &owner_names);
    enrich_with_friends(&state, by_community, &mut results).await;

    Ok(Json(EnvelopeData {
        data: Paginated::new(results, total, &pagination),
    })
    .into_response())
}

async fn minimal_search(
    state: &SharedState,
    pagination: &Pagination,
    search: Option<&str>,
    signer: Option<&Address>,
) -> Result<Response, CommError> {
    let (term, limit) = plan_minimal_search(search, signer.is_some(), pagination.limit).map_err(|e| match e {
        MinimalError::Unauthorized => CommError::not_authorized("Authentication required for minimal community search"),
        MinimalError::TooShort => CommError::bad_request(format!(
            "Search query must be at least {MIN_SEARCH_LENGTH_FOR_MINIMAL_RESPONSE} characters when using minimal"
        )),
    })?;
    let viewer = signer.expect("plan_minimal_search refused the anonymous caller");
    let (results, total) =
        queries::search_communities(&state.pool, term, viewer, limit, pagination.offset).await?;
    let capped = Pagination {
        limit,
        offset: pagination.offset,
    };
    Ok(Json(EnvelopeData {
        data: Paginated::new(results, total, &capped),
    })
    .into_response())
}

pub async fn get_community(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id_str): Path<String>,
) -> Result<Json<EnvelopeData<CommunityDetail>>, CommError> {
    let id = CommunityId::parse(&id_str)
        .map_err(|_| CommError::bad_request(super::http::INVALID_IDENTIFIER))?;
    let signer = state
        .optional_signer(
            &headers,
            "get",
            &signed_path(&original, format!("/v1/communities/{id_str}")),
        )
        .await;
    let Some(mut detail) = queries::get_by_id(&state.pool, id, signer.as_ref()).await? else {
        return Err(CommError::not_found(format!(
            "Community not found: {id_str}"
        )));
    };
    let owner_names = state
        .profiles
        .get_owner_names(&[detail.owner_address.to_lowercase()])
        .await;
    set_thumbnail_detail(&state.settings.cdn_url, &mut detail);
    detail.owner_name = Some(
        owner_names
            .get(&detail.owner_address.to_lowercase())
            .cloned()
            .unwrap_or_default(),
    );
    Ok(Json(EnvelopeData { data: detail }))
}

pub async fn get_communities_v2(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Response, CommError> {
    let pagination = get_pagination_params(&pairs);
    let search = get_first(&pairs, "search")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let minimal = get_bool(&pairs, "minimal");
    let community_id = selected_community(&pairs)?;
    let only_member_of = get_bool(&pairs, "onlyMemberOf");
    let only_with_active_voice_chat = get_bool(&pairs, "onlyWithActiveVoiceChat");
    let signer = state
        .optional_signer(&headers, "get", &signed_path(&original, "/v2/communities"))
        .await;

    if minimal {
        return minimal_search(&state, &pagination, search.as_deref(), signer.as_ref()).await;
    }
    let roles = plan_membership_filters(get_all(&pairs, "roles"), only_member_of, signer.is_some())
        .map_err(membership_filter_error)?;
    let (mut results, total) = queries::list(
        &state.pool,
        &pagination,
        search.as_deref(),
        signer.as_ref(),
        only_member_of,
        only_with_active_voice_chat,
        &roles,
        community_id,
    )
    .await?;
    for item in results.iter_mut() {
        set_thumbnail_list(&state.settings.cdn_url, item);
    }
    if let Some(user) = signer.as_ref() {
        let by_community = friends_by_community(&state, user, &results).await;
        for item in results.iter_mut() {
            if let Some(addresses) = by_community.get(&CommunityId::from_uuid(item.id)) {
                item.friends = Some(
                    addresses
                        .iter()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .collect(),
                );
            }
        }
    }
    Ok(Json(EnvelopeData {
        data: Paginated::new(results, total, &pagination),
    })
    .into_response())
}

pub async fn get_community_v2(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id_str): Path<String>,
) -> Result<Json<EnvelopeData<CommunityDetail>>, CommError> {
    let id = CommunityId::parse(&id_str)
        .map_err(|_| CommError::bad_request(super::http::INVALID_IDENTIFIER))?;
    let signer = state
        .optional_signer(
            &headers,
            "get",
            &signed_path(&original, format!("/v2/communities/{id_str}")),
        )
        .await;
    let Some(mut detail) = queries::get_by_id(&state.pool, id, signer.as_ref()).await? else {
        return Err(CommError::not_found(format!(
            "Community not found: {id_str}"
        )));
    };
    set_thumbnail_detail(&state.settings.cdn_url, &mut detail);
    Ok(Json(EnvelopeData { data: detail }))
}

fn parse_enum_field(value: String, allowed: [&str; 2], field: &str) -> Result<String, Refusal> {
    let trimmed = value.trim().to_string();
    if allowed.contains(&trimmed.as_str()) {
        return Ok(trimmed);
    }
    Err(refuse(
        StatusCode::BAD_REQUEST,
        format!(
            "Invalid {}: {}. Valid values are {}",
            field,
            if trimmed.is_empty() {
                "(empty)"
            } else {
                trimmed.as_str()
            },
            allowed.join(", ")
        ),
    ))
}

fn parse_privacy(value: String) -> Result<String, Refusal> {
    parse_enum_field(value, ["public", "private"], "privacy")
}

fn parse_visibility(value: String) -> Result<String, Refusal> {
    parse_enum_field(value, ["all", "unlisted"], "visibility")
}

async fn signer_or_refusal(
    state: &SharedState,
    headers: &HeaderMap,
    method: &str,
    path: super::auth::SignedPath,
) -> Result<Address, Response> {
    state
        .require_signer(headers, method, &path)
        .await
        .map_err(IntoResponse::into_response)
}

macro_rules! try_resp {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(resp) => return *resp,
        }
    };
}

pub async fn create_community(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    body: Bytes,
) -> Response {
    let signer = match signer_or_refusal(
        &state,
        &headers,
        "post",
        signed_path(&original, "/v1/communities"),
    )
    .await
    {
        Ok(s) => s,
        Err(r) => return r,
    };
    let Some(b) = boundary(&headers) else {
        return err_json(StatusCode::BAD_REQUEST, "expected multipart/form-data");
    };
    let fields = try_resp!(parse_multipart(b, body).await);
    let name = fields.name.unwrap_or_default();
    let description = fields.description.unwrap_or_default();
    let privacy = match fields.privacy {
        Some(p) => try_resp!(parse_privacy(p)),
        None => "public".to_string(),
    };
    let visibility = match fields.visibility {
        Some(v) => try_resp!(parse_visibility(v)),
        None => "all".to_string(),
    };
    let place_ids = fields.place_ids;
    let thumbnail = fields.thumbnail;
    if let Some(bytes) = thumbnail.as_deref()
        && let Err(e) = validate_thumbnail(bytes)
    {
        return err_json(StatusCode::BAD_REQUEST, e.message());
    }
    if let Err(e) = validate::validate_name(&name) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    if let Err(e) = validate::check_restricted_name(&name, &state.settings.restricted_names) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    if let Err(e) = validate::validate_description(&description) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    if let Some(false) = state.profiles.has_owned_name(signer.as_str()).await {
        return err_json(
            StatusCode::UNAUTHORIZED,
            format!("The user {signer} doesn't have any names"),
        );
    }
    if let Err(e) = state.validate_places_ownership(&place_ids, &signer).await {
        return e.into_response();
    }

    let private = privacy == "private";
    let unlisted = visibility == "unlisted";
    let id = CommunityId::new();
    let mut tx = try_resp!(map_db(state.pool.begin().await));
    try_resp!(map_db(
        sqlx::query(
            "INSERT INTO social.communities (id, name, description, owner_address, private, active, unlisted) \
             VALUES ($1, $2, $3, $4, $5, TRUE, $6)",
        )
        .bind(id)
        .bind(&name)
        .bind(&description)
        .bind(&signer)
        .bind(private)
        .bind(unlisted)
        .execute(&mut *tx)
        .await
    ));
    try_resp!(map_db(
        sqlx::query(
            "INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'owner') \
             ON CONFLICT (community_id, member_address) DO NOTHING",
        )
        .bind(id)
        .bind(&signer)
        .execute(&mut *tx)
        .await
    ));
    if let Some(bytes) = thumbnail.as_deref()
        && let Err(e) = store_thumbnail(&mut *tx, &state.storage, id, bytes).await
    {
        tracing::error!(error = %e, "failed to store community thumbnail");
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to store thumbnail",
        );
    }
    if !place_ids.is_empty() {
        try_resp!(map_db(
            sqlx::query(
                "INSERT INTO social.community_places (place_id, community_id, added_by) \
                 SELECT p, $2, $3 FROM unnest($1::text[]) AS p \
                 ON CONFLICT (place_id, community_id) DO NOTHING",
            )
            .bind(&place_ids)
            .bind(id)
            .bind(&signer)
            .execute(&mut *tx)
            .await
        ));
    }
    try_resp!(map_db(tx.commit().await));

    let mut data = json!({
        "id": id.as_uuid(),
        "name": name,
        "description": description,
        "ownerAddress": signer.as_str(),
        "privacy": if private { "private" } else { "public" },
        "visibility": if unlisted { "unlisted" } else { "all" },
        "active": true,
        "role": "owner",
        "membersCount": 1,
    });
    if thumbnail.is_some()
        && let Some(m) = data.as_object_mut()
    {
        m.insert(
            "thumbnailUrl".to_string(),
            serde_json::Value::String(state.thumbnail_url(id)),
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({ "message": "Community created successfully", "data": data })),
    )
        .into_response()
}

pub async fn update_community(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let signer = match signer_or_refusal(
        &state,
        &headers,
        "put",
        signed_path(&original, format!("/v1/communities/{id}")),
    )
    .await
    {
        Ok(s) => s,
        Err(r) => return r,
    };
    try_resp!(
        ClientCommunityWriteAuthority::resolve_requiring_capability(
            &state,
            uuid,
            &signer,
            CommunityPermission::EditInfo,
            "edit the community"
        )
        .await
    );
    let Some(b) = boundary(&headers) else {
        return err_json(StatusCode::BAD_REQUEST, "expected multipart/form-data");
    };
    let fields = try_resp!(parse_multipart(b, body).await);
    let name = fields.name;
    let description = fields.description;
    if let Err(e) = validate::validate_name_opt(name.as_deref()) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    if let Err(e) =
        validate::check_restricted_name_opt(name.as_deref(), &state.settings.restricted_names)
    {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    if let Err(e) = validate::validate_description_opt(description.as_deref()) {
        return err_json(StatusCode::BAD_REQUEST, e);
    }
    // EditName is owner-only in the capability matrix (the old client route
    // checked EditInfo alone and left the cell to the federation write path,
    // which is not ported): a name that differs from the stored one needs it.
    if let Some(new_name) = name.as_deref() {
        let current: Option<String> = try_resp!(map_db(
            sqlx::query_scalar("SELECT name FROM social.communities WHERE id = $1")
                .bind(uuid)
                .fetch_optional(&state.pool)
                .await
        ));
        if current.as_deref() != Some(new_name) {
            try_resp!(
                ClientCommunityWriteAuthority::resolve_requiring_capability(
                    &state,
                    uuid,
                    &signer,
                    CommunityPermission::EditName,
                    "edit the community name"
                )
                .await
            );
        }
    }
    let privacy: Option<bool> = match fields.privacy {
        Some(p) => Some(try_resp!(parse_privacy(p)) == "private"),
        None => None,
    };
    let visibility: Option<bool> = match fields.visibility {
        Some(v) => Some(try_resp!(parse_visibility(v)) == "unlisted"),
        None => None,
    };
    let thumbnail = fields.thumbnail;
    if let Some(bytes) = thumbnail.as_deref()
        && let Err(e) = validate_thumbnail(bytes)
    {
        return err_json(StatusCode::BAD_REQUEST, e.message());
    }
    try_resp!(map_db(
        sqlx::query(
            "UPDATE social.communities SET \
                name = COALESCE($2, name), \
                description = COALESCE($3, description), \
                private = COALESCE($4, private), \
                unlisted = COALESCE($5, unlisted), \
                updated_at = timezone('UTC', now()) \
              WHERE id = $1",
        )
        .bind(uuid)
        .bind(name.as_deref())
        .bind(description.as_deref())
        .bind(privacy)
        .bind(visibility)
        .execute(&state.pool)
        .await
    ));
    if let Some(bytes) = thumbnail.as_deref()
        && let Err(e) = store_thumbnail(&state.pool, &state.storage, uuid, bytes).await
    {
        tracing::error!(error = %e, "failed to store community thumbnail");
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to store thumbnail",
        );
    }
    let data = match queries::get_by_id(&state.pool, uuid, Some(&signer)).await {
        Ok(Some(mut obj)) => {
            if obj.has_thumbnail {
                obj.thumbnail_url = Some(state.thumbnail_url(uuid));
            }
            obj
        }
        Ok(None) => return err_json(StatusCode::NOT_FOUND, "Community not found"),
        Err(e) => return *map_db::<()>(Err(e)).unwrap_err(),
    };
    (StatusCode::OK, Json(json!({ "data": data }))).into_response()
}

#[derive(Debug, Deserialize)]
pub struct PatchBody {
    #[serde(rename = "editorsChoice", default)]
    pub editors_choice: Option<bool>,
}

/// Owner only; the one field is `editorsChoice`, the listing's first sort key.
pub async fn update_community_partially(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let signer = match signer_or_refusal(
        &state,
        &headers,
        "patch",
        signed_path(&original, format!("/v1/communities/{id}")),
    )
    .await
    {
        Ok(s) => s,
        Err(r) => return r,
    };
    try_resp!(
        ClientCommunityWriteAuthority::resolve_requiring_at_least(
            &state,
            uuid,
            &signer,
            CommunityStanding::Owner
        )
        .await
    );
    let parsed: PatchBody = serde_json::from_slice(&body).unwrap_or(PatchBody {
        editors_choice: None,
    });
    if let Some(ec) = parsed.editors_choice {
        try_resp!(map_db(
            sqlx::query("UPDATE social.communities SET editors_choice = $2, updated_at = timezone('UTC', now()) WHERE id = $1")
                .bind(uuid)
                .bind(ec)
                .execute(&state.pool)
                .await
        ));
    }
    StatusCode::NO_CONTENT.into_response()
}

pub async fn delete_community(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id): Path<String>,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let signer = match signer_or_refusal(
        &state,
        &headers,
        "delete",
        signed_path(&original, format!("/v1/communities/{id}")),
    )
    .await
    {
        Ok(s) => s,
        Err(r) => return r,
    };
    try_resp!(
        ClientCommunityWriteAuthority::resolve_requiring_capability(
            &state,
            uuid,
            &signer,
            CommunityPermission::DeleteCommunity,
            "delete the community"
        )
        .await
    );
    try_resp!(map_db(
        sqlx::query("UPDATE social.communities SET active = FALSE, updated_at = timezone('UTC', now()) WHERE id = $1")
            .bind(uuid)
            .execute(&state.pool)
            .await
    ));
    state.community_closed_to_worlds(uuid);
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communities::dto::VoiceChatStatus;
    use uuid::Uuid;

    fn sample_item(has_thumbnail: bool) -> CommunityListItem {
        CommunityListItem {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            name: "Test".to_string(),
            description: "desc".to_string(),
            owner_address: "0xOWNER".to_string(),
            privacy: "public".to_string(),
            active: true,
            members_count: 5,
            is_live: false,
            voice_chat_status: VoiceChatStatus::idle(),
            visibility: Some("all".to_string()),
            role: Some("member".to_string()),
            is_banned: None,
            thumbnail_url: None,
            owner_name: None,
            friends: Some(Vec::new()),
            has_thumbnail,
        }
    }

    #[test]
    fn list_item_carries_all_unity_fields() {
        let mut items = vec![sample_item(true)];
        let mut owners = HashMap::new();
        owners.insert("0xowner".to_string(), "OwnerName".to_string());
        enrich_list_v1("https://cdn.example", &mut items, &owners);
        let v = serde_json::to_value(&items[0]).unwrap();
        let m = v.as_object().unwrap();
        for key in [
            "id",
            "name",
            "description",
            "ownerAddress",
            "ownerName",
            "thumbnailUrl",
            "privacy",
            "visibility",
            "role",
            "membersCount",
            "friends",
            "voiceChatStatus",
        ] {
            assert!(m.contains_key(key), "missing list field {key}");
        }
        assert!(!m.contains_key("_hasThumbnail"));
        assert_eq!(m["ownerName"], "OwnerName");
        assert_eq!(
            m["thumbnailUrl"],
            "https://cdn.example/social/communities/11111111-1111-1111-1111-111111111111/raw-thumbnail.png"
        );
        assert!(m["friends"].is_array());
    }

    #[test]
    fn v2_thumbnail_is_address_only_no_owner_name_and_missing_has_no_key() {
        let mut item = sample_item(true);
        set_thumbnail_list("https://cdn.example/", &mut item);
        let v = serde_json::to_value(&item).unwrap();
        let m = v.as_object().unwrap();
        assert!(m.contains_key("ownerAddress"));
        assert!(
            !m.contains_key("ownerName"),
            "v2 shaping must not resolve/add ownerName"
        );
        assert_eq!(
            m["thumbnailUrl"],
            "https://cdn.example/social/communities/11111111-1111-1111-1111-111111111111/raw-thumbnail.png"
        );

        let mut items = vec![sample_item(false)];
        enrich_list_v1("https://cdn.example", &mut items, &HashMap::new());
        let v = serde_json::to_value(&items[0]).unwrap();
        assert!(
            v.get("thumbnailUrl").is_none(),
            "a community without a thumbnail has no thumbnailUrl, as production"
        );
        assert_eq!(v["ownerName"], "");
    }

    #[test]
    fn minimal_search_rules() {
        assert_eq!(
            plan_minimal_search(Some("cool"), false, 20),
            Err(MinimalError::Unauthorized)
        );
        assert_eq!(
            plan_minimal_search(Some("a"), false, 20),
            Err(MinimalError::Unauthorized)
        );
        assert_eq!(
            plan_minimal_search(Some("ab"), true, 20),
            Err(MinimalError::TooShort)
        );
        assert_eq!(plan_minimal_search(Some("abc"), true, 20), Ok(("abc", 20)));
        assert_eq!(
            plan_minimal_search(None, true, 20),
            Err(MinimalError::TooShort)
        );
        assert_eq!(
            plan_minimal_search(Some("cool"), true, 100),
            Ok(("cool", MAX_LIMIT_FOR_MINIMAL_RESPONSE))
        );
        assert_eq!(
            plan_minimal_search(Some("cool"), true, 10),
            Ok(("cool", 10))
        );
    }

    #[test]
    fn membership_filters_require_an_identity_and_refuse_all_unknown_roles() {
        assert_eq!(
            plan_membership_filters(vec!["owner".into()], false, false),
            Err(MembershipFilterError::Unauthorized)
        );
        assert_eq!(
            plan_membership_filters(vec![], true, false),
            Err(MembershipFilterError::Unauthorized)
        );
        assert_eq!(plan_membership_filters(vec![], false, false), Ok(vec![]));
        assert_eq!(
            plan_membership_filters(vec!["onwer".into()], false, true),
            Err(MembershipFilterError::AllRolesUnknown(vec!["onwer".into()]))
        );
        assert_eq!(
            plan_membership_filters(vec!["owner".into(), "invalid".into()], false, true),
            Ok(vec!["owner".into()])
        );
        assert_eq!(
            plan_membership_filters(vec!["".into()], false, false),
            Ok(vec![])
        );
    }

    #[test]
    fn friend_profile_matches_the_wire_shape() {
        let info = ProfileInfo {
            name: "Alice".to_string(),
            profile_picture_url: "https://content/contents/QmFace".to_string(),
            has_claimed_name: true,
            name_color: None,
        };
        let f = friend_profile("0xabc", &info);
        let m = f.as_object().unwrap();
        assert_eq!(m.len(), 4, "FriendProfile has exactly 4 fields");
        assert_eq!(m["address"], "0xabc");
        assert_eq!(m["name"], "Alice");
        assert_eq!(m["hasClaimedName"], true);
        assert_eq!(m["profilePictureUrl"], "https://content/contents/QmFace");
    }

    #[test]
    fn enum_fields_refuse_values_the_enum_does_not_name() {
        assert_eq!(parse_privacy("private".into()).unwrap(), "private");
        assert_eq!(parse_privacy(" public ".into()).unwrap(), "public");
        assert_eq!(parse_visibility("unlisted".into()).unwrap(), "unlisted");
        assert_eq!(parse_visibility("all".into()).unwrap(), "all");
        for bad in ["privte", "", "Private", "invalid"] {
            assert_eq!(
                parse_privacy(bad.into()).unwrap_err().status(),
                StatusCode::BAD_REQUEST,
                "privacy {bad:?}"
            );
        }
        for bad in ["unlsited", "", "hidden"] {
            assert_eq!(
                parse_visibility(bad.into()).unwrap_err().status(),
                StatusCode::BAD_REQUEST,
                "visibility {bad:?}"
            );
        }
    }
}
