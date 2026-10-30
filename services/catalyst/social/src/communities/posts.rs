//! Posts: the feed (members only in a private community; anonymous viewers
//! see a private community as missing), owner/moderator writes, owner
//! deletes anything, a moderator only their own, likes by anyone not banned
//! (members only when private).

use axum::Json;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::identity::can_delete_post;
use crate::{Address, CommunityId, CommunityPermission, CommunityStanding};

use super::http::{CommError, err_json, get_pagination_params};
use super::profiles::enrich_posts_with_authors;
use super::standing::load_standing;
use super::write_support::{
    ClientCommunityWriteAuthority, load_client_standing, map_db, parse_community_id,
    validate_like_unlike_access,
};
use super::{SharedState, queries, signed_path};

macro_rules! try_resp {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(resp) => return *resp,
        }
    };
}

/// A signed viewer of a private community must hold a member row (any
/// stored role text); an anonymous one passed the public-only existence
/// check already.
pub(crate) async fn require_membership_of_a_private_community(
    state: &SharedState,
    id: CommunityId,
    signer: Option<&Address>,
    refusal_message: impl FnOnce(&Address) -> String,
) -> Result<(), CommError> {
    let Some(addr) = signer else {
        return Ok(());
    };
    if !queries::is_private(&state.pool, id).await? {
        return Ok(());
    }
    let standing = load_standing(&state.pool, id, addr).await?;
    if standing.row_exists() {
        return Ok(());
    }
    Err(CommError::not_authorized(refusal_message(addr)))
}

async fn posts_page(
    state: &SharedState,
    headers: &HeaderMap,
    original: &OriginalUri,
    id_str: &str,
    version: &str,
    pairs: &[(String, String)],
) -> Result<(Vec<serde_json::Value>, i64), CommError> {
    let id = CommunityId::parse(id_str)
        .map_err(|_| CommError::bad_request(super::http::INVALID_IDENTIFIER))?;
    let signer = state
        .optional_signer(
            headers,
            "get",
            &signed_path(original, format!("/{version}/communities/{id_str}/posts")),
        )
        .await;
    if !queries::community_exists(&state.pool, id, signer.is_none()).await? {
        return Err(CommError::not_found(format!(
            "Community not found: {id_str}"
        )));
    }
    require_membership_of_a_private_community(state, id, signer.as_ref(), |addr| {
        format!("{addr} is not a member of private community {id}. You need to be a member to get posts in this community.")
    })
    .await?;
    let pagination = get_pagination_params(pairs);
    let (posts, total) = queries::list_posts(&state.pool, id, &pagination, signer.as_ref()).await?;
    Ok((
        posts
            .into_iter()
            .map(|p| serde_json::to_value(p).unwrap_or(serde_json::Value::Null))
            .collect(),
        total,
    ))
}

pub async fn get_posts(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id_str): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<serde_json::Value>, CommError> {
    let (mut rows, total) = posts_page(&state, &headers, &original, &id_str, "v1", &pairs).await?;
    enrich_posts_with_authors(&state.profiles, &mut rows, "authorAddress").await;
    Ok(Json(json!({ "data": { "posts": rows, "total": total } })))
}

pub async fn get_posts_v2(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id_str): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<serde_json::Value>, CommError> {
    let (rows, total) = posts_page(&state, &headers, &original, &id_str, "v2", &pairs).await?;
    Ok(Json(json!({ "data": { "posts": rows, "total": total } })))
}

#[derive(Debug, Deserialize)]
pub struct PostBody {
    pub content: String,
}

fn parse_post_id(raw: &str) -> Result<Uuid, Response> {
    Uuid::parse_str(raw).map_err(|_| err_json(StatusCode::BAD_REQUEST, "invalid post id"))
}

fn parse_content(body: &[u8]) -> Result<String, Response> {
    let parsed: PostBody = serde_json::from_slice(body)
        .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid body: {e}")))?;
    let content = parsed.content.trim().to_string();
    if content.is_empty() {
        return Err(err_json(StatusCode::BAD_REQUEST, "content is required"));
    }
    Ok(content)
}

/// Names the post an `Idempotency-Key` asks for (the header the shop's writes
/// take): a UUIDv5 of the community, the signer and the key, so a retry of a
/// publish whose answer was lost finds the post the first attempt stored
/// instead of storing a second one. Without the header every publish is new.
const POST_KEY_NAMESPACE: Uuid = Uuid::from_u128(0x5c0f_9a3e_71d2_4b8e_a6c4_2e9d_0b17_f358);

fn keyed_post_id(headers: &HeaderMap, community: Uuid, author: &str) -> Result<Uuid, Response> {
    let Some(raw) = headers.get("idempotency-key") else {
        return Ok(Uuid::new_v4());
    };
    let key = raw
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|key| !key.is_empty() && key.len() <= 255)
        .ok_or_else(|| {
            err_json(
                StatusCode::BAD_REQUEST,
                "Idempotency-Key must be 1 to 255 visible ASCII characters",
            )
        })?;
    Ok(Uuid::new_v5(
        &POST_KEY_NAMESPACE,
        format!("{community}:{author}:{key}").as_bytes(),
    ))
}

pub async fn create_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let signer = match state
        .require_signer(
            &headers,
            "post",
            &signed_path(&original, format!("/v1/communities/{id}/posts")),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    try_resp!(
        ClientCommunityWriteAuthority::resolve_requiring_capability(
            &state,
            uuid,
            &signer,
            CommunityPermission::CreatePosts,
            "create posts in the community"
        )
        .await
    );
    let content = match parse_content(&body) {
        Ok(content) => content,
        Err(response) => return response,
    };
    let post_id = match keyed_post_id(&headers, uuid.as_uuid(), signer.as_str()) {
        Ok(post_id) => post_id,
        Err(response) => return response,
    };
    let inserted = try_resp!(map_db(
        sqlx::query_as::<_, (Uuid, chrono::NaiveDateTime)>(
            "INSERT INTO social.community_posts (id, community_id, author_address, content) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING RETURNING id, created_at",
        )
        .bind(post_id)
        .bind(uuid)
        .bind(&signer)
        .bind(&content)
        .fetch_optional(&state.pool)
        .await
    ));
    // A replayed key answers with the post it stored the first time.
    let (post_id, content, created_at) = match inserted {
        Some((post_id, created_at)) => (post_id, content, created_at),
        None => match try_resp!(map_db(
            sqlx::query_as::<_, (String, chrono::NaiveDateTime)>(
                "SELECT content, created_at FROM social.community_posts \
                 WHERE id = $1 AND community_id = $2 AND author_address = $3",
            )
            .bind(post_id)
            .bind(uuid)
            .bind(&signer)
            .fetch_optional(&state.pool)
            .await
        )) {
            Some((stored, created_at)) => (post_id, stored, created_at),
            None => {
                return err_json(
                    StatusCode::CONFLICT,
                    "Idempotency-Key already names another post",
                );
            }
        },
    };
    let data = json!({
        "id": post_id,
        "communityId": uuid.as_uuid(),
        "authorAddress": signer.as_str(),
        "content": content,
        "createdAt": crate::shared::as_of::wire(created_at.and_utc()),
    });
    (StatusCode::CREATED, Json(json!({ "data": data }))).into_response()
}

/// Only the current publishing author may change a post's text. Keep the
/// membership and active community stable until the edit commits.
pub async fn edit_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path((id, post_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let post_uuid = match parse_post_id(&post_id) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let signer = match state
        .require_signer(
            &headers,
            "patch",
            &signed_path(&original, format!("/v1/communities/{id}/posts/{post_id}")),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let content = match parse_content(&body) {
        Ok(content) => content,
        Err(r) => return r,
    };
    let mut tx = try_resp!(map_db(state.pool.begin().await));
    let member: Option<String> = try_resp!(map_db(
        sqlx::query_scalar(
            "SELECT m.role FROM social.communities c JOIN social.community_members m ON m.community_id = c.id \
         WHERE c.id = $1 AND c.active = TRUE AND m.member_address = $2 FOR SHARE OF c, m"
        )
        .bind(uuid)
        .bind(&signer)
        .fetch_optional(&mut *tx)
        .await
    ));
    if member.is_none() {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "Current publishing membership is required to edit a post",
        );
    }
    let banned: bool = try_resp!(map_db(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM social.community_bans WHERE community_id = $1 AND banned_address = $2 AND active = TRUE)")
            .bind(uuid)
            .bind(&signer)
            .fetch_one(&mut *tx)
            .await
    ));
    if banned {
        return err_json(
            StatusCode::FORBIDDEN,
            "Forbidden: banned from this community",
        );
    }
    if !CommunityStanding::from_rows(banned, member.as_deref())
        .has(CommunityPermission::CreatePosts)
    {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "Current publishing membership is required to edit a post",
        );
    }
    let author: Option<String> = try_resp!(map_db(
        sqlx::query_scalar(
            "SELECT author_address FROM social.community_posts WHERE id = $1 AND community_id = $2"
        )
        .bind(post_uuid)
        .bind(uuid)
        .fetch_optional(&mut *tx)
        .await
    ));
    let Some(author) = author else {
        return err_json(StatusCode::NOT_FOUND, "Post not found");
    };
    if !author.eq_ignore_ascii_case(signer.as_str()) {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "Only the author can edit this post",
        );
    }
    let updated = try_resp!(map_db(
        sqlx::query(
            "UPDATE social.community_posts SET content = $3 WHERE id = $1 AND community_id = $2"
        )
        .bind(post_uuid)
        .bind(uuid)
        .bind(content)
        .execute(&mut *tx)
        .await
    ));
    if updated.rows_affected() == 0 {
        return err_json(StatusCode::NOT_FOUND, "Post not found");
    }
    try_resp!(map_db(tx.commit().await));
    StatusCode::NO_CONTENT.into_response()
}

pub async fn delete_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path((id, post_id)): Path<(String, String)>,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let post_uuid = match parse_post_id(&post_id) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let signer = match state
        .require_signer(
            &headers,
            "delete",
            &signed_path(&original, format!("/v1/communities/{id}/posts/{post_id}")),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let author: Option<String> = try_resp!(map_db(
        sqlx::query_scalar(
            "SELECT author_address FROM social.community_posts WHERE id = $1 AND community_id = $2"
        )
        .bind(post_uuid)
        .bind(uuid)
        .fetch_optional(&state.pool)
        .await
    ));
    let Some(author) = author else {
        return err_json(StatusCode::NOT_FOUND, "Post not found");
    };
    let standing = try_resp!(load_client_standing(&state, uuid, &signer).await);
    let is_author = author.eq_ignore_ascii_case(signer.as_str());
    if !can_delete_post(standing.tier(), is_author) {
        return err_json(
            StatusCode::UNAUTHORIZED,
            format!("The user {signer} doesn't have permission to delete posts from the community"),
        );
    }
    try_resp!(map_db(
        sqlx::query("DELETE FROM social.community_posts WHERE id = $1 AND community_id = $2")
            .bind(post_uuid)
            .bind(uuid)
            .execute(&state.pool)
            .await
    ));
    StatusCode::NO_CONTENT.into_response()
}

pub async fn like_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path((id, post_id)): Path<(String, String)>,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let post_uuid = match parse_post_id(&post_id) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let signer = match state
        .require_signer(
            &headers,
            "post",
            &signed_path(
                &original,
                format!("/v1/communities/{id}/posts/{post_id}/like"),
            ),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    try_resp!(validate_like_unlike_access(&state, uuid, &signer).await);
    let exists: Option<bool> = try_resp!(map_db(
        sqlx::query_scalar(
            "SELECT TRUE FROM social.community_posts WHERE id = $1 AND community_id = $2"
        )
        .bind(post_uuid)
        .bind(uuid)
        .fetch_optional(&state.pool)
        .await
    ));
    if exists.is_none() {
        return err_json(StatusCode::NOT_FOUND, "Post not found");
    }
    try_resp!(map_db(
        sqlx::query(
            "INSERT INTO social.community_post_likes (post_id, user_address) VALUES ($1, $2) \
             ON CONFLICT (post_id, user_address) DO NOTHING",
        )
        .bind(post_uuid)
        .bind(&signer)
        .execute(&state.pool)
        .await
    ));
    StatusCode::CREATED.into_response()
}

pub async fn unlike_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Path((id, post_id)): Path<(String, String)>,
) -> Response {
    let uuid = try_resp!(parse_community_id(&id));
    let post_uuid = match parse_post_id(&post_id) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let signer = match state
        .require_signer(
            &headers,
            "delete",
            &signed_path(
                &original,
                format!("/v1/communities/{id}/posts/{post_id}/like"),
            ),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    try_resp!(validate_like_unlike_access(&state, uuid, &signer).await);
    try_resp!(map_db(
        sqlx::query(
            "DELETE FROM social.community_post_likes WHERE post_id = $1 AND user_address = $2"
        )
        .bind(post_uuid)
        .bind(&signer)
        .execute(&state.pool)
        .await
    ));
    StatusCode::NO_CONTENT.into_response()
}
