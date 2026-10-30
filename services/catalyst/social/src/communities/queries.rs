//! The read queries of the community surface over `social.*`
//! (`schemas/40_social.sql`). Three schema merges show up here:
//!
//! - `has_thumbnail` is "a `social.community_thumbnails` row exists"
//!   (the old `community_ranking_metrics` flag column);
//! - the voice-chat status derives from `social.community_voice_chat_users`
//!   (the old `community_voice_chats` projection had no writer): a room is
//!   live while it has a participant whose status is `connected` or
//!   `connection_interrupted`; participants and moderators are counts over
//!   those rows, `started_at` the earliest join;
//! - `community_places.id` became `place_id`.
//!
//! The public listing carries exactly `c.active AND NOT c.unlisted AND NOT
//! c.suspended` so `communities_ranking_idx` serves its `ORDER BY`.

use chrono::NaiveDateTime;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{Address, CommunityId, CommunityStanding};

use super::dto::*;
use super::http::Pagination;

/// The members count of community `{alias}.id`: the count of its
/// `community_members`, plus, for a mirrored community, the upstream's
/// (`community_mirror.members_count`; its local rows are the joins on this
/// node).
fn members_count_sql(alias: &str) -> String {
    format!(
        "(COALESCE((SELECT mm.members_count FROM social.community_mirror mm WHERE mm.community_id = {alias}.id), 0) \
          + (SELECT COUNT(*) FROM social.community_members m WHERE m.community_id = {alias}.id))"
    )
}

/// A mirrored community's upstream thumbnail URL (`None` for a local one or
/// before the mirror found it).
fn mirror_thumbnail_sql(alias: &str) -> String {
    format!(
        "(SELECT mm.thumbnail_url FROM social.community_mirror mm WHERE mm.community_id = {alias}.id)"
    )
}

/// The derived voice-room state per community.
const VOICE_CTE: &str = "voice AS ( \
    SELECT community_id, \
           COUNT(*)::int4 AS participants, \
           COUNT(*) FILTER (WHERE is_moderator)::int4 AS moderators, \
           MIN(joined_at) AS started_at \
    FROM social.community_voice_chat_users \
    WHERE status IN ('connected', 'connection_interrupted') \
    GROUP BY community_id)";

const MEMBER_EXPR: &str = "EXISTS (SELECT 1 FROM social.community_members m WHERE m.community_id = c.id AND m.member_address = $USER)";

/// The `total` of an offset page: the window count on the first row, else
/// the re-count when the page is past the end, else 0. A failing re-count is
/// the caller's error, never `0`: DATABASE.md keeps a fault distinct from an
/// empty result (`{total: 0, results: []}` with 200 would read as "none").
async fn page_total<F>(first: Option<i64>, offset: i64, recount: F) -> sqlx::Result<i64>
where
    F: std::future::Future<Output = sqlx::Result<i64>>,
{
    match first {
        Some(total) => Ok(total),
        None if offset > 0 => recount.await,
        None => Ok(0),
    }
}

fn default_visibility_clause(
    member_expr: &str,
    has_user: bool,
    only_member_of: bool,
    only_with_active_voice_chat: bool,
) -> Option<String> {
    if !has_user {
        Some("NOT c.unlisted AND NOT c.private".to_string())
    } else if only_member_of {
        None
    } else if only_with_active_voice_chat {
        Some(format!(
            "({member_expr} OR (NOT c.unlisted AND NOT c.private))"
        ))
    } else {
        Some("NOT c.unlisted".to_string())
    }
}

fn escape_like(s: &str) -> String {
    s.replace('%', "\\%").replace('_', "\\_")
}

pub async fn community_exists(
    pool: &PgPool,
    id: CommunityId,
    only_public: bool,
) -> sqlx::Result<bool> {
    let sql = if only_public {
        "SELECT EXISTS (SELECT 1 FROM social.communities WHERE id = $1 AND active AND NOT suspended AND NOT private)"
    } else {
        "SELECT EXISTS (SELECT 1 FROM social.communities WHERE id = $1 AND active AND NOT suspended)"
    };
    sqlx::query_scalar(sql).bind(id).fetch_one(pool).await
}

/// Existence, privacy and the viewer's membership row in one statement;
/// `None` when the community is missing, inactive or suspended.
pub async fn members_gate(
    pool: &PgPool,
    id: CommunityId,
    viewer: Option<&Address>,
) -> sqlx::Result<Option<(bool, bool)>> {
    sqlx::query_as::<_, (bool, bool)>(
        "SELECT c.private, \
                EXISTS (SELECT 1 FROM social.community_members m \
                        WHERE m.community_id = c.id AND m.member_address = $2) \
         FROM social.communities c \
         WHERE c.id = $1 AND c.active AND NOT c.suspended",
    )
    .bind(id)
    .bind(viewer)
    .fetch_optional(pool)
    .await
}

pub async fn is_private(pool: &PgPool, id: CommunityId) -> sqlx::Result<bool> {
    let row: Option<bool> =
        sqlx::query_scalar("SELECT private FROM social.communities WHERE id = $1 AND active")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.unwrap_or(false))
}

pub async fn thumbnail_hash(pool: &PgPool, id: CommunityId) -> sqlx::Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT thumbnail_hash FROM social.community_thumbnails WHERE community_id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

type DetailRow = (
    Uuid,
    String,
    String,
    String,
    bool,
    bool,
    bool,
    Option<String>,
    Option<bool>,
    i64,
    bool,
    Option<i32>,
    Option<i32>,
    Option<String>,
);

/// The private gate is the WHERE predicate: a viewer who is neither a member
/// nor a pending invitee (anonymous included) reads no row at all.
pub async fn get_by_id(
    pool: &PgPool,
    id: CommunityId,
    viewer: Option<&Address>,
) -> sqlx::Result<Option<CommunityDetail>> {
    let members_count = members_count_sql("c");
    let mirror_thumbnail = mirror_thumbnail_sql("c");
    let sql = format!(
        "WITH {VOICE_CTE} \
         SELECT c.id, c.name, c.description, c.owner_address, c.private, c.active, c.unlisted, \
                m.role, b.active, \
                {members_count}, \
                (t.community_id IS NOT NULL), v.participants, v.moderators, {mirror_thumbnail} \
         FROM social.communities c \
         LEFT JOIN social.community_members m ON m.community_id = c.id AND m.member_address = $2 \
         LEFT JOIN social.community_bans b ON b.community_id = c.id AND b.banned_address = $2 \
         LEFT JOIN social.community_thumbnails t ON t.community_id = c.id \
         LEFT JOIN voice v ON v.community_id = c.id \
         WHERE c.id = $1 AND c.active AND NOT c.suspended \
           AND (NOT c.private OR m.role IS NOT NULL OR EXISTS ( \
                 SELECT 1 FROM social.community_requests r \
                 WHERE r.community_id = c.id AND r.member_address = $2 \
                   AND r.type = 'invite' AND r.status = 'pending'))"
    );
    let row = sqlx::query_as::<_, DetailRow>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(viewer)
        .fetch_optional(pool)
        .await?;
    let Some((
        id,
        name,
        description,
        owner_address,
        private,
        active,
        unlisted,
        member_role,
        banned,
        members_count,
        has_thumbnail,
        participants,
        moderators,
        mirror_thumbnail,
    )) = row
    else {
        return Ok(None);
    };
    let mut detail = CommunityDetail {
        id,
        name,
        description,
        owner_address,
        privacy: if private { "private" } else { "public" }.to_string(),
        active,
        members_count,
        is_live: false,
        voice_chat_status: VoiceChatStatus::idle(),
        visibility: None,
        role: None,
        is_banned: None,
        thumbnail_url: mirror_thumbnail,
        owner_name: None,
        has_thumbnail,
    };
    if viewer.is_some() {
        detail.visibility = Some(if unlisted { "unlisted" } else { "all" }.to_string());
        detail.role = Some(member_role.unwrap_or_else(|| "none".to_string()));
        detail.is_banned = Some(banned.unwrap_or(false));
    }
    if let Some((participants, moderators)) = participants.zip(moderators) {
        detail.is_live = true;
        detail.voice_chat_status = VoiceChatStatus {
            is_active: true,
            participant_count: participants as i64,
            moderator_count: moderators as i64,
        };
    }
    Ok(Some(detail))
}

type ListRow = (
    Uuid,
    String,
    String,
    String,
    bool,
    bool,
    bool,
    Option<String>,
    i64,
    String,
    bool,
    bool,
    i32,
    i32,
    i64,
);

#[allow(clippy::too_many_arguments)]
pub async fn list(
    pool: &PgPool,
    pagination: &Pagination,
    search: Option<&str>,
    viewer: Option<&Address>,
    only_member_of: bool,
    only_with_active_voice_chat: bool,
    roles: &[String],
    community_id: Option<CommunityId>,
) -> sqlx::Result<(Vec<CommunityListItem>, i64)> {
    let mut where_clauses: Vec<String> =
        vec!["c.active".to_string(), "NOT c.suspended".to_string()];
    let mut params: Vec<String> = Vec::new();

    if let Some(id) = community_id {
        params.push(id.to_string());
        where_clauses.push(format!("c.id = ${}::uuid", params.len()));
    }

    if let Some(clause) = default_visibility_clause(
        MEMBER_EXPR,
        viewer.is_some(),
        only_member_of,
        only_with_active_voice_chat,
    ) {
        where_clauses.push(clause);
    }
    if let Some(s) = search {
        params.push(format!("%{}%", escape_like(s)));
        let i = params.len();
        where_clauses.push(format!("(c.name ILIKE ${i} OR c.description ILIKE ${i})"));
    }
    let user_param_idx = viewer.map(|u| {
        params.push(u.as_str().to_string());
        params.len()
    });
    for clause in where_clauses.iter_mut() {
        *clause = match user_param_idx {
            Some(i) => clause.replace("$USER", &format!("${i}")),
            None => clause.replace("$USER", "''"),
        };
    }
    if only_member_of {
        match user_param_idx {
            Some(i) => where_clauses.push(format!(
                "EXISTS (SELECT 1 FROM social.community_members m WHERE m.community_id = c.id AND m.member_address = ${i})"
            )),
            None => where_clauses.push("FALSE".to_string()),
        }
    }
    if only_with_active_voice_chat {
        where_clauses.push("v.community_id IS NOT NULL".to_string());
    }
    if !roles.is_empty() {
        match user_param_idx {
            Some(i) => {
                params.push(roles.join(","));
                let r = params.len();
                where_clauses.push(format!(
                    "EXISTS (SELECT 1 FROM social.community_members m WHERE m.community_id = c.id AND m.member_address = ${i} AND m.role = ANY(string_to_array(${r}, ',')))"
                ));
            }
            None => where_clauses.push("FALSE".to_string()),
        }
    }
    let where_sql = where_clauses.join(" AND ");
    let limit_idx = params.len() + 1;
    let offset_idx = params.len() + 2;
    let role_select = match user_param_idx {
        Some(i) => format!(
            "COALESCE((SELECT m.role FROM social.community_members m WHERE m.community_id = c.id AND m.member_address = ${i}), 'none')"
        ),
        None => "'none'".to_string(),
    };
    let members_count = members_count_sql("p");
    let mirror_thumbnail = mirror_thumbnail_sql("p");
    // The member count is derived per page row, after the LIMIT: a CTE
    // grouping every membership row cost 17.7 of a 20.8 ms page at 137k
    // memberships and grew with the table, not the page (METRICS.md,
    // "Stage 5: social bundle").
    let select_sql = format!(
        "WITH {VOICE_CTE}, page AS ( \
         SELECT c.id, c.name, c.description, c.owner_address, c.private, c.active, c.unlisted, \
                {role_select} AS role, \
                (t.community_id IS NOT NULL) AS has_thumbnail, \
                (v.community_id IS NOT NULL) AS has_voice, \
                COALESCE(v.participants, 0) AS vc_participants, \
                COALESCE(v.moderators, 0) AS vc_moderators, \
                COUNT(*) OVER () AS total, \
                c.editors_choice, c.ranking_score \
         FROM social.communities c \
         LEFT JOIN social.community_thumbnails t ON t.community_id = c.id \
         LEFT JOIN voice v ON v.community_id = c.id \
         WHERE {where_sql} \
         ORDER BY c.editors_choice DESC, c.ranking_score DESC, c.name ASC \
         LIMIT ${limit_idx} OFFSET ${offset_idx}) \
         SELECT p.id, p.name, p.description, p.owner_address, p.private, p.active, p.unlisted, \
                {mirror_thumbnail}, \
                {members_count} AS members_count, \
                p.role, p.has_thumbnail, p.has_voice, p.vc_participants, p.vc_moderators, p.total \
         FROM page p \
         ORDER BY p.editors_choice DESC, p.ranking_score DESC, p.name ASC"
    );
    let mut q = sqlx::query_as::<_, ListRow>(sqlx::AssertSqlSafe(select_sql));
    for p in &params {
        q = q.bind(p);
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;

    let total = page_total(rows.first().map(|r| r.14), pagination.offset, async {
        let count_sql =
            format!("WITH {VOICE_CTE} SELECT COUNT(*) FROM social.communities c LEFT JOIN voice v ON v.community_id = c.id WHERE {where_sql}");
        let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
        for p in &params {
            cq = cq.bind(p);
        }
        cq.fetch_one(pool).await
    })
    .await?;

    let signed = viewer.is_some();
    let results = rows
        .into_iter()
        .map(
            |(
                id,
                name,
                description,
                owner_address,
                private,
                active,
                unlisted,
                mirror_thumbnail,
                members_count,
                role,
                has_thumbnail,
                has_voice,
                vc_participants,
                vc_moderators,
                _total,
            )| {
                let voice_visible = !private || role != "none";
                let (is_live, voice_chat_status) = if voice_visible && has_voice {
                    (
                        true,
                        VoiceChatStatus {
                            is_active: true,
                            participant_count: vc_participants as i64,
                            moderator_count: vc_moderators as i64,
                        },
                    )
                } else {
                    (false, VoiceChatStatus::idle())
                };
                CommunityListItem {
                    id,
                    name,
                    description,
                    owner_address,
                    privacy: if private { "private" } else { "public" }.to_string(),
                    active,
                    members_count,
                    is_live,
                    voice_chat_status,
                    visibility: signed
                        .then(|| if unlisted { "unlisted" } else { "all" }.to_string()),
                    role: signed.then_some(role),
                    is_banned: None,
                    thumbnail_url: mirror_thumbnail,
                    owner_name: None,
                    friends: signed.then(Vec::new),
                    has_thumbnail,
                }
            },
        )
        .collect();
    Ok((results, total))
}

/// A wallet's communities as upstream lists them (`communities-db.ts`
/// `getMemberCommunities`, `queries.ts` `sortBy: 'role'` at `fb3dcb8`): owner,
/// then moderator, then the rest, each by name in the database's collation.
pub async fn member_communities(
    pool: &PgPool,
    member: &Address,
    pagination: &Pagination,
    roles: Option<&[&str]>,
    only_public_visible: bool,
) -> sqlx::Result<(Vec<MemberCommunity>, i64)> {
    let roles: Option<Vec<String>> = roles
        .filter(|rs| !rs.is_empty())
        .map(|rs| rs.iter().map(|s| s.to_string()).collect());
    let mut where_sql = "m.member_address = $1".to_string();
    if roles.is_some() {
        where_sql.push_str(" AND m.role = ANY($2::text[])");
    }
    if only_public_visible {
        where_sql.push_str(" AND NOT c.private AND NOT c.unlisted");
    }
    let limit_idx = if roles.is_some() { 3 } else { 2 };
    let offset_idx = limit_idx + 1;
    let select_sql = format!(
        "SELECT c.id, c.name, c.owner_address, m.role, COUNT(*) OVER () AS total \
         FROM social.community_members m JOIN social.communities c ON c.id = m.community_id \
         WHERE {where_sql} AND c.active \
         ORDER BY CASE m.role WHEN 'owner' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END, c.name ASC, c.id \
         LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let mut q =
        sqlx::query_as::<_, (Uuid, String, String, String, i64)>(sqlx::AssertSqlSafe(select_sql))
            .bind(member);
    if let Some(rs) = &roles {
        q = q.bind(rs.clone());
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;
    let total = page_total(rows.first().map(|r| r.4), pagination.offset, async {
            let count_sql = format!(
                "SELECT COUNT(*) FROM social.community_members m JOIN social.communities c ON c.id = m.community_id \
                 WHERE {where_sql} AND c.active"
            );
            let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql)).bind(member);
            if let Some(rs) = roles {
                cq = cq.bind(rs);
            }
            cq.fetch_one(pool).await
        })
        .await?;
    let results = rows
        .into_iter()
        .map(|(id, name, owner_address, role, _)| MemberCommunity {
            id,
            name,
            owner_address,
            role,
        })
        .collect();
    Ok((results, total))
}

type AdminRow = (
    Uuid,
    String,
    String,
    String,
    bool,
    bool,
    bool,
    bool,
    Option<NaiveDateTime>,
    Option<String>,
    Option<String>,
    NaiveDateTime,
    NaiveDateTime,
    i64,
    i64,
);

pub async fn admin_list(
    pool: &PgPool,
    pagination: &Pagination,
    status: &str,
    owner: Option<&str>,
    search: Option<&str>,
) -> sqlx::Result<(Vec<serde_json::Value>, i64)> {
    let mut where_clauses: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();
    match status {
        "active" => where_clauses.push("c.active AND NOT c.suspended".to_string()),
        "suspended" => where_clauses.push("c.suspended".to_string()),
        "inactive" => where_clauses.push("NOT c.active".to_string()),
        _ => {}
    }
    if let Some(o) = owner {
        params.push(o.to_lowercase());
        where_clauses.push(format!("c.owner_address = ${}", params.len()));
    }
    if let Some(s) = search {
        params.push(format!("%{}%", escape_like(s)));
        where_clauses.push(format!("c.name ILIKE ${}", params.len()));
    }
    let where_sql = if where_clauses.is_empty() {
        "TRUE".to_string()
    } else {
        where_clauses.join(" AND ")
    };
    let limit_idx = params.len() + 1;
    let offset_idx = params.len() + 2;
    let select_sql = format!(
        "SELECT c.id, c.name, c.description, c.owner_address, c.private, c.active, \
                c.unlisted, c.suspended, c.suspended_at, c.suspended_by, c.suspension_reason, \
                c.created_at, c.updated_at, \
                (SELECT COUNT(*) FROM social.community_members m WHERE m.community_id = c.id) AS members_count, \
                COUNT(*) OVER () AS total \
         FROM social.communities c \
         WHERE {where_sql} \
         ORDER BY c.created_at DESC \
         LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let mut q = sqlx::query_as::<_, AdminRow>(sqlx::AssertSqlSafe(select_sql));
    for p in &params {
        q = q.bind(p);
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;
    let total = page_total(rows.first().map(|r| r.14), pagination.offset, async {
        let count_sql = format!("SELECT COUNT(*) FROM social.communities c WHERE {where_sql}");
        let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
        for p in &params {
            cq = cq.bind(p);
        }
        cq.fetch_one(pool).await
    })
    .await?;
    let results = rows
        .into_iter()
        .map(
            |(
                id,
                name,
                description,
                owner_address,
                private,
                active,
                unlisted,
                suspended,
                suspended_at,
                suspended_by,
                suspension_reason,
                created_at,
                updated_at,
                members_count,
                _,
            )| {
                serde_json::json!({
                    "id": id,
                    "name": name,
                    "description": description,
                    "ownerAddress": owner_address,
                    "privacy": if private { "private" } else { "public" },
                    "visibility": if unlisted { "unlisted" } else { "all" },
                    "active": active,
                    "unlisted": unlisted,
                    "suspended": suspended,
                    "suspendedAt": suspended_at,
                    "suspendedBy": suspended_by,
                    "suspensionReason": suspension_reason,
                    "membersCount": members_count,
                    "createdAt": created_at,
                    "updatedAt": updated_at,
                })
            },
        )
        .collect();
    Ok((results, total))
}

/// The suspension CHECK ties `suspended_at`/`suspended_by` to the flag, so
/// both directions set all four columns together.
pub async fn set_suspended(
    pool: &PgPool,
    id: CommunityId,
    suspended: bool,
    actor: &str,
    reason: Option<&str>,
) -> sqlx::Result<bool> {
    let affected = if suspended {
        sqlx::query(
            "UPDATE social.communities \
             SET suspended = TRUE, suspended_at = timezone('UTC', now()), suspended_by = $2, \
                 suspension_reason = $3, updated_at = timezone('UTC', now()) \
             WHERE id = $1",
        )
        .bind(id)
        .bind(actor)
        .bind(reason)
        .execute(pool)
        .await?
        .rows_affected()
    } else {
        sqlx::query(
            "UPDATE social.communities \
             SET suspended = FALSE, suspended_at = NULL, suspended_by = NULL, \
                 suspension_reason = NULL, updated_at = timezone('UTC', now()) \
             WHERE id = $1",
        )
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected()
    };
    Ok(affected > 0)
}

/// The batch's communities the address is a member of
/// (`COMMUNITY_MEMBERSHIP_SQL`, shared with worlds' allow-list), with the wire role.
pub async fn member_communities_by_ids(
    pool: &PgPool,
    ids: &[CommunityId],
    member: &Address,
) -> sqlx::Result<Vec<(Uuid, String)>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query_as::<_, (Uuid, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT c.id, cm.role FROM {} AND c.id = ANY($1)",
        crate::shared::identity::COMMUNITY_MEMBERSHIP_SQL
    )))
    .bind(ids)
    .bind(member)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, role)| {
            CommunityStanding::from_rows(false, Some(&role))
                .role()
                .map(|r| (id, r.as_stored_text().to_string()))
        })
        .collect())
}

/// The `minimal=true` search: name prefix or word match over the listed (or
/// joined) communities the caller is not banned from, name order.
pub async fn search_communities(
    pool: &PgPool,
    search: &str,
    viewer: &Address,
    limit: i64,
    offset: i64,
) -> sqlx::Result<(Vec<CommunitySearchResult>, i64)> {
    let mut params: Vec<String> = vec![viewer.as_str().to_string()];
    let mut where_sql = String::from(
        "c.active AND NOT c.suspended \
         AND cb.banned_address IS NULL \
         AND (NOT c.unlisted OR EXISTS (\
             SELECT 1 FROM social.community_members cm \
             WHERE cm.community_id = c.id AND cm.member_address = $1))",
    );
    let search = search.trim();
    if !search.is_empty() {
        let esc = escape_like(search);
        params.push(format!("{esc}%"));
        params.push(format!("% {esc}%"));
        let word = params.len();
        let prefix = word - 1;
        where_sql.push_str(&format!(
            " AND (c.name ILIKE ${prefix} OR c.name ILIKE ${word})"
        ));
    }
    let limit_idx = params.len() + 1;
    let offset_idx = params.len() + 2;
    let ban_join = "LEFT JOIN social.community_bans cb ON c.id = cb.community_id AND cb.banned_address = $1 AND cb.active";
    let members_count = members_count_sql("c");
    let select_sql = format!(
        "SELECT c.id, c.name, \
                {members_count} AS members_count, \
                c.private, COUNT(*) OVER () AS total \
         FROM social.communities c {ban_join} \
         WHERE {where_sql} \
         ORDER BY c.name ASC \
         LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let mut q =
        sqlx::query_as::<_, (Uuid, String, i64, bool, i64)>(sqlx::AssertSqlSafe(select_sql));
    for p in &params {
        q = q.bind(p);
    }
    q = q.bind(limit).bind(offset);
    let rows = q.fetch_all(pool).await?;
    let total = page_total(rows.first().map(|r| r.4), offset, async {
        let count_sql =
            format!("SELECT COUNT(*) FROM social.communities c {ban_join} WHERE {where_sql}");
        let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
        for p in &params {
            cq = cq.bind(p);
        }
        cq.fetch_one(pool).await
    })
    .await?;
    let results = rows
        .into_iter()
        .map(
            |(id, name, members_count, private, _)| CommunitySearchResult {
                id,
                name,
                members_count,
                privacy: if private { "private" } else { "public" },
            },
        )
        .collect();
    Ok((results, total))
}

/// The wire spelling of a stored role; the CHECK allows only the three, the
/// fold keeps the old tolerance for imported spellings.
pub(crate) fn normalize_role(role: &str) -> String {
    match role {
        "owner" => "owner",
        "admin" | "mod" | "moderator" => "moderator",
        _ => "member",
    }
    .to_string()
}

const MEMBER_ORDER: &str = "ORDER BY CASE role WHEN 'owner' THEN 1 WHEN 'moderator' THEN 2 WHEN 'member' THEN 3 ELSE 4 END ASC, joined_at ASC";

type MemberRow = (Uuid, String, String, NaiveDateTime, i64);

fn row_to_member((community_id, member_address, role, joined_at, _): MemberRow) -> CommunityMember {
    CommunityMember {
        community_id,
        member_address,
        role: normalize_role(&role),
        joined_at,
    }
}

pub async fn list_members(
    pool: &PgPool,
    id: CommunityId,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityMember>, i64)> {
    let sql = format!(
        "SELECT community_id, member_address, role, joined_at, COUNT(*) OVER () AS total \
         FROM social.community_members WHERE community_id = $1 {MEMBER_ORDER} LIMIT $2 OFFSET $3"
    );
    let rows = sqlx::query_as::<_, MemberRow>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(pool)
        .await?;
    let total = page_total(rows.first().map(|r| r.4), pagination.offset, async {
        sqlx::query_scalar("SELECT COUNT(*) FROM social.community_members WHERE community_id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
    })
    .await?;
    Ok((rows.into_iter().map(row_to_member).collect(), total))
}

pub async fn list_online_members(
    pool: &PgPool,
    id: CommunityId,
    online: &[String],
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityMember>, i64)> {
    let filter: Vec<String> = online.iter().map(|a| a.to_lowercase()).collect();
    let sql = format!(
        "SELECT community_id, member_address, role, joined_at, COUNT(*) OVER () AS total \
         FROM social.community_members WHERE community_id = $1 AND member_address = ANY($2::text[]) \
         {MEMBER_ORDER} LIMIT $3 OFFSET $4"
    );
    let rows = sqlx::query_as::<_, MemberRow>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(&filter)
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(pool)
        .await?;
    let total = page_total(rows.first().map(|r| r.4), pagination.offset, async {
        sqlx::query_scalar("SELECT COUNT(*) FROM social.community_members WHERE community_id = $1 AND member_address = ANY($2::text[])")
            .bind(id)
            .bind(&filter)
            .fetch_one(pool)
            .await
    })
    .await?;
    Ok((rows.into_iter().map(row_to_member).collect(), total))
}

type RequestRow = (
    Uuid,
    Uuid,
    String,
    String,
    String,
    NaiveDateTime,
    NaiveDateTime,
);

pub async fn list_requests_by_community(
    pool: &PgPool,
    id: CommunityId,
    kind: Option<&str>,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityRequest>, i64)> {
    let mut where_sql = "community_id = $1".to_string();
    if kind.is_some() {
        where_sql.push_str(" AND type = $2");
    }
    let limit_idx = if kind.is_some() { 3 } else { 2 };
    let offset_idx = limit_idx + 1;
    let select_sql = format!(
        "SELECT id, community_id, member_address, status, type, created_at, updated_at \
         FROM social.community_requests WHERE {where_sql} ORDER BY created_at DESC LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let count_sql = format!("SELECT COUNT(*) FROM social.community_requests WHERE {where_sql}");
    let mut q = sqlx::query_as::<_, RequestRow>(sqlx::AssertSqlSafe(select_sql)).bind(id);
    let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql)).bind(id);
    if let Some(k) = kind {
        q = q.bind(k);
        cq = cq.bind(k);
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;
    let total = cq.fetch_one(pool).await?;
    let out = rows
        .into_iter()
        .map(
            |(id, community_id, member_address, status, kind, created_at, updated_at)| {
                CommunityRequest {
                    id,
                    community_id,
                    member_address,
                    status,
                    kind,
                    created_at,
                    updated_at,
                }
            },
        )
        .collect();
    Ok((out, total))
}

type AggregatedRequestRow = (
    Uuid,
    Uuid,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
    bool,
    i64,
    bool,
);

/// The wallet's own requests/invites with the community summary the client
/// renders them with; `_hasThumbnail` is a private marker the handler
/// replaces with `thumbnailUrl`.
pub async fn list_requests_by_member(
    pool: &PgPool,
    member: &Address,
    kind: Option<&str>,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<serde_json::Value>, i64)> {
    let mut where_sql = "r.member_address = $1".to_string();
    if kind.is_some() {
        where_sql.push_str(" AND r.type = $2");
    }
    let limit_idx = if kind.is_some() { 3 } else { 2 };
    let offset_idx = limit_idx + 1;
    let select_sql = format!(
        "SELECT r.id, r.community_id, r.member_address, r.status, r.type, \
                c.name, c.description, c.owner_address, c.private, c.active, \
                (SELECT COUNT(*) FROM social.community_members m WHERE m.community_id = c.id) AS members_count, \
                EXISTS (SELECT 1 FROM social.community_thumbnails t WHERE t.community_id = c.id) AS has_thumbnail \
         FROM social.community_requests r JOIN social.communities c ON c.id = r.community_id \
         WHERE {where_sql} ORDER BY r.created_at DESC LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let count_sql = format!(
        "SELECT COUNT(*) FROM social.community_requests r JOIN social.communities c ON c.id = r.community_id WHERE {where_sql}"
    );
    let mut q =
        sqlx::query_as::<_, AggregatedRequestRow>(sqlx::AssertSqlSafe(select_sql)).bind(member);
    let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql)).bind(member);
    if let Some(k) = kind {
        q = q.bind(k);
        cq = cq.bind(k);
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;
    let total = cq.fetch_one(pool).await?;
    let out = rows
        .into_iter()
        .map(
            |(
                id,
                community_id,
                member_address,
                status,
                kind,
                name,
                description,
                owner_address,
                private,
                active,
                members_count,
                has_thumbnail,
            )| {
                serde_json::json!({
                    "id": id,
                    "communityId": community_id,
                    "memberAddress": member_address,
                    "type": kind,
                    "status": status,
                    "name": name,
                    "description": description,
                    "ownerAddress": owner_address,
                    "privacy": if private { "private" } else { "public" },
                    "active": active,
                    "membersCount": members_count,
                    "role": "none",
                    "friends": serde_json::Value::Array(vec![]),
                    "_hasThumbnail": has_thumbnail,
                })
            },
        )
        .collect();
    Ok((out, total))
}

pub async fn list_posts(
    pool: &PgPool,
    id: CommunityId,
    pagination: &Pagination,
    viewer: Option<&Address>,
) -> sqlx::Result<(Vec<CommunityPost>, i64)> {
    let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, NaiveDateTime, i64, bool)>(
        "SELECT p.id, p.community_id, p.author_address, p.content, p.created_at, \
                (SELECT COUNT(*) FROM social.community_post_likes l WHERE l.post_id = p.id) AS likes_count, \
                EXISTS (SELECT 1 FROM social.community_post_likes l WHERE l.post_id = p.id AND l.user_address = $4) AS liked_by_me \
         FROM social.community_posts p \
         WHERE p.community_id = $1 \
         ORDER BY p.created_at DESC, p.id DESC \
         LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(pagination.limit)
    .bind(pagination.offset)
    .bind(viewer)
    .fetch_all(pool)
    .await?;
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM social.community_posts WHERE community_id = $1")
            .bind(id)
            .fetch_one(pool)
            .await?;
    let posts = rows
        .into_iter()
        .map(
            |(id, community_id, author_address, content, created_at, likes_count, liked_by_me)| {
                CommunityPost {
                    id,
                    community_id,
                    author_address,
                    content,
                    created_at,
                    likes_count,
                    liked_by_me,
                }
            },
        )
        .collect();
    Ok((posts, total))
}

pub async fn list_places(
    pool: &PgPool,
    id: CommunityId,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityPlace>, i64)> {
    let rows = sqlx::query_as::<_, (String, String, NaiveDateTime)>(
        "SELECT place_id, added_by, added_at FROM social.community_places \
         WHERE community_id = $1 ORDER BY added_at DESC LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(pagination.limit)
    .bind(pagination.offset)
    .fetch_all(pool)
    .await?;
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM social.community_places WHERE community_id = $1")
            .bind(id)
            .fetch_one(pool)
            .await?;
    let places = rows
        .into_iter()
        .map(|(id, added_by, added_at)| CommunityPlace {
            id,
            added_by,
            added_at,
        })
        .collect();
    Ok((places, total))
}

pub async fn list_bans(
    pool: &PgPool,
    id: CommunityId,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityBan>, i64)> {
    let rows = sqlx::query_as::<_, (Uuid, String, String, NaiveDateTime, Option<String>)>(
        "SELECT community_id, banned_address, banned_by, banned_at, reason \
         FROM social.community_bans WHERE community_id = $1 AND active \
         ORDER BY banned_at ASC LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(pagination.limit)
    .bind(pagination.offset)
    .fetch_all(pool)
    .await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM social.community_bans WHERE community_id = $1 AND active",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    let bans = rows
        .into_iter()
        .map(
            |(community_id, banned_address, banned_by, banned_at, reason)| CommunityBan {
                community_id,
                banned_address,
                banned_by,
                banned_at,
                reason,
            },
        )
        .collect();
    Ok((bans, total))
}

/// The inviter's communities the invitee is not yet in.
pub async fn list_invites(
    pool: &PgPool,
    inviter: &Address,
    invitee: &Address,
) -> sqlx::Result<Vec<CommunityInvite>> {
    let rows = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT c.id, c.name FROM social.communities c \
         JOIN social.community_members m ON m.community_id = c.id AND m.member_address = $1 \
         WHERE c.active \
           AND NOT EXISTS (SELECT 1 FROM social.community_members m2 WHERE m2.community_id = c.id AND m2.member_address = $2) \
         ORDER BY c.name ASC",
    )
    .bind(inviter)
    .bind(invitee)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name)| CommunityInvite { id, name })
        .collect())
}

pub async fn moderation_all(
    pool: &PgPool,
    search: Option<&str>,
    pagination: &Pagination,
) -> sqlx::Result<(Vec<CommunityForModeration>, i64)> {
    // upstream `getAllCommunitiesForModeration` (`communities-db.ts` at
    // `fb3dcb8`): active communities, the name or description matching,
    // the member count plus one, the largest first, then by name
    let mut where_sql = "c.active".to_string();
    let mut params: Vec<String> = Vec::new();
    if let Some(s) = search {
        params.push(format!("%{}%", escape_like(s)));
        let i = params.len();
        where_sql.push_str(&format!(
            " AND (c.name ILIKE ${i} OR c.description ILIKE ${i})"
        ));
    }
    let limit_idx = params.len() + 1;
    let offset_idx = params.len() + 2;
    let members_count = members_count_sql("c");
    let select_sql = format!(
        "SELECT c.id, c.name, c.description, c.owner_address, c.private, c.active, \
                ({members_count} + 1)::int AS members_count, c.created_at \
         FROM social.communities c WHERE {where_sql} \
         ORDER BY members_count DESC, c.name ASC, c.id LIMIT ${limit_idx} OFFSET ${offset_idx}"
    );
    let count_sql = format!("SELECT COUNT(*) FROM social.communities c WHERE {where_sql}");
    let mut q = sqlx::query_as::<_, (Uuid, String, String, String, bool, bool, i32, NaiveDateTime)>(
        sqlx::AssertSqlSafe(select_sql),
    );
    let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
    for p in &params {
        q = q.bind(p);
        cq = cq.bind(p);
    }
    q = q.bind(pagination.limit).bind(pagination.offset);
    let rows = q.fetch_all(pool).await?;
    let total = cq.fetch_one(pool).await?;
    let out = rows
        .into_iter()
        .map(
            |(id, name, description, owner_address, private, active, members_count, created_at)| {
                CommunityForModeration {
                    id,
                    name,
                    description,
                    owner_address,
                    privacy: if private { "private" } else { "public" }.to_string(),
                    active,
                    members_count,
                    created_at,
                }
            },
        )
        .collect();
    Ok((out, total))
}

pub struct ActiveVoiceRow {
    pub community_id: Uuid,
    pub community_name: String,
    pub has_thumbnail: bool,
    pub is_member: bool,
    pub participant_count: i32,
    pub moderator_count: i32,
}

/// The live rooms the viewer may see: public listed communities plus the
/// ones the viewer belongs to, newest room first.
pub async fn active_voice_for_user(
    pool: &PgPool,
    viewer: &Address,
) -> sqlx::Result<Vec<ActiveVoiceRow>> {
    let sql = format!(
        "WITH {VOICE_CTE} \
         SELECT v.community_id, c.name, \
                (t.community_id IS NOT NULL) AS has_thumbnail, \
                EXISTS (SELECT 1 FROM social.community_members m WHERE m.community_id = v.community_id AND m.member_address = $1) AS is_member, \
                v.participants, v.moderators \
         FROM voice v \
         JOIN social.communities c ON c.id = v.community_id \
         LEFT JOIN social.community_thumbnails t ON t.community_id = v.community_id \
         WHERE c.active \
           AND ((NOT c.private AND NOT c.unlisted) \
                OR EXISTS (SELECT 1 FROM social.community_members m WHERE m.community_id = v.community_id AND m.member_address = $1)) \
         ORDER BY v.started_at DESC"
    );
    let rows = sqlx::query_as::<_, (Uuid, String, bool, bool, i32, i32)>(sqlx::AssertSqlSafe(sql))
        .bind(viewer)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                community_id,
                community_name,
                has_thumbnail,
                is_member,
                participant_count,
                moderator_count,
            )| ActiveVoiceRow {
                community_id,
                community_name,
                has_thumbnail,
                is_member,
                participant_count,
                moderator_count,
            },
        )
        .collect())
}

#[cfg(test)]
mod page_total_tests {
    use super::page_total;

    #[tokio::test]
    async fn a_failing_recount_is_an_error_not_an_empty_total() {
        assert_eq!(
            page_total(Some(7), 50, async {
                unreachable!("the window count settles it")
            })
            .await
            .unwrap(),
            7
        );
        assert_eq!(
            page_total(None, 0, async { unreachable!("an empty first page is 0") })
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            page_total(None, 50, async { Ok(12) }).await.unwrap(),
            12,
            "past the end re-counts"
        );
        let err = page_total(None, 50, async { Err(sqlx::Error::PoolClosed) })
            .await
            .expect_err("a fault propagates");
        assert!(matches!(err, sqlx::Error::PoolClosed), "{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_authenticated_listing_excludes_unlisted() {
        let clause = default_visibility_clause(MEMBER_EXPR, true, false, false)
            .expect("default authed listing appends a visibility clause");
        assert_eq!(clause, "NOT c.unlisted");
        assert!(
            !clause.contains("member_address"),
            "default listing must NOT include the member disjunct (would leak unlisted): {clause}"
        );
    }

    #[test]
    fn unauthenticated_listing_is_public_only() {
        let clause = default_visibility_clause(MEMBER_EXPR, false, false, false).unwrap();
        assert_eq!(clause, "NOT c.unlisted AND NOT c.private");
    }

    #[test]
    fn only_member_of_appends_no_visibility_clause() {
        assert!(default_visibility_clause(MEMBER_EXPR, true, true, false).is_none());
    }

    #[test]
    fn active_voice_chat_keeps_member_or_public_listed_filter() {
        let clause = default_visibility_clause(MEMBER_EXPR, true, false, true).unwrap();
        assert_eq!(
            clause,
            format!("({MEMBER_EXPR} OR (NOT c.unlisted AND NOT c.private))")
        );
    }

    #[test]
    fn role_normalizes_to_unity_enum_names() {
        assert_eq!(normalize_role("owner"), "owner");
        assert_eq!(normalize_role("admin"), "moderator");
        assert_eq!(normalize_role("mod"), "moderator");
        assert_eq!(normalize_role("moderator"), "moderator");
        assert_eq!(normalize_role("member"), "member");
        assert_eq!(normalize_role("whatever"), "member");
    }

    #[test]
    fn like_patterns_escape_the_wildcards() {
        assert_eq!(escape_like("a%b_c"), "a\\%b\\_c");
    }
}
