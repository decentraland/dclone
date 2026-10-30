//! The queries of the social RPC over the `social` schema (schemas/40_social.sql).
//! Addresses are bound lowercase; the current
//! time is always a bind (`$now`), never `now()` in an index predicate;
//! offset pages carry `COUNT(*) OVER ()` and re-count only when the offset
//! lands past the end, as the old service did.
//!
//! Differences from the old `social_rpc` database, all from the schema
//! design: `blocks` has no surrogate id (PK is the pair); settings live in
//! `social.user_settings`, where a wallet without a row reads
//! `private_messages_privacy` as `all` (upstream's `getDefaultSettings`) and
//! a row a partial upsert creates stores the column default `only_friends`
//! (upstream's migration `1741805019806`, the schema's DEFAULT);
//! a private call's expired row is
//! reclaimed inside the start transaction, since the caller/callee UNIQUEs
//! would otherwise refuse the insert the busy probe already passed; the
//! voice presence probes read `social.private_voice_chat_participants` and
//! `social.community_voice_chat_users` (the old comms gatekeeper tables, now
//! in this schema) instead of asking the gatekeeper over HTTP.

use crate::comms::gatekeeper::voice::db::{PrivateVoiceChatRow, voice_row};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use uuid::Uuid;

/// `private_messages_privacy` of a wallet with no `social.user_settings` row,
/// wherever it is read (upstream `src/logic/settings/utils.ts`,
/// `DEFAULT_DB_PRIVATE_MESSAGES_PRIVACY`).
pub const ABSENT_PRIVATE_MESSAGES_PRIVACY: &str = "all";
/// What a partial upsert stores when it creates the row: the column default
/// (`schemas/40_social.sql`, upstream's social-settings migration).
pub const NEW_ROW_PRIVATE_MESSAGES_PRIVACY: &str = "only_friends";
pub const DEFAULT_BLOCKED_USERS_MESSAGES_VISIBILITY: &str = "show_messages";
pub const DEFAULT_SHOW_SITUATION_REACTIONS: &str = "show";

/// Liveness of a presence row that never confirmed / lost its connection
/// (the comms gatekeeper's `VOICE_CHAT_CONNECTION_INTERRUPTED_TTL` and
/// `VOICE_CHAT_INITIAL_CONNECTION_TTL` defaults).
pub const VOICE_CONNECTION_INTERRUPTED_TTL_MS: i64 = 300_000;
pub const VOICE_INITIAL_CONNECTION_TTL_MS: i64 = 300_000;

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    /// Unix ms before which no private voice chat known to this process can
    /// expire; the periodic sweep issues no DELETE until then.
    voice_due: Arc<AtomicI64>,
}

/// The counterparty of `$1` in the pair: the requester on the received list,
/// the requested party on the sent list (upstream `getReceivedFriendshipRequests`
/// selects `fa.acting_user`; the old port computed it relative to the actor
/// and named the reader on the received list, which this fixes).
const REQUEST_ADDRESS_SELECT: &str = r#"CASE
    WHEN f.address_requester = $1 THEN f.address_requested
    ELSE f.address_requester
  END AS address"#;
const SENT_ACTOR_COND: &str = "f.last_actor = $1";
const RECEIVED_ACTOR_COND: &str = "f.last_actor <> $1";
const BLOCKING_CONDITION: &str = r#"NOT EXISTS (
    SELECT 1 FROM social.blocks b
    WHERE (b.blocker_address = $1 AND b.blocked_address = CASE
              WHEN f.address_requester = $1 THEN f.address_requested
              ELSE f.address_requester END)
       OR (b.blocked_address = $1 AND b.blocker_address = CASE
              WHEN f.address_requester = $1 THEN f.address_requested
              ELSE f.address_requester END)
  )"#;

/// The pair probe on the expressions of `friendships_unordered_pair_uidx`.
const PAIR_COND: &str = "LEAST(address_requester, address_requested) = LEAST($1, $2) \
     AND GREATEST(address_requester, address_requested) = GREATEST($1, $2)";

/// The comms gatekeeper's "connected" predicate over a presence row `p`,
/// with the bind names of `now`, the interrupted TTL and the initial TTL.
fn presence_live_cond(now: &str, interrupted_ttl: &str, initial_ttl: &str) -> String {
    format!(
        "(p.status = 'connected' \
         OR (p.status = 'connection_interrupted' AND p.status_updated_at > {now} - ({interrupted_ttl} * interval '1 millisecond')) \
         OR (p.status = 'not_connected' AND p.joined_at > {now} - ({initial_ttl} * interval '1 millisecond')))"
    )
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct LastAction {
    pub friendship_id: Uuid,
    pub action: String,
    pub acting_user: String,
    pub is_active: bool,
}

/// `last_friendship_action` for one pair, plus whether either side blocks the other.
#[derive(Debug, Clone, Default)]
pub struct FriendshipProbe {
    pub last: Option<LastAction>,
    pub blocked: bool,
    pub blocked_by: bool,
}

const LAST_ACTION_SELECT: &str = r#"
SELECT f.id AS friendship_id, f.is_active AS is_active,
       fa.action AS action, fa.acting_user AS acting_user
FROM social.friendships f
LEFT JOIN LATERAL (
  SELECT action, acting_user FROM social.friendship_actions
  WHERE friendship_id = f.id ORDER BY "timestamp" DESC, id DESC LIMIT 1
) fa ON TRUE
WHERE LEAST(f.address_requester, f.address_requested) = LEAST($1, $2)
  AND GREATEST(f.address_requester, f.address_requested) = GREATEST($1, $2)
LIMIT 1"#;

fn last_action_from_row(r: &sqlx::postgres::PgRow) -> LastAction {
    LastAction {
        friendship_id: r.get::<Uuid, _>("friendship_id"),
        action: r.try_get::<String, _>("action").unwrap_or_default(),
        acting_user: r.try_get::<String, _>("acting_user").unwrap_or_default(),
        is_active: r.try_get::<bool, _>("is_active").unwrap_or(false),
    }
}

#[derive(Debug, Clone)]
pub struct FriendshipRequestRow {
    pub id: Uuid,
    pub address: String,
    pub timestamp: DateTime<Utc>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BlockedRow {
    pub address: String,
    pub blocked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocialSettingsRow {
    pub private_messages_privacy: String,
    pub blocked_users_messages_visibility: String,
    pub show_situation_reactions: String,
}

impl Default for SocialSettingsRow {
    fn default() -> Self {
        Self {
            private_messages_privacy: ABSENT_PRIVATE_MESSAGES_PRIVACY.into(),
            blocked_users_messages_visibility: DEFAULT_BLOCKED_USERS_MESSAGES_VISIBILITY.into(),
            show_situation_reactions: DEFAULT_SHOW_SITUATION_REACTIONS.into(),
        }
    }
}

/// Everything `start_private_voice_chat` reads before it inserts, in one statement.
#[derive(Debug, Clone)]
pub struct PrivateVoicePreflight {
    pub blocked: bool,
    pub caller_privacy: String,
    pub callee_privacy: String,
    /// `None` when no friendship row exists in either direction.
    pub friends: Option<bool>,
    pub busy: bool,
    /// Either party is in a live community voice room.
    pub caller_in_community_voice: bool,
    pub callee_in_community_voice: bool,
    /// Either party has live presence in a private call room.
    pub caller_in_voice: bool,
    pub callee_in_voice: bool,
}

fn now_naive() -> NaiveDateTime {
    Utc::now().naive_utc()
}

fn expiry_bound(now: NaiveDateTime, expiration_ms: i64) -> NaiveDateTime {
    now - ChronoDuration::milliseconds(expiration_ms)
}

impl Db {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            voice_due: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A call expiring at `due_ms` exists: the sweep runs no later than that.
    pub fn note_private_voice_due(&self, due_ms: i64) {
        self.voice_due.fetch_min(due_ms, Ordering::Relaxed);
    }

    pub fn private_voice_sweep_due(&self, now_ms: i64) -> bool {
        self.voice_due.load(Ordering::Relaxed) <= now_ms
    }

    /// Takes the pending due mark before a sweep; calls started meanwhile re-arm it.
    pub fn claim_private_voice_sweep(&self) {
        self.voice_due.store(i64::MAX, Ordering::Relaxed);
    }

    /// Milliseconds until the earliest pending call expires (negative when one
    /// already has); `None` when there is none. An accepted call (it has
    /// participant rows) never expires, so it does not arm the sweep.
    pub async fn private_voice_next_expiry_ms(
        &self,
        expiration_ms: i64,
    ) -> Result<Option<i64>, DbError> {
        let earliest: Option<NaiveDateTime> =
            sqlx::query_scalar(
                "SELECT MIN(created_at) FROM social.private_voice_chats \
                 WHERE NOT EXISTS (SELECT 1 FROM social.private_voice_chat_participants p WHERE p.call_id = private_voice_chats.id)",
            )
                .fetch_one(&self.pool)
                .await?;
        Ok(earliest.map(|created| {
            let expires = created + ChronoDuration::milliseconds(expiration_ms);
            (expires - now_naive()).num_milliseconds()
        }))
    }

    /// One page of friends plus the full count.
    pub async fn get_friends(
        &self,
        address: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<String>, i64), DbError> {
        let addr = address.to_lowercase();
        let rows = sqlx::query(
            r#"
            SELECT CASE WHEN address_requester = $1 THEN address_requested
                        ELSE address_requester END AS friend,
                   COUNT(*) OVER () AS total
            FROM social.friendships
            WHERE is_active = TRUE AND (address_requester = $1 OR address_requested = $1)
            ORDER BY friend
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(&addr)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let total = match rows.first() {
            Some(r) => r.get::<i64, _>("total"),
            None if offset > 0 => self.count_friends(&addr).await?,
            None => 0,
        };
        Ok((
            rows.into_iter()
                .map(|r| r.get::<String, _>("friend"))
                .collect(),
            total,
        ))
    }

    pub async fn count_friends(&self, address: &str) -> Result<i64, DbError> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM social.friendships \
             WHERE is_active = TRUE AND (address_requester = $1 OR address_requested = $1)",
        )
        .bind(address.to_lowercase())
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    pub async fn get_mutual_friends(
        &self,
        a: &str,
        b: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<String>, i64), DbError> {
        let a = a.to_lowercase();
        let b = b.to_lowercase();
        let rows = sqlx::query(
            r#"
            SELECT f1.friend, COUNT(*) OVER () AS total FROM (
              SELECT CASE WHEN address_requester = $1 THEN address_requested ELSE address_requester END AS friend
              FROM social.friendships WHERE is_active AND ($1 IN (address_requester, address_requested))
            ) f1
            JOIN (
              SELECT CASE WHEN address_requester = $2 THEN address_requested ELSE address_requester END AS friend
              FROM social.friendships WHERE is_active AND ($2 IN (address_requester, address_requested))
            ) f2 ON f1.friend = f2.friend
            WHERE f1.friend NOT IN ($1, $2)
            ORDER BY f1.friend
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(&a)
        .bind(&b)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let total = match rows.first() {
            Some(r) => r.get::<i64, _>("total"),
            None if offset > 0 => self.count_mutual_friends(&a, &b).await?,
            None => 0,
        };
        Ok((
            rows.into_iter()
                .map(|r| r.get::<String, _>("friend"))
                .collect(),
            total,
        ))
    }

    pub async fn count_mutual_friends(&self, a: &str, b: &str) -> Result<i64, DbError> {
        let n: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM (
              SELECT CASE WHEN address_requester = $1 THEN address_requested ELSE address_requester END AS friend
              FROM social.friendships WHERE is_active AND ($1 IN (address_requester, address_requested))
            ) f1
            JOIN (
              SELECT CASE WHEN address_requester = $2 THEN address_requested ELSE address_requester END AS friend
              FROM social.friendships WHERE is_active AND ($2 IN (address_requester, address_requested))
            ) f2 ON f1.friend = f2.friend
            WHERE f1.friend NOT IN ($1, $2)
            "#,
        )
        .bind(a.to_lowercase())
        .bind(b.to_lowercase())
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    /// Pending requests received (`incoming`) or sent by `address`: the
    /// friendships whose projected `last_action` is `request`, newest first,
    /// with the counterparty's blocks filtered. The page and its total read
    /// the partial pending indexes (bounded by the wallet's pending count,
    /// not its degree); the request message is one action probe per page
    /// row, after the LIMIT.
    pub async fn get_friendship_requests(
        &self,
        address: &str,
        incoming: bool,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<FriendshipRequestRow>, i64), DbError> {
        let addr = address.to_lowercase();
        let sql = format!(
            r#"
            WITH page AS (
                SELECT f.id, f.address_requester, f.address_requested, f.last_action_at,
                       COUNT(*) OVER () AS total
                FROM social.friendships f
                WHERE f.last_action = 'request'
                  AND (f.address_requester = $1 OR f.address_requested = $1)
                  AND {actor_cond}
                  AND {blocking}
                ORDER BY f.last_action_at DESC, f.id DESC
                LIMIT $2 OFFSET $3
            )
            SELECT f.id AS id,
                   {address_select},
                   f.last_action_at AS ts,
                   fa.metadata AS metadata,
                   f.total
            FROM page f
            LEFT JOIN LATERAL (
                SELECT metadata FROM social.friendship_actions
                WHERE friendship_id = f.id ORDER BY "timestamp" DESC, id DESC LIMIT 1
            ) fa ON TRUE
            ORDER BY f.last_action_at DESC, f.id DESC
            "#,
            address_select = REQUEST_ADDRESS_SELECT,
            actor_cond = if incoming {
                RECEIVED_ACTOR_COND
            } else {
                SENT_ACTOR_COND
            },
            blocking = BLOCKING_CONDITION,
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&addr)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let total = match rows.first() {
            Some(r) => r.get::<i64, _>("total"),
            None if offset > 0 => self.count_friendship_requests(&addr, incoming).await?,
            None => 0,
        };
        let page = rows
            .into_iter()
            .map(|r| {
                let metadata: Option<serde_json::Value> = r.try_get("metadata").ok();
                let message = metadata
                    .as_ref()
                    .and_then(|m| m.get("message"))
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string());
                FriendshipRequestRow {
                    id: r.get::<Uuid, _>("id"),
                    address: r.get::<String, _>("address"),
                    timestamp: r.get::<NaiveDateTime, _>("ts").and_utc(),
                    message,
                }
            })
            .collect();
        Ok((page, total))
    }

    pub async fn count_friendship_requests(
        &self,
        address: &str,
        incoming: bool,
    ) -> Result<i64, DbError> {
        let sql = format!(
            r#"
            SELECT COUNT(*)
            FROM social.friendships f
            WHERE f.last_action = 'request'
              AND (f.address_requester = $1 OR f.address_requested = $1)
              AND {actor_cond}
              AND {blocking}
            "#,
            actor_cond = if incoming {
                RECEIVED_ACTOR_COND
            } else {
                SENT_ACTOR_COND
            },
            blocking = BLOCKING_CONDITION,
        );
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(address.to_lowercase())
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn last_friendship_action(
        &self,
        a: &str,
        b: &str,
    ) -> Result<Option<LastAction>, DbError> {
        let row = sqlx::query(LAST_ACTION_SELECT)
            .bind(a.to_lowercase())
            .bind(b.to_lowercase())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(last_action_from_row))
    }

    /// The pair's friendship row with its latest action plus both block
    /// directions, in one statement.
    pub async fn friendship_probe(
        &self,
        me: &str,
        other: &str,
    ) -> Result<FriendshipProbe, DbError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            SELECT f.id AS friendship_id, f.is_active AS is_active,
                   fa.action AS action, fa.acting_user AS acting_user,
                   EXISTS (SELECT 1 FROM social.blocks
                           WHERE blocker_address = $1 AND blocked_address = $2) AS blocked,
                   EXISTS (SELECT 1 FROM social.blocks
                           WHERE blocker_address = $2 AND blocked_address = $1) AS blocked_by
            FROM (SELECT 1) AS one
            LEFT JOIN LATERAL (
              SELECT id, is_active FROM social.friendships
              WHERE {PAIR_COND}
              LIMIT 1
            ) f ON TRUE
            LEFT JOIN LATERAL (
              SELECT action, acting_user FROM social.friendship_actions
              WHERE friendship_id = f.id ORDER BY "timestamp" DESC, id DESC LIMIT 1
            ) fa ON TRUE
            "#
        )))
        .bind(me.to_lowercase())
        .bind(other.to_lowercase())
        .fetch_one(&self.pool)
        .await?;
        Ok(FriendshipProbe {
            last: row
                .get::<Option<Uuid>, _>("friendship_id")
                .map(|_| last_action_from_row(&row)),
            blocked: row.get::<bool, _>("blocked"),
            blocked_by: row.get::<bool, _>("blocked_by"),
        })
    }

    /// Records the block and returns the pair's latest friendship action in one statement.
    pub async fn block_user_and_last(
        &self,
        blocker: &str,
        blocked: &str,
    ) -> Result<Option<LastAction>, DbError> {
        let sql = format!(
            r#"WITH ins AS (
                 INSERT INTO social.blocks (blocker_address, blocked_address) VALUES ($1, $2)
                 ON CONFLICT (blocker_address, blocked_address) DO NOTHING
               )
               {LAST_ACTION_SELECT}"#
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(blocker.to_lowercase())
            .bind(blocked.to_lowercase())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(last_action_from_row))
    }

    /// Removes the block and returns the pair's latest friendship action in one statement.
    pub async fn unblock_user_and_last(
        &self,
        blocker: &str,
        blocked: &str,
    ) -> Result<Option<LastAction>, DbError> {
        let sql = format!(
            r#"WITH del AS (
                 DELETE FROM social.blocks WHERE blocker_address = $1 AND blocked_address = $2
               )
               {LAST_ACTION_SELECT}"#
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(blocker.to_lowercase())
            .bind(blocked.to_lowercase())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(last_action_from_row))
    }

    /// Upserts the friendship row and appends the action in one statement:
    /// `is_active`, `last_action`, `last_actor`, `last_action_at` and the
    /// action row move together, stamped with the statement's one `now()`.
    /// A concurrent insert on the same unordered pair resolves through
    /// `friendships_unordered_pair_uidx`.
    pub async fn apply_friendship_action(
        &self,
        acting_user: &str,
        other: &str,
        action: &str,
        is_active: bool,
        existing: Option<Uuid>,
        message: Option<&str>,
    ) -> Result<(Uuid, DateTime<Utc>), DbError> {
        let acting_user = acting_user.to_lowercase();
        let other = other.to_lowercase();
        let metadata = message.map(|m| serde_json::json!({ "message": m }));
        let row = match existing {
            Some(id) => {
                sqlx::query(
                    r#"WITH f AS (
                         UPDATE social.friendships
                         SET is_active = $1, last_action = $4, last_actor = $5,
                             last_action_at = timezone('UTC', now()), updated_at = timezone('UTC', now())
                         WHERE id = $2 RETURNING id
                       )
                       INSERT INTO social.friendship_actions (id, friendship_id, action, acting_user, metadata, "timestamp")
                       SELECT $3::uuid, f.id, $4::text, $5::text, $6::json, timezone('UTC', now()) FROM f
                       RETURNING friendship_id, "timestamp""#,
                )
                .bind(is_active)
                .bind(id)
                .bind(Uuid::new_v4())
                .bind(action)
                .bind(&acting_user)
                .bind(metadata)
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    r#"WITH f AS (
                         INSERT INTO social.friendships (id, address_requester, address_requested, is_active,
                                                         last_action, last_actor, last_action_at)
                         VALUES ($1, $2, $3, $4, $6, $7, timezone('UTC', now()))
                         ON CONFLICT ((LEAST(address_requester, address_requested)),
                                      (GREATEST(address_requester, address_requested)))
                         DO UPDATE SET is_active = EXCLUDED.is_active, last_action = EXCLUDED.last_action,
                                       last_actor = EXCLUDED.last_actor, last_action_at = EXCLUDED.last_action_at,
                                       updated_at = timezone('UTC', now())
                         RETURNING id
                       )
                       INSERT INTO social.friendship_actions (id, friendship_id, action, acting_user, metadata, "timestamp")
                       SELECT $5::uuid, f.id, $6::text, $7::text, $8::json, timezone('UTC', now()) FROM f
                       RETURNING friendship_id, "timestamp""#,
                )
                .bind(Uuid::new_v4())
                .bind(&acting_user)
                .bind(&other)
                .bind(is_active)
                .bind(Uuid::new_v4())
                .bind(action)
                .bind(&acting_user)
                .bind(metadata)
                .fetch_one(&self.pool)
                .await?
            }
        };
        Ok((
            row.get::<Uuid, _>("friendship_id"),
            row.get::<NaiveDateTime, _>("timestamp").and_utc(),
        ))
    }

    pub async fn is_friendship_blocked(&self, a: &str, b: &str) -> Result<bool, DbError> {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM social.blocks \
             WHERE (blocker_address = $1 AND blocked_address = $2) \
                OR (blocker_address = $2 AND blocked_address = $1))",
        )
        .bind(a.to_lowercase())
        .bind(b.to_lowercase())
        .fetch_one(&self.pool)
        .await?;
        Ok(blocked)
    }

    pub async fn is_blocked(&self, blocker: &str, blocked: &str) -> Result<bool, DbError> {
        let found: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM social.blocks WHERE blocker_address = $1 AND blocked_address = $2)")
                .bind(blocker.to_lowercase())
                .bind(blocked.to_lowercase())
                .fetch_one(&self.pool)
                .await?;
        Ok(found)
    }

    pub async fn block_user(&self, blocker: &str, blocked: &str) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO social.blocks (blocker_address, blocked_address) VALUES ($1, $2) \
             ON CONFLICT (blocker_address, blocked_address) DO NOTHING",
        )
        .bind(blocker.to_lowercase())
        .bind(blocked.to_lowercase())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn unblock_user(&self, blocker: &str, blocked: &str) -> Result<(), DbError> {
        sqlx::query(
            "DELETE FROM social.blocks WHERE blocker_address = $1 AND blocked_address = $2",
        )
        .bind(blocker.to_lowercase())
        .bind(blocked.to_lowercase())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_blocked_users(
        &self,
        blocker: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<BlockedRow>, i64), DbError> {
        let blocker = blocker.to_lowercase();
        let rows = sqlx::query(
            "SELECT blocked_address, blocked_at, COUNT(*) OVER () AS total FROM social.blocks \
             WHERE blocker_address = $1 \
             ORDER BY blocked_at DESC, blocked_address ASC LIMIT $2 OFFSET $3",
        )
        .bind(&blocker)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let total = match rows.first() {
            Some(r) => r.get::<i64, _>("total"),
            None if offset > 0 => self.count_blocked_users(&blocker).await?,
            None => 0,
        };
        let page = rows
            .into_iter()
            .map(|r| BlockedRow {
                address: r.get::<String, _>("blocked_address"),
                blocked_at: r.get::<NaiveDateTime, _>("blocked_at").and_utc(),
            })
            .collect();
        Ok((page, total))
    }

    pub async fn count_blocked_users(&self, blocker: &str) -> Result<i64, DbError> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM social.blocks WHERE blocker_address = $1")
                .bind(blocker.to_lowercase())
                .fetch_one(&self.pool)
                .await?;
        Ok(n)
    }

    /// `(who I blocked, who blocked me)`.
    pub async fn get_blocking_status(
        &self,
        address: &str,
    ) -> Result<(Vec<String>, Vec<String>), DbError> {
        let rows = sqlx::query(
            "SELECT blocked_address AS address, TRUE AS outgoing FROM social.blocks WHERE blocker_address = $1 \
             UNION ALL \
             SELECT blocker_address, FALSE FROM social.blocks WHERE blocked_address = $1",
        )
        .bind(address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;
        let (mut blocked, mut blocked_by) = (Vec::new(), Vec::new());
        for r in rows {
            let address = r.get::<String, _>("address");
            if r.get::<bool, _>("outgoing") {
                blocked.push(address)
            } else {
                blocked_by.push(address)
            }
        }
        Ok((blocked, blocked_by))
    }

    pub async fn get_social_settings(
        &self,
        address: &str,
    ) -> Result<Option<SocialSettingsRow>, DbError> {
        let row = sqlx::query(
            "SELECT private_messages_privacy, blocked_users_messages_visibility, show_situation_reactions \
             FROM social.user_settings WHERE address = $1",
        )
        .bind(address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| SocialSettingsRow {
            private_messages_privacy: r.get("private_messages_privacy"),
            blocked_users_messages_visibility: r.get("blocked_users_messages_visibility"),
            show_situation_reactions: r.get("show_situation_reactions"),
        }))
    }

    /// Absent fields keep the stored value, or the schema default on a new row
    /// (so a new row's privacy is `only_friends`, not the absent-row `all`).
    pub async fn upsert_social_settings(
        &self,
        address: &str,
        private_messages_privacy: Option<&str>,
        blocked_users_messages_visibility: Option<&str>,
        show_situation_reactions: Option<&str>,
    ) -> Result<SocialSettingsRow, DbError> {
        let row = sqlx::query(
            r#"
            INSERT INTO social.user_settings (address, private_messages_privacy,
                   blocked_users_messages_visibility, show_situation_reactions)
            VALUES ($1, COALESCE($2, $5), COALESCE($3, $6), COALESCE($4, $7))
            ON CONFLICT (address) DO UPDATE SET
              private_messages_privacy = COALESCE($2, social.user_settings.private_messages_privacy),
              blocked_users_messages_visibility =
                COALESCE($3, social.user_settings.blocked_users_messages_visibility),
              show_situation_reactions = COALESCE($4, social.user_settings.show_situation_reactions),
              updated_at = timezone('UTC', now())
            RETURNING private_messages_privacy, blocked_users_messages_visibility, show_situation_reactions
            "#,
        )
        .bind(address.to_lowercase())
        .bind(private_messages_privacy)
        .bind(blocked_users_messages_visibility)
        .bind(show_situation_reactions)
        .bind(NEW_ROW_PRIVATE_MESSAGES_PRIVACY)
        .bind(DEFAULT_BLOCKED_USERS_MESSAGES_VISIBILITY)
        .bind(DEFAULT_SHOW_SITUATION_REACTIONS)
        .fetch_one(&self.pool)
        .await?;
        Ok(SocialSettingsRow {
            private_messages_privacy: row.get("private_messages_privacy"),
            blocked_users_messages_visibility: row.get("blocked_users_messages_visibility"),
            show_situation_reactions: row.get("show_situation_reactions"),
        })
    }

    pub async fn reset_social_settings(&self, address: &str) -> Result<bool, DbError> {
        let res = sqlx::query("DELETE FROM social.user_settings WHERE address = $1")
            .bind(address.to_lowercase())
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// `(address, privacy, is_friend)` per target, in input order; a target
    /// blocked in either direction is not a friend for this purpose.
    pub async fn private_messages_settings(
        &self,
        caller: &str,
        targets: &[String],
    ) -> Result<Vec<(String, String, bool)>, DbError> {
        let targets: Vec<String> = targets.iter().map(|t| t.to_lowercase()).collect();
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT t.addr AS address,
                   COALESCE(s.private_messages_privacy, $3) AS privacy,
                   EXISTS (
                     SELECT 1 FROM social.friendships f
                     WHERE f.is_active = TRUE
                       AND LEAST(f.address_requester, f.address_requested) = LEAST($1, t.addr)
                       AND GREATEST(f.address_requester, f.address_requested) = GREATEST($1, t.addr)
                       AND NOT EXISTS (
                         SELECT 1 FROM social.blocks b
                         WHERE (b.blocker_address = $1 AND b.blocked_address = t.addr)
                            OR (b.blocked_address = $1 AND b.blocker_address = t.addr))
                   ) AS is_friend
            FROM unnest($2::text[]) WITH ORDINALITY AS t(addr, ord)
            LEFT JOIN social.user_settings s ON s.address = t.addr
            ORDER BY t.ord
            "#,
        )
        .bind(caller.to_lowercase())
        .bind(&targets)
        .bind(ABSENT_PRIVATE_MESSAGES_PRIVACY)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>("address"),
                    r.get::<String, _>("privacy"),
                    r.get::<bool, _>("is_friend"),
                )
            })
            .collect())
    }

    pub async fn are_users_being_called_or_calling_someone(
        &self,
        addresses: &[String],
        expiration_ms: i64,
    ) -> Result<bool, DbError> {
        let normalized: Vec<String> = addresses.iter().map(|a| a.to_lowercase()).collect();
        let busy: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM social.private_voice_chats \
             WHERE created_at >= $2 AND (caller_address = ANY($1) OR callee_address = ANY($1)))",
        )
        .bind(&normalized)
        .bind(expiry_bound(now_naive(), expiration_ms))
        .fetch_one(&self.pool)
        .await?;
        Ok(busy)
    }

    pub async fn friendship_is_active(&self, a: &str, b: &str) -> Result<Option<bool>, DbError> {
        let row: Option<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT is_active FROM social.friendships WHERE {PAIR_COND} LIMIT 1"
        )))
        .bind(a.to_lowercase())
        .bind(b.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// One read for every gate of `start_private_voice_chat`: blocks, both
    /// privacy settings, the friendship, the busy probe and the four presence
    /// probes the old service asked the gatekeeper for.
    pub async fn private_voice_preflight(
        &self,
        caller: &str,
        callee: &str,
        expiration_ms: i64,
    ) -> Result<PrivateVoicePreflight, DbError> {
        let caller = caller.to_lowercase();
        let callee = callee.to_lowercase();
        let now = now_naive();
        let sql = format!(
            r#"SELECT
                 EXISTS (
                   SELECT 1 FROM social.blocks
                   WHERE (blocker_address = $1 AND blocked_address = $2)
                      OR (blocker_address = $2 AND blocked_address = $1)
                 ) AS blocked,
                 (SELECT private_messages_privacy FROM social.user_settings WHERE address = $1) AS caller_privacy,
                 (SELECT private_messages_privacy FROM social.user_settings WHERE address = $2) AS callee_privacy,
                 (SELECT is_active FROM social.friendships WHERE {PAIR_COND} LIMIT 1) AS friends,
                 EXISTS (
                   SELECT 1 FROM social.private_voice_chats
                   WHERE created_at >= $3
                     AND (caller_address IN ($1, $2) OR callee_address IN ($1, $2))
                 ) AS busy,
                 EXISTS (SELECT 1 FROM social.community_voice_chat_users p WHERE p.address = $1 AND {live}) AS caller_in_community_voice,
                 EXISTS (SELECT 1 FROM social.community_voice_chat_users p WHERE p.address = $2 AND {live}) AS callee_in_community_voice,
                 EXISTS (SELECT 1 FROM social.private_voice_chat_participants p WHERE p.address = $1 AND {live}) AS caller_in_voice,
                 EXISTS (SELECT 1 FROM social.private_voice_chat_participants p WHERE p.address = $2 AND {live}) AS callee_in_voice"#,
            live = presence_live_cond("$4", "$5", "$6"),
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&caller)
            .bind(&callee)
            .bind(expiry_bound(now, expiration_ms))
            .bind(now)
            .bind(VOICE_CONNECTION_INTERRUPTED_TTL_MS)
            .bind(VOICE_INITIAL_CONNECTION_TTL_MS)
            .fetch_one(&self.pool)
            .await?;
        Ok(PrivateVoicePreflight {
            blocked: row.get("blocked"),
            caller_privacy: row
                .get::<Option<String>, _>("caller_privacy")
                .unwrap_or_else(|| ABSENT_PRIVATE_MESSAGES_PRIVACY.into()),
            callee_privacy: row
                .get::<Option<String>, _>("callee_privacy")
                .unwrap_or_else(|| ABSENT_PRIVATE_MESSAGES_PRIVACY.into()),
            friends: row.get("friends"),
            busy: row.get("busy"),
            caller_in_community_voice: row.get("caller_in_community_voice"),
            callee_in_community_voice: row.get("callee_in_community_voice"),
            caller_in_voice: row.get("caller_in_voice"),
            callee_in_voice: row.get("callee_in_voice"),
        })
    }

    /// The comms gatekeeper's `/users/{address}/voice-chat-status`: live
    /// presence in a private call room.
    pub async fn is_user_in_private_voice_chat(&self, address: &str) -> Result<bool, DbError> {
        let found: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM social.private_voice_chat_participants p WHERE p.address = $1 AND {})",
            presence_live_cond("$2", "$3", "$4")
        )))
        .bind(address.to_lowercase())
        .bind(now_naive())
        .bind(VOICE_CONNECTION_INTERRUPTED_TTL_MS)
        .bind(VOICE_INITIAL_CONNECTION_TTL_MS)
        .fetch_one(&self.pool)
        .await?;
        Ok(found)
    }

    /// The gatekeeper's `/users/{address}/community-voice-chat-status`.
    pub async fn is_user_in_community_voice_chat(&self, address: &str) -> Result<bool, DbError> {
        let found: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM social.community_voice_chat_users p WHERE p.address = $1 AND {})",
            presence_live_cond("$2", "$3", "$4")
        )))
        .bind(address.to_lowercase())
        .bind(now_naive())
        .bind(VOICE_CONNECTION_INTERRUPTED_TTL_MS)
        .bind(VOICE_INITIAL_CONNECTION_TTL_MS)
        .fetch_one(&self.pool)
        .await?;
        Ok(found)
    }

    pub async fn get_private_voice_chat(
        &self,
        call_id: Uuid,
    ) -> Result<Option<PrivateVoiceChatRow>, DbError> {
        let row = sqlx::query("SELECT id, caller_address, callee_address FROM social.private_voice_chats WHERE id = $1")
            .bind(call_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(voice_row))
    }

    pub async fn incoming_private_voice_chat(
        &self,
        callee: &str,
    ) -> Result<Option<PrivateVoiceChatRow>, DbError> {
        let row = sqlx::query(
            "SELECT id, caller_address, callee_address FROM social.private_voice_chats \
             WHERE callee_address = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(callee.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(voice_row))
    }

    /// The pending call this address is on, on either side.
    pub async fn get_private_voice_chat_of_user(
        &self,
        address: &str,
    ) -> Result<Option<PrivateVoiceChatRow>, DbError> {
        let row = sqlx::query(
            "SELECT id, caller_address, callee_address FROM social.private_voice_chats \
             WHERE caller_address = $1 OR callee_address = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(voice_row))
    }

    /// The live calls, newest first, with their expiry (the admin read).
    pub async fn list_active_private_voice_chats(
        &self,
        limit: i64,
        expiration_ms: i64,
    ) -> Result<Vec<(Uuid, String, String, DateTime<Utc>, DateTime<Utc>)>, DbError> {
        let rows = sqlx::query(
            "SELECT id, caller_address, callee_address, created_at FROM social.private_voice_chats \
             WHERE created_at >= $2 ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .bind(expiry_bound(now_naive(), expiration_ms))
        .fetch_all(&self.pool)
        .await?;
        let ttl = ChronoDuration::milliseconds(expiration_ms);
        Ok(rows
            .into_iter()
            .map(|r| {
                let created_at = r.get::<NaiveDateTime, _>("created_at").and_utc();
                (
                    r.get::<Uuid, _>("id"),
                    r.get::<String, _>("caller_address"),
                    r.get::<String, _>("callee_address"),
                    created_at,
                    created_at + ttl,
                )
            })
            .collect())
    }

    /// The stored role in an ACTIVE community; a soft-deleted community grants
    /// no role to anyone (tests/community_voice_soft_delete.rs).
    pub async fn community_role(
        &self,
        community_id: &str,
        address: &str,
    ) -> Result<Option<String>, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(None);
        };
        let role: Option<String> = sqlx::query_scalar(
            "SELECT role FROM social.community_members \
             WHERE community_id = $1 AND member_address = $2 \
               AND EXISTS (SELECT 1 FROM social.communities c WHERE c.id = $1 AND c.active)",
        )
        .bind(cid)
        .bind(address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(role)
    }

    /// Live ban status from `community_bans`; a ban deletes the membership
    /// row, so a role captured before the ban is the stale value being raced.
    pub async fn is_member_banned(
        &self,
        community_id: &str,
        address: &str,
    ) -> Result<bool, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(false);
        };
        let active: Option<bool> =
            sqlx::query_scalar("SELECT active FROM social.community_bans WHERE community_id = $1 AND banned_address = $2")
                .bind(cid)
                .bind(address.to_lowercase())
                .fetch_optional(&self.pool)
                .await?;
        Ok(active.unwrap_or(false))
    }

    pub async fn friend_addresses(&self, address: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT CASE WHEN address_requester = $1 THEN address_requested ELSE address_requester END \
             FROM social.friendships WHERE is_active = TRUE AND ($1 IN (address_requester, address_requested))",
        )
        .bind(address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Keyset pages bound recipient allocation before fan-out; the address pair indexes
    /// select one wallet's active relationships. No schema/index change is needed.
    pub async fn connectivity_friends_page(
        &self,
        address: &str,
        after: &str,
        unblocked: bool,
    ) -> Result<Vec<String>, DbError> {
        let query = format!(
            "SELECT friend FROM (SELECT CASE WHEN f.address_requester = $1 THEN f.address_requested ELSE f.address_requester END AS friend \
             FROM social.friendships f WHERE f.is_active AND $1 IN (f.address_requester, f.address_requested) \
             AND (NOT $3 OR ({BLOCKING_CONDITION}))) candidates WHERE friend > $2 ORDER BY friend LIMIT 256"
        );
        Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(address.to_lowercase())
            .bind(after)
            .bind(unblocked)
            .fetch_all(&self.pool)
            .await?)
    }

    /// The same active/no-block rule as initial and live pages, checked for one queued recipient.
    pub async fn connectivity_friend_allowed(
        &self,
        actor: &str,
        recipient: &str,
    ) -> Result<bool, DbError> {
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM social.friendships f WHERE f.is_active AND $1 IN (f.address_requester, f.address_requested) AND $2 = CASE WHEN f.address_requester = $1 THEN f.address_requested ELSE f.address_requester END AND ({BLOCKING_CONDITION}))"
        );
        Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(actor.to_lowercase())
            .bind(recipient.to_lowercase())
            .fetch_one(&self.pool)
            .await?)
    }

    pub async fn connectivity_member_allowed(
        &self,
        actor: &str,
        recipient: &str,
        community: Uuid,
    ) -> Result<bool, DbError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social.communities c JOIN social.community_members a ON a.community_id=c.id AND a.member_address=$1 JOIN social.community_members r ON r.community_id=c.id AND r.member_address=$2 WHERE c.id=$3 AND c.active)")
            .bind(actor.to_lowercase()).bind(recipient.to_lowercase()).bind(community).fetch_one(&self.pool).await?)
    }

    /// The membership PK (community_id, member_address) is the stable two-column cursor.
    pub async fn connectivity_members_page(
        &self,
        address: &str,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<(Uuid, String)>, DbError> {
        let (community, member) = after.unwrap_or((Uuid::nil(), String::new()));
        Ok(sqlx::query_as(
            "SELECT m.community_id, m.member_address FROM social.community_members m \
             JOIN social.communities c ON c.id = m.community_id AND c.active \
             JOIN social.community_members own ON own.community_id = m.community_id AND own.member_address = $1 \
             WHERE m.member_address <> $1 AND (m.community_id, m.member_address) > ($2, $3) \
             ORDER BY m.community_id, m.member_address LIMIT 256"
        ).bind(address.to_lowercase()).bind(community).bind(member).fetch_all(&self.pool).await?)
    }

    /// Active friends with no block in either direction.
    pub async fn friend_addresses_unblocked(&self, address: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT CASE WHEN address_requester = $1 THEN address_requested ELSE address_requester END AS friend
            FROM social.friendships
            WHERE is_active = TRUE
              AND ($1 IN (address_requester, address_requested))
              AND NOT EXISTS (
                SELECT 1 FROM social.blocks b
                WHERE (b.blocker_address = $1 AND b.blocked_address = CASE
                          WHEN address_requester = $1 THEN address_requested ELSE address_requester END)
                   OR (b.blocked_address = $1 AND b.blocker_address = CASE
                          WHEN address_requester = $1 THEN address_requested ELSE address_requester END))
            "#,
        )
        .bind(address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// `(private, role of a, role of b)` in one read; `None` when the community
    /// row is missing; roles are `None` for an inactive community.
    pub async fn community_voice_roles(
        &self,
        community_id: &str,
        a: &str,
        b: &str,
    ) -> Result<Option<(bool, Option<String>, Option<String>)>, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT c.private, \
                    CASE WHEN c.active THEN (SELECT role FROM social.community_members \
                                             WHERE community_id = c.id AND member_address = $2) END AS role_a, \
                    CASE WHEN c.active THEN (SELECT role FROM social.community_members \
                                             WHERE community_id = c.id AND member_address = $3) END AS role_b \
             FROM social.communities c WHERE c.id = $1",
        )
        .bind(cid)
        .bind(a.to_lowercase())
        .bind(b.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get("private"), r.get("role_a"), r.get("role_b"))))
    }

    /// `(role, banned)` of one address; `None` when the community row is missing.
    pub async fn community_membership(
        &self,
        community_id: &str,
        address: &str,
    ) -> Result<Option<(Option<String>, bool)>, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT CASE WHEN c.active THEN m.role END AS role, COALESCE(b.active, FALSE) AS banned \
             FROM social.communities c \
             LEFT JOIN social.community_members m ON m.community_id = c.id AND m.member_address = $2 \
             LEFT JOIN social.community_bans b ON b.community_id = c.id AND b.banned_address = $2 \
             WHERE c.id = $1",
        )
        .bind(cid)
        .bind(address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get("role"), r.get("banned"))))
    }

    /// `(name, member addresses)` for a voice fan-out; `None` when the
    /// community row is missing.
    pub async fn community_voice_fanout(
        &self,
        community_id: &str,
    ) -> Result<Option<(String, Vec<String>)>, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT c.name, \
                    COALESCE((SELECT array_agg(member_address) FROM social.community_members \
                              WHERE community_id = c.id), '{}'::text[]) AS members \
             FROM social.communities c WHERE c.id = $1",
        )
        .bind(cid)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get("name"), r.get("members"))))
    }

    /// `(role of the actor, name, member addresses)` for starting a voice
    /// chat; the member list is aggregated only when the actor may start one.
    pub async fn community_voice_start(
        &self,
        community_id: &str,
        address: &str,
    ) -> Result<Option<(Option<String>, String, Vec<String>)>, DbError> {
        let Ok(cid) = Uuid::parse_str(community_id) else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT c.name, \
                    CASE WHEN c.active THEN m.role END AS role, \
                    CASE WHEN c.active AND m.role IN ('owner', 'moderator') THEN \
                      COALESCE((SELECT array_agg(member_address) FROM social.community_members \
                                WHERE community_id = c.id), '{}'::text[]) \
                    ELSE '{}'::text[] END AS members \
             FROM social.communities c \
             LEFT JOIN social.community_members m ON m.community_id = c.id AND m.member_address = $2 \
             WHERE c.id = $1",
        )
        .bind(cid)
        .bind(address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get("role"), r.get("name"), r.get("members"))))
    }

    /// `(community_id, member_address)` for every other member of every
    /// active community this address belongs to (the presence fan-out); an
    /// inactive one's members (a deactivated mirrored community keeps them)
    /// are inert.
    pub async fn community_co_members(
        &self,
        address: &str,
    ) -> Result<Vec<(String, String)>, DbError> {
        let rows = sqlx::query(
            "SELECT m.community_id, m.member_address FROM social.community_members m \
             JOIN social.communities c ON c.id = m.community_id AND c.active \
             WHERE m.member_address <> $1 \
               AND m.community_id IN (SELECT community_id FROM social.community_members WHERE member_address = $1)",
        )
        .bind(address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<Uuid, _>("community_id").to_string(),
                    r.get::<String, _>("member_address"),
                )
            })
            .collect())
    }
}
