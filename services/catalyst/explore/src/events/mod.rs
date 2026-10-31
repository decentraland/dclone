//! The events read port over `explore.event` (the mirrored catalogue) and the
//! local overlays (`explore.event_attendance_local`, `explore.moderators`).
//! Occurrences are recomputed from `raw->'recurrent_dates'` at read time, so a
//! recurring event's `next_start_at`/`live` never depend on how fresh the
//! mirror's snapshot columns are.

pub mod attendees;
pub mod categories;
pub mod handlers;
pub mod mirror;
pub mod moderation;
pub mod posters;
pub mod profiles;
mod recurrence;
pub mod schedules;
pub mod sitemap;
pub mod v1;
pub mod writes;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;

use crate::sanitize::{sanitize_asset_url, sanitize_event_description, validate_event_url};
use crate::shared::cache::{TtlCache, cache_get, cache_put};
use crate::shared::errors::ApiError;

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub struct EventRecord {
    pub id: String,
    pub name: String,
    pub image: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub image_vertical: Option<Value>,
    pub description: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub start_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub finish_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub next_start_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub next_finish_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub duration: Option<i64>,
    pub all_day: bool,
    pub x: i32,
    pub y: i32,
    pub server: Option<String>,
    pub url: Option<String>,
    pub user: Option<String>,
    pub user_name: Option<String>,
    pub estate_id: Option<String>,
    pub estate_name: Option<String>,
    pub scene_name: Option<String>,
    pub approved: bool,
    pub rejected: bool,
    pub highlighted: bool,
    pub trending: bool,
    pub world: bool,
    pub recurrent: bool,
    pub recurrent_frequency: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub recurrent_weekday_mask: i64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub recurrent_month_mask: i64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub recurrent_interval: i64,
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub recurrent_setpos: Option<i64>,
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub recurrent_monthday: Option<i64>,
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub recurrent_count: Option<i64>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub recurrent_until: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "Array<string>"))]
    pub recurrent_dates: Vec<DateTime<Utc>>,
    pub categories: Vec<String>,
    pub schedules: Vec<String>,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub total_attendees: i64,
    pub latest_attendees: Vec<String>,
    pub coordinates: [i32; 2],
    pub position: [i32; 2],
    pub live: bool,
    pub attending: bool,
    pub place_id: Option<String>,
    pub community_id: Option<String>,
    pub featured_item: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub created_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub updated_at: Option<DateTime<Utc>>,
    pub approved_by: Option<String>,
    pub rejected_by: Option<String>,
    pub rejection_reason: Option<String>,
    pub deleted_by_user: bool,
    pub deleted_by_admin: bool,
    pub deleted_by: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub deleted_at: Option<DateTime<Utc>>,
    pub deleted_reason: Option<String>,
    pub previous_place_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub connected_addresses: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub struct EventCategoryRecord {
    pub name: String,
    pub active: bool,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub created_at: DateTime<Utc>,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub updated_at: DateTime<Utc>,
    #[cfg_attr(feature = "ts", ts(type = "Record<string, unknown>"))]
    pub i18n: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub struct EventAttendeeRecord {
    pub event_id: String,
    pub user: String,
    pub user_name: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export, export_to = "events/"))]
pub struct ScheduleRecord {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub image: Option<String>,
    pub theme: Option<String>,
    pub background: Vec<String>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub active_since: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub active_until: Option<DateTime<Utc>>,
    pub active: bool,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub created_at: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "ts", ts(type = "string | null"))]
    pub updated_at: Option<DateTime<Utc>>,
}

/// The next upcoming occurrence of an event at a destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextEvent {
    pub id: String,
    pub name: String,
    pub next_start_at: String,
}

pub struct EventsComponent {
    pool: PgPool,
    rewrite_domain: Option<String>,
    moderators: TtlCache<HashSet<String>>,
}

/// Read-side moderator visibility only; writes keep their own gate.
const MODERATOR_SET_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub struct EventListFilters {
    pub limit: i64,
    pub offset: i64,
    pub list: EventListType,
    pub order: SortOrder,
    pub highlighted: Option<bool>,
    pub creator: Option<String>,
    pub world: Option<bool>,
    pub world_names: Vec<String>,
    pub positions: Vec<(i32, i32)>,
    pub estate_id: Option<String>,
    pub community_id: Option<String>,
    pub schedule: Option<String>,
    /// Lists include at most 100 active/future dates, or 1000 when requested.
    pub with_recurrent_dates: bool,
    pub places_ids: Vec<String>,
    pub destination_ids: Vec<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub search: Option<String>,
    pub user: Option<String>,
    pub rejected: Option<bool>,
    pub approved: Option<bool>,
    pub deleted: Option<bool>,
    pub admin: bool,
    pub only_attendee: bool,
    pub owner: bool,
    pub include_own: bool,
    pub include_deleted: bool,
    pub include_unapproved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventListType {
    All,
    #[default]
    Active,
    Live,
    Upcoming,
    Relevance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: String,
    name: String,
    start_at: Option<NaiveDateTime>,
    finish_at: Option<NaiveDateTime>,
    duration_ms: Option<i64>,
    recurrent: bool,
    highlighted: bool,
    trending: bool,
    approved: bool,
    attending: Option<bool>,
    community_id: Option<String>,
    user_creator: Option<String>,
    coordinates_x: Option<i32>,
    coordinates_y: Option<i32>,
    description: Option<String>,
    raw: Value,
    local_attendees: i64,
    #[sqlx(default)]
    total_count: i64,
    #[sqlx(default)]
    viewer_attending: Option<bool>,
}

impl EventsComponent {
    /// `rewrite_domain`: the deployment's own domain, swapped into mirrored
    /// `events-assets-*.decentraland.org` poster URLs (`None` keeps them).
    pub fn new(pool: PgPool, rewrite_domain: Option<String>) -> Self {
        Self {
            pool,
            rewrite_domain,
            moderators: Default::default(),
        }
    }

    pub async fn viewer_is_moderator(&self, address: &str) -> Result<bool, ApiError> {
        let address = address.to_ascii_lowercase();
        if let Some(set) = cache_get(&self.moderators, "all") {
            return Ok(set.contains(&address));
        }
        let rows: Vec<(String,)> = sqlx::query_as("SELECT address FROM explore.moderators")
            .fetch_all(&self.pool)
            .await?;
        let set: HashSet<String> = rows
            .into_iter()
            .map(|(a,)| a.to_ascii_lowercase())
            .collect();
        let hit = set.contains(&address);
        cache_put(&self.moderators, "all".to_string(), set, MODERATOR_SET_TTL);
        Ok(hit)
    }

    fn destination_clause(ids: &[String], binds: &mut Vec<EventBind>) -> String {
        let ids: Vec<String> = ids.iter().map(|d| d.to_lowercase()).collect();
        let p = next_placeholder(binds, EventBind::TextArray(ids));
        format!(
            "(lower(raw->>'place_id') = ANY({p}) \
             OR (COALESCE((raw->>'world')::boolean, false) IS TRUE \
             AND lower(raw->>'server') = ANY({p})))"
        )
    }

    fn build_where(f: &EventListFilters, binds: &mut Vec<EventBind>) -> String {
        let mut sql = String::from(" WHERE 1=1");
        let now = Utc::now();
        let nf = EFF_NEXT_FINISH_SQL;
        let ns = EFF_NEXT_START_SQL;

        let is_owner = f.owner;
        if is_owner {
            match f.user.as_deref() {
                Some(u) => {
                    let p = next_placeholder(binds, EventBind::Text(u.to_lowercase()));
                    sql.push_str(&format!(" AND lower(user_creator) = {p}"));
                }
                None => sql.push_str(" AND FALSE"),
            }
        }

        match f.list {
            EventListType::All | EventListType::Relevance => {}
            EventListType::Active => {
                let p = next_placeholder(binds, EventBind::Time(now));
                sql.push_str(&format!(" AND {nf} > {p}"));
            }
            EventListType::Live => {
                let p1 = next_placeholder(binds, EventBind::Time(now));
                let p2 = next_placeholder(binds, EventBind::Time(now));
                sql.push_str(&format!(" AND {nf} > {p1} AND {ns} < {p2}"));
            }
            EventListType::Upcoming => {
                let p1 = next_placeholder(binds, EventBind::Time(now));
                let p2 = next_placeholder(binds, EventBind::Time(now));
                sql.push_str(&format!(" AND {nf} > {p1} AND {ns} > {p2}"));
            }
        }

        if !is_owner && let Some(c) = &f.creator {
            let p = next_placeholder(binds, EventBind::Text(c.to_lowercase()));
            sql.push_str(&format!(" AND lower(user_creator) = {p}"));
        }
        if let Some(eid) = &f.estate_id {
            let p = next_placeholder(binds, EventBind::Text(eid.clone()));
            sql.push_str(&format!(" AND raw->>'estate_id' = {p}"));
        }
        if let Some(schedule) = &f.schedule {
            let p = next_placeholder(binds, EventBind::Text(schedule.clone()));
            sql.push_str(&format!(" AND raw->'schedules' ? {p}"));
        }
        if f.highlighted == Some(true) {
            sql.push_str(" AND highlighted IS TRUE");
        }
        match f.world {
            Some(true) => sql.push_str(" AND COALESCE((raw->>'world')::boolean, false) IS TRUE"),
            Some(false) => sql.push_str(" AND COALESCE((raw->>'world')::boolean, false) IS FALSE"),
            None => {}
        }
        if !f.world_names.is_empty() {
            let p = next_placeholder(binds, EventBind::TextArray(f.world_names.clone()));
            sql.push_str(&format!(" AND raw->>'server' = ANY({p})"));
        }
        if !f.positions.is_empty() {
            let mut clauses: Vec<String> = Vec::new();
            for (x, y) in &f.positions {
                let px = next_placeholder(binds, EventBind::Int(*x));
                let py = next_placeholder(binds, EventBind::Int(*y));
                clauses.push(format!("(coordinates_x = {px} AND coordinates_y = {py})"));
            }
            sql.push_str(&format!(" AND ({})", clauses.join(" OR ")));
        }

        let mut by_destination: Vec<String> = Vec::new();
        if !f.places_ids.is_empty() {
            by_destination.push(Self::destination_clause(&f.places_ids, binds));
        }
        if let Some(c) = &f.community_id {
            let pc = next_placeholder(binds, EventBind::Text(c.clone()));
            by_destination.push(format!("community_id = {pc}"));
        }
        if !by_destination.is_empty() {
            sql.push_str(&format!(" AND ({})", by_destination.join(" OR ")));
        }
        if !f.destination_ids.is_empty() {
            let clause = Self::destination_clause(&f.destination_ids, binds);
            sql.push_str(&format!(" AND {clause}"));
        }

        if let Some(from) = f.from {
            let p = next_placeholder(binds, EventBind::Time(from));
            sql.push_str(&format!(" AND {ns} >= {p}"));
        }
        if let Some(to) = f.to {
            let p = next_placeholder(binds, EventBind::Time(to));
            sql.push_str(&format!(" AND {ns} < {p}"));
        }

        if let Some(s) = &f.search {
            let p = next_placeholder(binds, EventBind::Text(to_tsquery(s)));
            sql.push_str(&format!(
                " AND ts_rank_cd({TEXTSEARCH_EXPR}, to_tsquery('english', {p})) > 0"
            ));
        }

        let own_viewer = f
            .user
            .as_deref()
            .filter(|_| !f.include_unapproved && f.include_own);
        if let (false, Some(u)) = (is_owner, own_viewer) {
            let p = next_placeholder(binds, EventBind::Text(u.to_lowercase()));
            sql.push_str(&format!(
                " AND ((approved IS TRUE AND COALESCE((raw->>'rejected')::boolean, false) IS FALSE) \
                 OR lower(user_creator) = {p})"
            ));
        } else if !is_owner {
            match (f.include_unapproved, f.rejected) {
                (_, Some(true)) => {
                    sql.push_str(" AND COALESCE((raw->>'rejected')::boolean, false) IS TRUE")
                }
                (_, Some(false)) | (false, None) => {
                    sql.push_str(" AND COALESCE((raw->>'rejected')::boolean, false) IS FALSE")
                }
                (true, None) => {}
            }
            match (f.include_unapproved, f.approved) {
                (_, Some(true)) | (false, None) => sql.push_str(" AND approved IS TRUE"),
                (_, Some(false)) => sql.push_str(" AND approved IS FALSE"),
                (true, None) => {}
            }
        }

        if f.only_attendee
            && let Some(u) = &f.user
        {
            let p1 = next_placeholder(binds, EventBind::Text(u.to_lowercase()));
            let p2 = next_placeholder(binds, EventBind::Text(u.to_lowercase()));
            sql.push_str(&format!(
                " AND (id IN (SELECT event_id FROM explore.event_attendance_local WHERE signer = {p1} AND action = 'going') OR raw->'latest_attendees' ? {p2})"
            ));
        }

        if let Some(own) = crate::copies::origin::event_rows_sql() {
            sql.push_str(" AND ");
            sql.push_str(own);
        }

        match (f.admin, f.deleted) {
            (true, Some(true)) => sql.push_str(DELETED_ONLY_SQL),
            (true, None) if f.include_deleted => {}
            _ => sql.push_str(NOT_DELETED_SQL),
        }
        sql
    }

    pub async fn query(
        &self,
        f: &EventListFilters,
        with_total: bool,
    ) -> Result<(Vec<EventRecord>, i64), ApiError> {
        let mut binds: Vec<EventBind> = Vec::new();
        let sql = build_list_sql(f, with_total, &mut binds);
        let mut q = sqlx::query_as::<_, EventRow>(sqlx::AssertSqlSafe(sql));
        for b in &binds {
            q = bind_one(q, b);
        }
        let rows = q.fetch_all(&self.pool).await?;
        let total = if with_total {
            match rows.first() {
                Some(r) => r.total_count,
                None if f.offset > 0 || f.limit <= 0 => self.count_only(f).await,
                None => 0,
            }
        } else {
            0
        };
        let user = f.user.as_deref();
        let records = rows
            .into_iter()
            .map(|r| event_row_to_record(r, user, self.rewrite_domain.as_deref()))
            .collect();
        Ok((records, total))
    }

    async fn count_only(&self, f: &EventListFilters) -> i64 {
        let mut cbinds: Vec<EventBind> = Vec::new();
        let count_sql = count_only_sql(f, &mut cbinds);
        let mut cq = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
        for b in &cbinds {
            cq = bind_one_scalar(cq, b);
        }
        cq.fetch_one(&self.pool).await.unwrap_or(0)
    }

    pub async fn get(&self, event_id: &str) -> Result<Option<EventRecord>, ApiError> {
        let row = sqlx::query_as::<_, EventRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM explore.event WHERE id = $1"
        )))
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| event_row_to_record(r, None, self.rewrite_domain.as_deref())))
    }

    /// `get` plus the viewer's attendance (local RSVP or upstream attendee) in one statement.
    pub async fn get_for_viewer(
        &self,
        event_id: &str,
        viewer: Option<&str>,
    ) -> Result<Option<EventRecord>, ApiError> {
        let Some(viewer) = viewer else {
            return self.get(event_id).await;
        };
        let row = sqlx::query_as::<_, EventRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS}, (EXISTS ( \
               SELECT 1 FROM explore.event_attendance_local al \
               WHERE al.event_id = event.id AND al.signer = $2 AND al.action = 'going' \
             ) OR raw->'latest_attendees' ? $2) AS viewer_attending \
             FROM explore.event WHERE id = $1"
        )))
        .bind(event_id)
        .bind(viewer.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            let attending = r.viewer_attending.unwrap_or(false);
            let mut rec = event_row_to_record(r, None, self.rewrite_domain.as_deref());
            rec.attending = attending;
            rec
        }))
    }

    pub async fn attending(&self, user: &str) -> Result<Vec<EventRecord>, ApiError> {
        let user_lc = user.to_lowercase();
        let rows = sqlx::query_as::<_, EventRow>(sqlx::AssertSqlSafe(attending_sql()))
            .bind(&user_lc)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let mut rec =
                    event_row_to_record(r, Some(&user_lc), self.rewrite_domain.as_deref());
                rec.attending = true;
                rec
            })
            .collect())
    }

    pub async fn moderation_pending(&self, limit: i64) -> Result<Vec<EventRecord>, ApiError> {
        let limit = limit.clamp(0, 500);
        let columns = list_event_columns(false);
        let rows = sqlx::query_as::<_, EventRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {columns} FROM explore.event \
             WHERE approved IS NOT TRUE \
                OR COALESCE((raw->>'rejected')::boolean, false) IS TRUE \
             ORDER BY next_start_at DESC NULLS LAST, id ASC \
             LIMIT $1"
        )))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| event_row_to_record(r, None, self.rewrite_domain.as_deref()))
            .collect())
    }

    /// The live event (if any) at each destination key: a place id, or a world
    /// name for a world event. `None` under a key means nothing is live there.
    pub async fn live_event_names(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<String>>, ApiError> {
        let mut out: HashMap<String, Option<String>> =
            keys.iter().map(|k| (k.clone(), None)).collect();
        if keys.is_empty() {
            return Ok(out);
        }
        let rows = self.events_at(keys, EventListType::Live).await?;
        let asked = AskedKeys::new(keys);
        for ev in rows {
            let Some(key) = asked.resolve(&ev) else {
                continue;
            };
            if out.get(&key).is_some_and(Option::is_some) {
                continue;
            }
            out.insert(key, ev.name.filter(|n| !n.is_empty()));
        }
        Ok(out)
    }

    /// The first upcoming occurrence at each destination key that has one.
    pub async fn next_events(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, NextEvent>, ApiError> {
        let mut out: HashMap<String, NextEvent> = HashMap::new();
        if keys.is_empty() {
            return Ok(out);
        }
        let rows = self.events_at(keys, EventListType::Upcoming).await?;
        let asked = AskedKeys::new(keys);
        for ev in rows {
            let Some(key) = asked.resolve(&ev) else {
                continue;
            };
            let (Some(name), Some(next_start_at)) = (ev.name, ev.next_start_at) else {
                continue;
            };
            out.entry(key).or_insert(NextEvent {
                id: ev.id,
                name,
                next_start_at: next_start_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            });
        }
        Ok(out)
    }

    /// Public (approved, not rejected, not deleted) events at the given
    /// destinations, earliest next occurrence first.
    async fn events_at(
        &self,
        keys: &[String],
        list: EventListType,
    ) -> Result<Vec<EventAt>, ApiError> {
        let f = EventListFilters {
            list,
            destination_ids: keys.to_vec(),
            ..Default::default()
        };
        let mut binds: Vec<EventBind> = Vec::new();
        let where_sql = Self::build_where(&f, &mut binds);
        let sql = format!(
            "SELECT id, name, raw->>'place_id' AS place_id, raw->>'server' AS server, \
                    COALESCE((raw->>'world')::boolean, false) AS world, \
                    {EFF_NEXT_START_SQL} AS next_start_at \
             FROM explore.event{where_sql} \
             ORDER BY {EFF_NEXT_START_SQL} ASC NULLS LAST, id ASC LIMIT 2500"
        );
        let mut q = sqlx::query_as::<_, EventAt>(sqlx::AssertSqlSafe(sql));
        for b in &binds {
            q = bind_one(q, b);
        }
        Ok(q.fetch_all(&self.pool).await?)
    }
}

#[derive(sqlx::FromRow)]
struct EventAt {
    id: String,
    name: Option<String>,
    place_id: Option<String>,
    server: Option<String>,
    world: bool,
    next_start_at: Option<DateTime<Utc>>,
}

/// The keys a caller asked for, matched case-insensitively: a place by its
/// wire place_id, a world by its server name (only when the event is a world
/// event), and reported back under the spelling the caller used.
struct AskedKeys(HashMap<String, String>);

impl AskedKeys {
    fn new(asked: &[String]) -> Self {
        Self(
            asked
                .iter()
                .map(|key| (key.to_lowercase(), key.clone()))
                .collect(),
        )
    }

    fn resolve(&self, ev: &EventAt) -> Option<String> {
        let by_place = ev
            .place_id
            .as_deref()
            .and_then(|id| self.0.get(&id.to_lowercase()));
        let by_world = ev
            .server
            .as_deref()
            .filter(|_| ev.world)
            .and_then(|name| self.0.get(&name.to_lowercase()));
        by_place.or(by_world).cloned()
    }
}

/// Column list shared by every event read query.
/// `local_attendees`: this node's RSVPs, which `total_attendees` adds to the
/// upstream's count (a mirrored event's); a signer the upstream's
/// `latest_attendees` already names is counted there.
const EVENT_COLUMNS: &str = "id, name, start_at, finish_at, duration_ms, recurrent, highlighted, trending, \
     approved, attending, community_id, user_creator, coordinates_x, coordinates_y, \
     description, raw, \
     (SELECT count(*) FROM explore.event_attendance_local la \
      WHERE la.event_id = event.id AND la.action = 'going' \
        AND NOT (COALESCE(event.raw->'latest_attendees', '[]') ? la.signer)) AS local_attendees";

// Keep a bounded active/future date window for existing list consumers.
// Effective next_start_at still uses the complete series, before projection.
fn list_event_columns(include_dates: bool) -> String {
    let date_limit = if include_dates { 1000 } else { 100 };
    let dates = format!(
        "COALESCE((SELECT jsonb_agg(upcoming.starts_at ORDER BY upcoming.starts_at) \
         FROM (SELECT d.starts_at FROM ({VALID_RECURRENCE_DATES_SQL}) d \
           WHERE d.starts_at + COALESCE(duration_ms,0) * interval '1 millisecond' > now() \
           ORDER BY d.starts_at LIMIT {date_limit}) upcoming),'[]'::jsonb)"
    );
    EVENT_COLUMNS.replace(", raw,", &format!(
        ", ((raw - 'recurrent_dates') || jsonb_build_object('next_start_at', ({EFF_NEXT_START_SQL})::timestamptz, 'recurrent_dates', {dates})) AS raw,"
    ))
}

const NOT_DELETED_SQL: &str = " AND (raw->>'deleted_by_user') IS DISTINCT FROM 'true' \
     AND (raw->>'deleted_by_admin') IS DISTINCT FROM 'true'";

const DELETED_ONLY_SQL: &str = " AND ((raw->>'deleted_by_user') = 'true' \
     OR (raw->>'deleted_by_admin') = 'true')";

/// The effective next occurrence, upstream's `calculateNextRecurrentDates`
/// as its cron leaves the stored column: the stored `next_start_at` while its
/// occurrence has not finished (an occurrence the creator's edit dropped from
/// `recurrent_dates` keeps running), else the first recurrence that has not
/// finished, else the last recurrence, else the snapshot columns. `now()` and
/// the recurrence strings are timestamptz, so the snapshot columns (UTC
/// timestamps) join them under the pool's UTC session.
macro_rules! valid_recurrence_dates_sql {
    () => {
        "SELECT parsed.starts_at FROM ( \
           SELECT CASE WHEN pg_input_is_valid(value,'timestamp with time zone') \
             THEN value::timestamptz END AS starts_at \
           FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(raw->'recurrent_dates')='array' \
             THEN raw->'recurrent_dates' ELSE '[]'::jsonb END) \
         ) parsed WHERE parsed.starts_at IS NOT NULL AND isfinite(parsed.starts_at)"
    };
}
const VALID_RECURRENCE_DATES_SQL: &str = valid_recurrence_dates_sql!();

macro_rules! eff_next_start_sql {
    () => {
        concat!(
            "CASE WHEN next_start_at + COALESCE(duration_ms, 0) * interval '1 millisecond' > now() \
             THEN next_start_at ELSE COALESCE((SELECT min(d.starts_at) FROM (",
            valid_recurrence_dates_sql!(),
            ") d WHERE d.starts_at + COALESCE(duration_ms, 0) * interval '1 millisecond' > now()), \
             (SELECT max(d.starts_at) FROM (",
            valid_recurrence_dates_sql!(),
            ") d), next_start_at, start_at) END"
        )
    };
}

const EFF_NEXT_START_SQL: &str = eff_next_start_sql!();

/// `next_finish_at` is the effective start plus the duration (upstream's
/// `Time(next_start_at).add(duration)`).
const EFF_NEXT_FINISH_SQL: &str = concat!(
    "(",
    eff_next_start_sql!(),
    ") + COALESCE(duration_ms, 0) * interval '1 millisecond'"
);

fn next_start_order_by(order: SortOrder) -> String {
    let dir = match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    format!(" ORDER BY {EFF_NEXT_START_SQL} {dir} NULLS LAST, id ASC")
}

fn attending_sql() -> String {
    let columns = list_event_columns(false);
    format!(
        "SELECT {columns} FROM explore.event \
         WHERE {EFF_NEXT_FINISH_SQL} > now() \
           AND COALESCE((raw->>'rejected')::boolean, false) IS FALSE \
           AND (raw->>'deleted_by_user') IS DISTINCT FROM 'true' \
           AND (raw->>'deleted_by_admin') IS DISTINCT FROM 'true' \
           AND ( \
             id IN (SELECT event_id FROM explore.event_attendance_local WHERE signer = $1 AND action = 'going') \
             OR raw->'latest_attendees' ? $1 \
           ) \
         ORDER BY {EFF_NEXT_START_SQL} ASC NULLS LAST, id ASC"
    )
}

const TEXTSEARCH_EXPR: &str = "(setweight(to_tsvector('english', coalesce(name,'')), 'A') || \
     setweight(to_tsvector('english', coalesce(raw->>'user_name','')), 'B') || \
     setweight(to_tsvector('english', coalesce(raw->>'estate_name','')), 'B') || \
     setweight(to_tsvector('english', coalesce(description,'')), 'D'))";

fn to_tsquery(input: &str) -> String {
    let terms: Vec<String> = input
        .split_whitespace()
        .map(|t| {
            t.chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>()
        })
        .filter(|t| !t.is_empty())
        .map(|t| format!("{t}:*"))
        .collect();
    terms.join(" & ")
}

#[derive(Debug)]
enum EventBind {
    Text(String),
    Int(i32),
    Int64(i64),
    Time(DateTime<Utc>),
    TextArray(Vec<String>),
}

fn next_placeholder(binds: &mut Vec<EventBind>, bind: EventBind) -> String {
    binds.push(bind);
    format!("${}", binds.len())
}

/// Assemble the paginated list query. When `with_total`, the pre-LIMIT row count rides along as
/// `count(*) OVER() AS total_count` -- same semantics as a separate `count(*)` over the same
/// WHERE, no extra bind parameters, and it changes ONLY the select list.
fn build_list_sql(f: &EventListFilters, with_total: bool, binds: &mut Vec<EventBind>) -> String {
    let where_sql = EventsComponent::build_where(f, binds);
    let extra = if with_total {
        ", count(*) OVER() AS total_count"
    } else {
        ""
    };
    let viewer = match &f.user {
        Some(u) => {
            let p = next_placeholder(binds, EventBind::Text(u.to_lowercase()));
            format!(
                ", EXISTS (SELECT 1 FROM explore.event_attendance_local al \
                 WHERE al.event_id = event.id AND al.signer = {p} AND al.action = 'going') \
                 AS viewer_attending"
            )
        }
        None => String::new(),
    };
    let order_clause = if let Some(s) = &f.search {
        let dir = if matches!(f.order, SortOrder::Asc) {
            "ASC"
        } else {
            "DESC"
        };
        let p = next_placeholder(binds, EventBind::Text(to_tsquery(s)));
        format!(" ORDER BY ts_rank_cd({TEXTSEARCH_EXPR}, to_tsquery('english', {p})) {dir}, id ASC")
    } else {
        next_start_order_by(f.order)
    };
    let columns = list_event_columns(f.with_recurrent_dates);
    let limit = f
        .limit
        .clamp(0, if f.with_recurrent_dates { 25 } else { 500 });
    let lim_p = next_placeholder(binds, EventBind::Int64(limit));
    let off_p = next_placeholder(binds, EventBind::Int64(f.offset.max(0)));
    format!(
        "SELECT {columns}{extra}{viewer} FROM explore.event{where_sql}{order_clause} LIMIT {lim_p} OFFSET {off_p}"
    )
}

/// The rare-path count, unchanged from the old two-query design: recovers the true total for an
/// empty page that ran past the offset.
fn count_only_sql(f: &EventListFilters, binds: &mut Vec<EventBind>) -> String {
    let where_sql = EventsComponent::build_where(f, binds);
    format!("SELECT count(*) FROM explore.event{where_sql}")
}

fn bind_one<'q, R>(
    q: sqlx::query::QueryAs<'q, sqlx::Postgres, R, sqlx::postgres::PgArguments>,
    b: &'q EventBind,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, R, sqlx::postgres::PgArguments> {
    match b {
        EventBind::Text(s) => q.bind(s),
        EventBind::Int(i) => q.bind(i),
        EventBind::Int64(i) => q.bind(i),
        EventBind::Time(t) => q.bind(t),
        EventBind::TextArray(v) => q.bind(v),
    }
}

fn bind_one_scalar<'q>(
    q: sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    b: &'q EventBind,
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
    match b {
        EventBind::Text(s) => q.bind(s),
        EventBind::Int(i) => q.bind(i),
        EventBind::Int64(i) => q.bind(i),
        EventBind::Time(t) => q.bind(t),
        EventBind::TextArray(v) => q.bind(v),
    }
}

fn rewrite_asset_host(url: &str, domain: &str) -> Option<String> {
    let rest = url.strip_prefix("https://")?;
    let (host, path) = rest.split_once('/')?;
    let bucket = host.strip_suffix(".decentraland.org")?;
    bucket
        .starts_with("events-assets-")
        .then(|| format!("https://{bucket}.{domain}/{path}"))
}

fn rewrite_asset_url(url: &str, domain: Option<&str>) -> String {
    domain
        .and_then(|d| rewrite_asset_host(url, d))
        .unwrap_or_else(|| url.to_string())
}

fn event_row_to_record(
    r: EventRow,
    attending_user: Option<&str>,
    rewrite_domain: Option<&str>,
) -> EventRecord {
    let raw = &r.raw;
    let start_at = r.start_at.map(|n| n.and_utc());
    let finish_at = r.finish_at.map(|n| n.and_utc());
    let x = r.coordinates_x.unwrap_or(0);
    let y = r.coordinates_y.unwrap_or(0);
    let image = raw
        .get("image")
        .and_then(|v| v.as_str())
        .and_then(|s| sanitize_asset_url(s).ok())
        .map(|s| rewrite_asset_url(&s, rewrite_domain));
    let image_vertical = raw.get("image_vertical").cloned().and_then(|v| match v {
        Value::String(s) => sanitize_asset_url(&s)
            .ok()
            .map(|safe| Value::String(rewrite_asset_url(&safe, rewrite_domain))),
        other => Some(other),
    });
    let server = raw_str(raw, "server");
    let url = raw
        .get("url")
        .and_then(|v| v.as_str())
        .and_then(|s| validate_event_url(s).ok());
    let user = raw_str(raw, "user").or_else(|| r.user_creator.clone());
    let scene_name = raw_str(raw, "scene_name");
    let estate_name = raw_str(raw, "estate_name").or_else(|| scene_name.clone());
    let world = raw.get("world").and_then(|v| v.as_bool()).unwrap_or(false);
    let duration = r
        .duration_ms
        .or_else(|| raw.get("duration").and_then(|v| v.as_i64()));
    let recurrent_dates: Vec<DateTime<Utc>> = raw
        .get("recurrent_dates")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|x| parse_dt(Some(x))).collect())
        .unwrap_or_default();
    let occurrence_span = chrono::Duration::milliseconds(duration.unwrap_or(0));
    let now = Utc::now();
    let stored_next = parse_dt(raw.get("next_start_at"));
    let next_start_at = match stored_next {
        Some(ns) if ns + occurrence_span > now => Some(ns),
        _ => recurrent_dates
            .iter()
            .copied()
            .filter(|d| *d + occurrence_span > now)
            .min()
            .or_else(|| recurrent_dates.iter().copied().max())
            .or(stored_next)
            .or(start_at),
    };
    let next_finish_at = next_start_at.map(|d| d + occurrence_span);
    let live = match (next_start_at, duration) {
        (Some(ns), Some(d)) => now >= ns && now < ns + chrono::Duration::milliseconds(d),
        _ => raw.get("live").and_then(|v| v.as_bool()).unwrap_or(false),
    };
    let attending = match attending_user {
        Some(u) => {
            r.viewer_attending.unwrap_or(false)
                || raw
                    .get("latest_attendees")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter().any(|x| {
                            x.as_str()
                                .map(|s| s.eq_ignore_ascii_case(u))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
        }
        None => r.attending.unwrap_or(false),
    };
    let str_list = |key: &str| -> Vec<String> {
        raw.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };

    EventRecord {
        id: r.id,
        name: r.name,
        image,
        image_vertical,
        description: r
            .description
            .or_else(|| raw_str(raw, "description"))
            .map(|d| sanitize_event_description(&d)),
        start_at,
        finish_at,
        next_start_at,
        next_finish_at,
        duration,
        all_day: raw
            .get("all_day")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        x,
        y,
        server,
        url,
        user,
        user_name: raw_str(raw, "user_name"),
        estate_id: raw_str(raw, "estate_id"),
        estate_name,
        scene_name,
        approved: r.approved,
        rejected: raw
            .get("rejected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        highlighted: r.highlighted,
        trending: r.trending,
        world,
        recurrent: r.recurrent,
        recurrent_frequency: raw_str(raw, "recurrent_frequency"),
        recurrent_weekday_mask: raw
            .get("recurrent_weekday_mask")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        recurrent_month_mask: raw
            .get("recurrent_month_mask")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        recurrent_interval: raw
            .get("recurrent_interval")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        recurrent_setpos: raw.get("recurrent_setpos").and_then(|v| v.as_i64()),
        recurrent_monthday: raw.get("recurrent_monthday").and_then(|v| v.as_i64()),
        recurrent_count: raw.get("recurrent_count").and_then(|v| v.as_i64()),
        recurrent_until: parse_dt(raw.get("recurrent_until")),
        recurrent_dates,
        categories: str_list("categories"),
        schedules: str_list("schedules"),
        total_attendees: raw
            .get("total_attendees")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .max(0)
            + r.local_attendees,
        latest_attendees: str_list("latest_attendees"),
        coordinates: [x, y],
        position: [x, y],
        live,
        attending,
        place_id: raw_str(raw, "place_id"),
        community_id: r.community_id,
        featured_item: raw_str(raw, "featured_item"),
        created_at: parse_dt(raw.get("created_at")),
        updated_at: parse_dt(raw.get("updated_at")),
        approved_by: raw_str(raw, "approved_by"),
        rejected_by: raw_str(raw, "rejected_by"),
        rejection_reason: raw_str(raw, "rejection_reason"),
        deleted_by_user: raw
            .get("deleted_by_user")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        deleted_by_admin: raw
            .get("deleted_by_admin")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        deleted_by: raw_str(raw, "deleted_by"),
        deleted_at: parse_dt(raw.get("deleted_at")),
        deleted_reason: raw_str(raw, "deleted_reason"),
        previous_place_id: raw_str(raw, "previous_place_id"),
        connected_addresses: None,
    }
}

fn raw_str(raw: &Value, key: &str) -> Option<String> {
    raw.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn parse_dt(v: Option<&Value>) -> Option<DateTime<Utc>> {
    let s = v?.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn event_connected_users_uses_shared_rooms_and_deduplicates_locations() {
        struct Participants(std::sync::Mutex<Vec<(bool, String)>>);
        impl crate::places::live::Participants for Participants {
            fn participants<'a>(
                &'a self,
                world: bool,
                key: &'a str,
            ) -> futures::future::BoxFuture<'a, Option<Vec<String>>> {
                Box::pin(async move {
                    self.0.lock().unwrap().push((world, key.into()));
                    Some(vec![if world { "0xworld" } else { "0xparcel" }.into()])
                })
            }
        }
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let live = std::sync::Arc::new(crate::places::live::Live::new(pool));
        let participants = std::sync::Arc::new(Participants(Default::default()));
        live.set_participants(participants.clone());
        let mut events = vec![
            event_row_to_record(row_with(json!({"x":10,"y":20}), None, None), None, None),
            event_row_to_record(row_with(json!({"x":10,"y":20}), None, None), None, None),
            event_row_to_record(
                row_with(json!({"world":true,"server":"MyWorld.DCL.eth"}), None, None),
                None,
                None,
            ),
        ];
        super::handlers::fill_connected(&mut events, &live).await;
        assert_eq!(events[0].connected_addresses, Some(vec!["0xparcel".into()]));
        assert_eq!(events[1].connected_addresses, events[0].connected_addresses);
        assert_eq!(events[2].connected_addresses, Some(vec!["0xworld".into()]));
        let calls = participants.0.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.contains(&(true, "myworld.dcl.eth".into())));
    }

    const DELETED_USER_CLAUSE: &str = "(raw->>'deleted_by_user') IS DISTINCT FROM 'true'";
    const DELETED_ADMIN_CLAUSE: &str = "(raw->>'deleted_by_admin') IS DISTINCT FROM 'true'";

    fn row_with(raw: Value, start_at: Option<DateTime<Utc>>, duration_ms: Option<i64>) -> EventRow {
        EventRow {
            id: "e1".into(),
            name: "Weekly Show".into(),
            start_at: start_at.map(|s| s.naive_utc()),
            finish_at: start_at.map(|s| {
                (s + chrono::Duration::milliseconds(duration_ms.unwrap_or(0))).naive_utc()
            }),
            duration_ms,
            recurrent: true,
            highlighted: false,
            trending: false,
            approved: true,
            attending: None,
            community_id: None,
            user_creator: None,
            coordinates_x: Some(0),
            coordinates_y: Some(0),
            description: None,
            raw,
            local_attendees: 0,
            total_count: 0,
            viewer_attending: None,
        }
    }

    #[test]
    fn next_start_order_by_and_attending_sql_include_the_id_tiebreaker() {
        for order in [SortOrder::Asc, SortOrder::Desc] {
            let sql = next_start_order_by(order);
            assert!(sql.trim_end().ends_with(", id ASC"), "{sql}");
        }
        assert!(attending_sql().contains("NULLS LAST, id ASC"));
    }

    #[test]
    fn rewrite_asset_host_swaps_only_the_upstream_poster_bucket() {
        assert_eq!(
            rewrite_asset_host(
                "https://events-assets-099ac00.decentraland.org/poster/abc.webp",
                "interconnected.online"
            )
            .as_deref(),
            Some("https://events-assets-099ac00.interconnected.online/poster/abc.webp")
        );
        for untouched in [
            "https://peer.decentraland.org/content/contents/x",
            "https://example.com/poster/abc.webp",
            "http://events-assets-099ac00.decentraland.org/poster/abc.webp",
            "/poster/abc.webp",
        ] {
            assert_eq!(
                rewrite_asset_host(untouched, "interconnected.online"),
                None,
                "{untouched}"
            );
        }
        let raw = json!({
            "image": "https://events-assets-099ac00.decentraland.org/poster/d2dfb36b6a012314.jpg",
            "image_vertical": "https://events-assets-099ac00.decentraland.org/poster/v.webp",
        });
        let rec = event_row_to_record(
            row_with(raw.clone(), None, None),
            None,
            Some("interconnected.online"),
        );
        assert_eq!(
            rec.image.as_deref(),
            Some("https://events-assets-099ac00.interconnected.online/poster/d2dfb36b6a012314.jpg")
        );
        assert_eq!(
            rec.image_vertical,
            Some(Value::String(
                "https://events-assets-099ac00.interconnected.online/poster/v.webp".into()
            ))
        );
        let rec = event_row_to_record(row_with(raw, None, None), None, None);
        assert_eq!(
            rec.image.as_deref(),
            Some("https://events-assets-099ac00.decentraland.org/poster/d2dfb36b6a012314.jpg")
        );
    }

    #[test]
    fn event_record_surfaces_featured_item_from_raw() {
        let urn =
            "urn:decentraland:matic:collections-v2:0x1234567890abcdef1234567890abcdef12345678:1";
        let rec = event_row_to_record(
            row_with(json!({ "featured_item": urn }), None, None),
            None,
            None,
        );
        assert_eq!(rec.featured_item.as_deref(), Some(urn));
        for absent in [json!({}), json!({ "featured_item": null })] {
            let rec = event_row_to_record(row_with(absent, None, None), None, None);
            assert_eq!(rec.featured_item, None);
        }
    }

    #[test]
    fn build_where_excludes_soft_deleted_for_every_list_type_and_appends_it_last() {
        for list in [
            EventListType::All,
            EventListType::Active,
            EventListType::Live,
            EventListType::Upcoming,
        ] {
            let f = EventListFilters {
                list,
                ..Default::default()
            };
            let mut binds = Vec::new();
            let sql = EventsComponent::build_where(&f, &mut binds);
            assert!(sql.contains(DELETED_USER_CLAUSE), "{list:?}: {sql}");
            assert!(sql.contains(DELETED_ADMIN_CLAUSE), "{list:?}: {sql}");
        }
        let f = EventListFilters {
            creator: Some("0xABC".into()),
            community_id: Some("c1".into()),
            highlighted: Some(true),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&f, &mut binds);
        assert!(sql.ends_with(NOT_DELETED_SQL), "{sql}");
    }

    #[test]
    fn build_where_lets_only_an_admin_opt_into_deleted_rows() {
        let opted_in = EventListFilters {
            admin: true,
            include_deleted: true,
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&opted_in, &mut binds);
        assert!(
            !sql.contains(NOT_DELETED_SQL) && !sql.contains(DELETED_ONLY_SQL),
            "{sql}"
        );

        for guarded in [
            EventListFilters {
                include_deleted: true,
                ..Default::default()
            },
            EventListFilters {
                admin: true,
                include_deleted: true,
                deleted: Some(false),
                ..Default::default()
            },
            EventListFilters {
                admin: true,
                ..Default::default()
            },
        ] {
            let mut binds = Vec::new();
            let sql = EventsComponent::build_where(&guarded, &mut binds);
            assert!(sql.ends_with(NOT_DELETED_SQL), "{sql}");
        }
        let only_deleted = EventListFilters {
            admin: true,
            include_deleted: true,
            deleted: Some(true),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&only_deleted, &mut binds);
        assert!(sql.ends_with(DELETED_ONLY_SQL), "{sql}");
    }

    #[test]
    fn date_filters_use_recomputed_occurrences() {
        for list in [
            EventListType::Active,
            EventListType::Live,
            EventListType::Upcoming,
        ] {
            let f = EventListFilters {
                list,
                ..Default::default()
            };
            let mut binds = Vec::new();
            let sql = EventsComponent::build_where(&f, &mut binds);
            assert!(sql.contains("recurrent_dates"), "{list:?}: {sql}");
        }
        assert!(attending_sql().contains("recurrent_dates"));
    }

    const PUBLIC_ONLY_CLAUSE: &str =
        " AND COALESCE((raw->>'rejected')::boolean, false) IS FALSE AND approved IS TRUE";
    const APPROVED_OR_MINE_CLAUSE: &str = " AND ((approved IS TRUE AND COALESCE((raw->>'rejected')::boolean, false) IS FALSE) OR lower(user_creator) = ";

    #[test]
    fn a_signed_viewer_sees_their_own_pending_events_and_an_admin_list_is_unfiltered() {
        let f = EventListFilters {
            user: Some("0xABC".into()),
            include_own: true,
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&f, &mut binds);
        assert!(sql.contains(APPROVED_OR_MINE_CLAUSE), "{sql}");
        assert!(!sql.contains(PUBLIC_ONLY_CLAUSE), "{sql}");
        assert!(
            matches!(binds.last(), Some(EventBind::Text(u)) if u == "0xabc"),
            "{binds:?}"
        );

        for f in [
            EventListFilters::default(),
            EventListFilters {
                user: Some("0xABC".into()),
                ..Default::default()
            },
            EventListFilters {
                include_own: true,
                ..Default::default()
            },
        ] {
            let mut binds = Vec::new();
            let sql = EventsComponent::build_where(&f, &mut binds);
            assert!(sql.contains(PUBLIC_ONLY_CLAUSE), "{sql}");
            assert!(!sql.contains("lower(user_creator)"), "{sql}");
        }
        let admin = EventListFilters {
            user: Some("0xABC".into()),
            include_own: true,
            admin: true,
            include_unapproved: true,
            ..Default::default()
        };
        let mut binds = Vec::new();
        assert!(!EventsComponent::build_where(&admin, &mut binds).contains("approved"));

        let moderator = EventListFilters {
            user: Some("0xABC".into()),
            include_own: true,
            include_unapproved: true,
            include_deleted: true,
            deleted: Some(true),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&moderator, &mut binds);
        assert!(
            !sql.contains("approved") && !sql.contains("lower(user_creator)"),
            "{sql}"
        );
        assert!(
            sql.ends_with(NOT_DELETED_SQL) && !sql.contains(DELETED_ONLY_SQL),
            "{sql}"
        );
        let bearer_only = EventListFilters {
            admin: true,
            ..Default::default()
        };
        let mut binds = Vec::new();
        assert!(
            EventsComponent::build_where(&bearer_only, &mut binds).contains(PUBLIC_ONLY_CLAUSE)
        );
    }

    const DESTINATION_CLAUSE: &str = "(lower(raw->>'place_id') = ANY($1) \
     OR (COALESCE((raw->>'world')::boolean, false) IS TRUE \
     AND lower(raw->>'server') = ANY($1)))";

    #[test]
    fn a_world_event_matches_by_its_world_for_place_ids_and_destinations() {
        let f = EventListFilters {
            list: EventListType::All,
            places_ids: vec!["P1".into(), "My-World.dcl.eth".into()],
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&f, &mut binds);
        assert!(
            sql.contains(&format!(" AND ({DESTINATION_CLAUSE})")),
            "{sql}"
        );
        assert!(
            matches!(&binds[0], EventBind::TextArray(ids) if ids == &["p1", "my-world.dcl.eth"]),
            "{binds:?}"
        );

        let with_community = EventListFilters {
            list: EventListType::All,
            places_ids: vec!["p1".into()],
            community_id: Some("c1".into()),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&with_community, &mut binds);
        assert!(
            sql.contains(&format!(" AND ({DESTINATION_CLAUSE} OR community_id = $2)")),
            "{sql}"
        );

        let by_destination = EventListFilters {
            list: EventListType::All,
            destination_ids: vec!["my-world.dcl.eth".into()],
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&by_destination, &mut binds);
        assert!(sql.contains(&format!(" AND {DESTINATION_CLAUSE}")), "{sql}");
    }

    #[test]
    fn list_sql_folds_total_into_a_single_statement() {
        let f = EventListFilters {
            places_ids: vec!["p1".into(), "p2".into()],
            list: EventListType::Active,
            limit: 20,
            offset: 0,
            ..Default::default()
        };
        let mut binds_t: Vec<EventBind> = Vec::new();
        let sql_t = build_list_sql(&f, true, &mut binds_t);
        assert_eq!(
            sql_t.matches("count(*) OVER() AS total_count").count(),
            1,
            "{sql_t}"
        );
        assert!(!sql_t.contains(';'), "{sql_t}");
        let mut binds_f: Vec<EventBind> = Vec::new();
        let sql_f = build_list_sql(&f, false, &mut binds_f);
        assert!(!sql_f.contains("total_count"), "{sql_f}");
        assert_eq!(binds_t.len(), binds_f.len());
        assert_eq!(
            sql_t,
            sql_f.replacen(
                &format!("SELECT {} FROM explore.event", list_event_columns(false)),
                &format!(
                    "SELECT {}, count(*) OVER() AS total_count FROM explore.event",
                    list_event_columns(false)
                ),
                1,
            )
        );
        let mut wbinds: Vec<EventBind> = Vec::new();
        let where_out = EventsComponent::build_where(&f, &mut wbinds);
        let mut cbinds: Vec<EventBind> = Vec::new();
        assert_eq!(
            count_only_sql(&f, &mut cbinds),
            format!("SELECT count(*) FROM explore.event{where_out}")
        );
    }

    #[test]
    fn record_recomputes_next_occurrence_from_recurrent_dates() {
        let now = Utc::now();
        let past = now - chrono::Duration::days(7);
        let future = now + chrono::Duration::days(1);
        let raw = json!({
            "next_start_at": past.to_rfc3339(),
            "next_finish_at": (past + chrono::Duration::hours(2)).to_rfc3339(),
            "recurrent_dates": [past.to_rfc3339(), future.to_rfc3339()],
        });
        let rec = event_row_to_record(
            row_with(
                raw,
                Some(past - chrono::Duration::days(30)),
                Some(7_200_000),
            ),
            None,
            None,
        );
        assert_eq!(
            rec.next_start_at.map(|d| d.timestamp()),
            Some(future.timestamp())
        );
        assert_eq!(
            rec.next_finish_at.map(|d| d.timestamp()),
            Some((future + chrono::Duration::hours(2)).timestamp())
        );
        assert!(!rec.live);

        let started = now - chrono::Duration::minutes(30);
        let raw = json!({ "recurrent_dates": [started.to_rfc3339()] });
        let rec = event_row_to_record(row_with(raw, Some(started), Some(7_200_000)), None, None);
        assert_eq!(
            rec.next_start_at.map(|d| d.timestamp()),
            Some(started.timestamp())
        );
        assert!(rec.live);

        let raw = json!({
            "next_start_at": past.to_rfc3339(),
            "next_finish_at": (past + chrono::Duration::hours(2)).to_rfc3339(),
            "recurrent_dates": [past.to_rfc3339()],
        });
        let rec = event_row_to_record(row_with(raw, Some(past), Some(7_200_000)), None, None);
        assert_eq!(
            rec.next_start_at.map(|d| d.timestamp()),
            Some(past.timestamp())
        );
        assert!(!rec.live);

        let running = now - chrono::Duration::minutes(30);
        let raw = json!({
            "next_start_at": running.to_rfc3339(),
            "next_finish_at": (running + chrono::Duration::hours(2)).to_rfc3339(),
            "recurrent_dates": [future.to_rfc3339()],
        });
        let rec = event_row_to_record(row_with(raw, Some(past), Some(7_200_000)), None, None);
        assert_eq!(
            rec.next_start_at.map(|d| d.timestamp()),
            Some(running.timestamp()),
            "the stored occurrence keeps running although the recurrence list dropped it"
        );
        assert_eq!(
            rec.next_finish_at.map(|d| d.timestamp()),
            Some((running + chrono::Duration::hours(2)).timestamp())
        );
        assert!(rec.live);
    }

    #[test]
    fn owner_listing_scopes_to_the_user_and_drops_status_filters() {
        let f = EventListFilters {
            owner: true,
            user: Some("0xABC".into()),
            creator: Some("0xdef".into()),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&f, &mut binds);
        assert_eq!(sql.matches("lower(user_creator)").count(), 1, "{sql}");
        assert!(sql.contains("lower(user_creator) = $1"), "{sql}");
        assert!(!sql.contains("approved IS TRUE"), "{sql}");
        assert!(
            !sql.contains("COALESCE((raw->>'rejected')::boolean, false) IS FALSE"),
            "{sql}"
        );
        assert!(matches!(binds.first(), Some(EventBind::Text(u)) if u.as_str() == "0xabc"));

        let f = EventListFilters {
            user: Some("0xabc".into()),
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = EventsComponent::build_where(&f, &mut binds);
        assert!(
            sql.contains("approved IS TRUE")
                && sql.contains("COALESCE((raw->>'rejected')::boolean, false) IS FALSE")
        );

        let f = EventListFilters {
            owner: true,
            user: None,
            ..Default::default()
        };
        let mut binds = Vec::new();
        assert!(EventsComponent::build_where(&f, &mut binds).contains(" AND FALSE"));
    }

    #[test]
    fn lists_with_explicit_occurrence_payloads_have_a_smaller_page_limit() {
        let filters = EventListFilters {
            limit: 500,
            with_recurrent_dates: true,
            ..Default::default()
        };
        let mut binds = Vec::new();
        let sql = build_list_sql(&filters, false, &mut binds);
        assert!(sql.contains("LIMIT 1000"));
        assert!(matches!(
            binds.get(binds.len() - 2),
            Some(EventBind::Int64(25))
        ));
    }

    #[test]
    fn ordinary_lists_keep_requested_event_limits_with_a_hundred_date_window() {
        for limit in [100, 500] {
            let filters = EventListFilters {
                limit,
                ..Default::default()
            };
            let mut binds = Vec::new();
            let sql = build_list_sql(&filters, false, &mut binds);
            assert!(sql.contains("LIMIT 100"));
            assert!(
                matches!(binds.get(binds.len()-2),Some(EventBind::Int64(value)) if *value==limit)
            );
        }
    }

    #[test]
    fn record_sanitizes_descriptions_urls_and_images() {
        let mut row = row_with(json!({}), None, None);
        row.description = Some(
            "Join <link=\"file:///etc/passwd\">here</link> or <link=\"https://decentraland.org\">our site</link>".into(),
        );
        let rec = event_row_to_record(row, None, None);
        assert_eq!(
            rec.description.as_deref(),
            Some("Join here or <link=\"https://decentraland.org\">our site</link>")
        );
        let metadata_ip = ["169", "254", "169", "254"].join(".");
        let raw = json!({
            "description": format!("<a href=\"smb://attacker/share\">x</a> <link=\"http://{metadata_ip}/\">y</link>")
        });
        let rec = event_row_to_record(row_with(raw, None, None), None, None);
        assert_eq!(rec.description.as_deref(), Some("x y"));

        let raw = json!({
            "url": "javascript:alert(1)",
            "image": "http://127.0.0.1/poster.png",
            "image_vertical": "javascript:alert(1)",
        });
        let rec = event_row_to_record(row_with(raw, None, None), None, None);
        assert_eq!((rec.url, rec.image, rec.image_vertical), (None, None, None));

        let raw = json!({
            "url": "https://decentraland.org/events",
            "image": "https://decentraland.org/poster.png",
            "image_vertical": "/poster/bafabc123",
        });
        let rec = event_row_to_record(row_with(raw, None, None), None, None);
        assert_eq!(rec.url.as_deref(), Some("https://decentraland.org/events"));
        assert_eq!(
            rec.image.as_deref(),
            Some("https://decentraland.org/poster.png")
        );
        assert_eq!(
            rec.image_vertical,
            Some(Value::String("/poster/bafabc123".into()))
        );
    }

    fn at(
        id: &str,
        place_id: Option<&str>,
        server: Option<&str>,
        world: bool,
        name: Option<&str>,
    ) -> EventAt {
        EventAt {
            id: id.into(),
            name: name.map(String::from),
            place_id: place_id.map(String::from),
            server: server.map(String::from),
            world,
            next_start_at: Some(Utc::now()),
        }
    }

    #[test]
    fn a_world_event_resolves_by_its_world_and_a_scene_event_never_by_its_realm() {
        let asked = AskedKeys::new(&["My-World.dcl.eth".to_string(), "Place-1".to_string()]);
        for place_id in [Some("upstream-uuid"), None] {
            let ev = at(
                "e",
                place_id,
                Some("my-world.dcl.eth"),
                true,
                Some("World Party"),
            );
            assert_eq!(asked.resolve(&ev).as_deref(), Some("My-World.dcl.eth"));
        }
        let ev = at(
            "e",
            Some("place-1"),
            Some("peer.decentraland.org"),
            false,
            Some("Movie Night"),
        );
        assert_eq!(asked.resolve(&ev).as_deref(), Some("Place-1"));
        let ev = at(
            "e",
            None,
            Some("peer.decentraland.org"),
            false,
            Some("Movie Night"),
        );
        assert_eq!(asked.resolve(&ev), None);
        let ev = at("e", Some("place-9"), None, false, Some("Elsewhere"));
        assert_eq!(asked.resolve(&ev), None);
    }
}
