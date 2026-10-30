-- Catalyst v2: social domain: friendships, blocks, mutes, private calls,
-- communities (members, bans, requests, places, posts, thumbnails, voice
-- presence), scene and platform moderation (the comms-gatekeeper tables),
-- notifications with explicit delivery state, subscriptions, badges,
-- referral and the translation cache.
--
-- Designed from the workload, not copied: the old bundle's five databases
-- (communities, its unused twin social_rpc, comms_gatekeeper, notifications,
-- badges, media) collapse into ONE schema.
-- Sources: catalyrst/crates/catalyrst-social-service/migrations 0001..0013,
-- catalyrst-comms/migrations 0001..0009, catalyrst-notifications/migrations
-- 0001..0008, catalyrst-badges/migrations 0001..0005,
-- catalyrst-media/migrations 0001.
--
-- Conventions: wallet addresses are lowercase 0x + 40 hex text
-- (src/social/identity.rs::Address binds and decodes them); community ids are
-- uuid; place ids are the places catalogue text id or a world name; every
-- timestamp column is `timestamp without time zone` holding UTC (the pool pins
-- TimeZone=UTC); the current time is always a bind parameter, never in an
-- index predicate. No FK crosses a schema (community_places.place_id,
-- scene_*.place_id name explore/worlds rows and reconcile on read).
--
-- Domain agents append ONLY inside the four `-- ==== section: ... ====`
-- blocks at the end (communities federation log, comms MLS, notification
-- producers, badges seed), after re-reading this file.

CREATE SCHEMA social;

-- ── settings ───────────────────────────────────────────────────────────────

-- One row per wallet that changed a setting; an absent row means the
-- defaults (the read path COALESCEs). Merges social_settings (social-rpc) and
-- private_messages_privacy (gatekeeper): one fact, one row.
CREATE TABLE social.user_settings (
    address text NOT NULL,
    private_messages_privacy text NOT NULL DEFAULT 'only_friends',
    blocked_users_messages_visibility text NOT NULL DEFAULT 'show_messages',
    show_situation_reactions text NOT NULL DEFAULT 'show',
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT user_settings_pkey PRIMARY KEY (address),
    CONSTRAINT user_settings_private_messages_privacy_check
        CHECK (private_messages_privacy IN ('only_friends', 'all')),
    CONSTRAINT user_settings_blocked_visibility_check
        CHECK (blocked_users_messages_visibility IN ('show_messages', 'do_not_show_messages')),
    CONSTRAINT user_settings_situation_reactions_check
        CHECK (show_situation_reactions IN ('show', 'hide'))
);

-- ── friendships ────────────────────────────────────────────────────────────

-- One canonical row per unordered pair. last_action/last_actor/last_action_at
-- are the projection of the latest friendship_actions row and is_active the
-- projection of that (accept => true, everything else => false); all four
-- move in the same statement as the action insert (rpc db.rs
-- apply_friendship_action), so the pending-request state is a keyed column,
-- not a re-derivation over the history.
CREATE TABLE social.friendships (
    id uuid NOT NULL,
    address_requester text NOT NULL,
    address_requested text NOT NULL,
    is_active boolean NOT NULL DEFAULT false,
    last_action text NOT NULL,
    last_actor text NOT NULL,
    last_action_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT friendships_pkey PRIMARY KEY (id),
    CONSTRAINT friendships_not_self_check CHECK (address_requester <> address_requested),
    CONSTRAINT friendships_last_action_check
        CHECK (last_action IN ('request', 'accept', 'cancel', 'reject', 'delete', 'block')),
    CONSTRAINT friendships_last_actor_check CHECK (last_actor IN (address_requester, address_requested)),
    CONSTRAINT friendships_active_projection_check CHECK (is_active = (last_action = 'accept'))
);

-- The unordered pair is unique (the 0009 race fix); the insert conflicts on
-- these two expressions and the pair status probe is written on them.
CREATE UNIQUE INDEX friendships_unordered_pair_uidx
    ON social.friendships (LEAST(address_requester, address_requested),
                           GREATEST(address_requester, address_requested));
-- Every friends read is `address_requester = $me OR address_requested = $me`
-- (friends list, mutual friends, counts): one arm each, bitmap OR.
CREATE INDEX friendships_requester_idx ON social.friendships (address_requester);
CREATE INDEX friendships_requested_idx ON social.friendships (address_requested);
-- The pending requests received/sent by a wallet (`last_action = 'request'
-- AND (arm = $me) AND last_actor <>/= $me ORDER BY last_action_at DESC, id
-- DESC`): one arm each over the pending rows only, so the page and its total
-- are bounded by the wallet's pending count, not its degree.
CREATE INDEX friendships_pending_requester_idx
    ON social.friendships (address_requester, last_action_at DESC, id DESC) WHERE last_action = 'request';
CREATE INDEX friendships_pending_requested_idx
    ON social.friendships (address_requested, last_action_at DESC, id DESC) WHERE last_action = 'request';

-- Append-only history; the latest row per friendship is the status.
CREATE TABLE social.friendship_actions (
    id uuid NOT NULL,
    friendship_id uuid NOT NULL REFERENCES social.friendships (id) ON DELETE CASCADE,
    action text NOT NULL,
    acting_user text NOT NULL,
    metadata json,
    "timestamp" timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT friendship_actions_pkey PRIMARY KEY (id),
    CONSTRAINT friendship_actions_action_check
        CHECK (action IN ('request', 'accept', 'cancel', 'reject', 'delete', 'block'))
);

-- The "last action" lateral of the status probes and the request message
-- lookup of a pending-requests page row.
CREATE INDEX friendship_actions_friendship_time_idx
    ON social.friendship_actions (friendship_id, "timestamp" DESC, id DESC);

-- Directional: A blocking B is not B blocking A.
CREATE TABLE social.blocks (
    blocker_address text NOT NULL,
    blocked_address text NOT NULL,
    blocked_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT blocks_pkey PRIMARY KEY (blocker_address, blocked_address),
    CONSTRAINT blocks_not_self_check CHECK (blocker_address <> blocked_address)
);

-- "Who blocked me" (GetBlockingStatus) and the per-row blocked_by EXISTS.
CREATE INDEX blocks_blocked_idx ON social.blocks (blocked_address);

CREATE TABLE social.user_mutes (
    muter_address text NOT NULL,
    muted_address text NOT NULL,
    muted_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT user_mutes_pkey PRIMARY KEY (muter_address, muted_address),
    CONSTRAINT user_mutes_not_self_check CHECK (muter_address <> muted_address)
);

CREATE TABLE social.friend_messages (
    id bigint GENERATED ALWAYS AS IDENTITY,
    sender_address text NOT NULL,
    recipient_address text NOT NULL,
    body text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT friend_messages_pkey PRIMARY KEY (id),
    CONSTRAINT friend_messages_not_self_check CHECK (sender_address <> recipient_address)
);

-- The conversation page (`created_at < $before ORDER BY created_at DESC, id
-- DESC LIMIT n`) and its count, written on the pair expressions so one index
-- serves both directions.
CREATE INDEX friend_messages_pair_idx
    ON social.friend_messages (LEAST(sender_address, recipient_address),
                               GREATEST(sender_address, recipient_address),
                               created_at DESC, id DESC);

-- ── private voice calls ────────────────────────────────────────────────────

-- The call; its LiveKit room is 'voice-chat-private-' || id. A pending call (no
-- participant rows) lives while created_at >= $now - PRIVATE_VOICE_CHAT_EXPIRATION_TIME;
-- the sweep deletes expired pending calls. An accepted call lives until ended.
CREATE TABLE social.private_voice_chats (
    id uuid NOT NULL,
    caller_address text NOT NULL,
    callee_address text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT private_voice_chats_pkey PRIMARY KEY (id),
    CONSTRAINT private_voice_chats_caller_key UNIQUE (caller_address),
    CONSTRAINT private_voice_chats_callee_key UNIQUE (callee_address),
    CONSTRAINT private_voice_chats_no_self_call_check CHECK (caller_address <> callee_address)
);

-- The expiry sweep and the admin "active calls" read.
CREATE INDEX private_voice_chats_created_idx ON social.private_voice_chats (created_at);

-- Presence in the call room (the old comms voice_chat_users); follows the
-- transport's lifecycle and the call row's.
CREATE TABLE social.private_voice_chat_participants (
    call_id uuid NOT NULL REFERENCES social.private_voice_chats (id) ON DELETE CASCADE,
    address text NOT NULL,
    status text NOT NULL,
    joined_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    status_updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT private_voice_chat_participants_pkey PRIMARY KEY (call_id, address),
    CONSTRAINT private_voice_chat_participants_status_check
        CHECK (status IN ('connected', 'connection_interrupted', 'disconnected', 'not_connected'))
);

-- The presence sweep: `status = X AND status_updated_at <= $now - ttl`.
CREATE INDEX private_voice_chat_participants_status_idx
    ON social.private_voice_chat_participants (status, status_updated_at);

-- ── communities ────────────────────────────────────────────────────────────

-- active = false is the (federation-visible) deletion; suspended is the local
-- operator hold (a separate axis, 0005). ranking_score has no writer yet
-- (derived column, source "none"; stays 0), editors_choice is set by the
-- admin route; both are read by the listing's ORDER BY.
CREATE TABLE social.communities (
    id uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    owner_address text NOT NULL,
    private boolean NOT NULL DEFAULT false,
    active boolean NOT NULL DEFAULT true,
    unlisted boolean NOT NULL DEFAULT false,
    editors_choice boolean NOT NULL DEFAULT false,
    ranking_score real NOT NULL DEFAULT 0,
    suspended boolean NOT NULL DEFAULT false,
    suspended_at timestamp without time zone,
    suspended_by text,
    suspension_reason text,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    -- 'upstream': copied by the communities mirror (communities/mirror.rs);
    -- 'local': created on this node or federated to it.
    origin text NOT NULL DEFAULT 'local',
    CONSTRAINT communities_pkey PRIMARY KEY (id),
    CONSTRAINT communities_origin_known CHECK (origin IN ('upstream', 'local')),
    CONSTRAINT communities_suspension_check
        CHECK ((suspended AND suspended_at IS NOT NULL AND suspended_by IS NOT NULL)
            OR (NOT suspended AND suspended_at IS NULL AND suspended_by IS NULL AND suspension_reason IS NULL))
);

-- The public listing's ORDER BY; the listing carries exactly this predicate.
CREATE INDEX communities_ranking_idx
    ON social.communities (editors_choice DESC, ranking_score DESC, name ASC)
    WHERE active AND NOT unlisted AND NOT suspended;
-- "communities I own" and the ownership checks on delete/transfer.
CREATE INDEX communities_owner_idx ON social.communities (owner_address);

-- One spelling for a moderator ('moderator'; 'mod' is accepted on input by
-- identity.rs). A banned wallet has no row: the ban write deletes it in the
-- same transaction, and rejoin is refused while community_bans.active.
CREATE TABLE social.community_members (
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    member_address text NOT NULL,
    role text NOT NULL,
    joined_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_members_pkey PRIMARY KEY (community_id, member_address),
    CONSTRAINT community_members_role_check CHECK (role IN ('owner', 'moderator', 'member'))
);

-- Exactly one owner per community (communities.owner_address names it; the
-- transfer writes both rows in one transaction).
CREATE UNIQUE INDEX community_members_owner_uidx
    ON social.community_members (community_id) WHERE role = 'owner';
-- "my communities", onlyMemberOf, friends-in-community, invite candidates.
CREATE INDEX community_members_member_idx
    ON social.community_members (member_address, community_id);

CREATE TABLE social.community_bans (
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    banned_address text NOT NULL,
    banned_by text NOT NULL,
    banned_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    reason text,
    active boolean NOT NULL DEFAULT true,
    unbanned_by text,
    unbanned_at timestamp without time zone,
    CONSTRAINT community_bans_pkey PRIMARY KEY (community_id, banned_address),
    CONSTRAINT community_bans_lift_check
        CHECK ((active AND unbanned_by IS NULL AND unbanned_at IS NULL)
            OR (NOT active AND unbanned_by IS NOT NULL AND unbanned_at IS NOT NULL))
);

-- The moderator's bans listing (ORDER BY banned_at) and its count.
CREATE INDEX community_bans_active_idx
    ON social.community_bans (community_id, banned_at) WHERE active;

-- Invites (by owner/moderator) and join requests (by the wallet). Terminal
-- statuses are never reopened; a new request is a new row.
CREATE TABLE social.community_requests (
    id uuid NOT NULL,
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    member_address text NOT NULL,
    type text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_requests_pkey PRIMARY KEY (id),
    CONSTRAINT community_requests_type_check CHECK (type IN ('invite', 'request_to_join')),
    CONSTRAINT community_requests_status_check
        CHECK (status IN ('pending', 'accepted', 'rejected', 'cancelled'))
);

-- At most one pending request per (community, wallet), any type; also the
-- private-community visibility gate ("pending invite for me").
CREATE UNIQUE INDEX community_requests_pending_uidx
    ON social.community_requests (community_id, member_address) WHERE status = 'pending';
-- The moderator's listing/count by status (optionally type).
CREATE INDEX community_requests_community_idx
    ON social.community_requests (community_id, status, created_at DESC);
-- "my invites / my requests".
CREATE INDEX community_requests_member_idx
    ON social.community_requests (member_address, status, created_at DESC);

-- place_id is explore.place.id (uuid text) or a world name; no cross-schema
-- FK: a place the mirror dropped is filtered on read.
CREATE TABLE social.community_places (
    place_id text NOT NULL,
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    added_by text NOT NULL,
    added_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_places_pkey PRIMARY KEY (place_id, community_id)
);

-- The community's places listing (ORDER BY added_at DESC) and count.
CREATE INDEX community_places_community_idx
    ON social.community_places (community_id, added_at DESC);

CREATE TABLE social.community_posts (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    author_address text NOT NULL,
    content text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_posts_pkey PRIMARY KEY (id)
);

-- The feed (newest first) and the keyset tie-breaker of the new endpoint.
CREATE INDEX community_posts_feed_idx
    ON social.community_posts (community_id, created_at DESC, id DESC);

CREATE TABLE social.community_post_likes (
    post_id uuid NOT NULL REFERENCES social.community_posts (id) ON DELETE CASCADE,
    user_address text NOT NULL,
    liked_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_post_likes_pkey PRIMARY KEY (post_id, user_address)
);

-- The bytes live in the ONE blob store; has_thumbnail on the wire = row exists.
CREATE TABLE social.community_thumbnails (
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    thumbnail_hash text NOT NULL,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_thumbnails_pkey PRIMARY KEY (community_id)
);

-- Presence in the community voice room ('voice-chat-community-' ||
-- community_id); presence is not permission. Participant/moderator counts and
-- "has an active voice chat" derive from this table.
CREATE TABLE social.community_voice_chat_users (
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    address text NOT NULL,
    is_moderator boolean NOT NULL DEFAULT false,
    status text NOT NULL,
    sid text,
    joined_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    status_updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_voice_chat_users_pkey PRIMARY KEY (community_id, address),
    CONSTRAINT community_voice_chat_users_status_check
        CHECK (status IN ('connected', 'connection_interrupted', 'disconnected', 'not_connected'))
);

-- The sweep and the active-rooms/moderator-count aggregates.
CREATE INDEX community_voice_chat_users_status_idx
    ON social.community_voice_chat_users (status, status_updated_at);

-- ── scene moderation (comms-gatekeeper, place-scoped) ──────────────────────

-- Ownership of a place is resolved through explore/worlds, not stored here.
-- These three move to `worlds` unchanged when worlds/comms lands.
CREATE TABLE social.scene_admins (
    place_id text NOT NULL,
    admin_address text NOT NULL,
    added_by text NOT NULL,
    added_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    active boolean NOT NULL DEFAULT true,
    removed_at timestamp without time zone,
    CONSTRAINT scene_admins_pkey PRIMARY KEY (place_id, admin_address),
    CONSTRAINT scene_admins_removed_check
        CHECK ((active AND removed_at IS NULL) OR (NOT active AND removed_at IS NOT NULL))
);

CREATE TABLE social.scene_bans (
    place_id text NOT NULL,
    banned_address text NOT NULL,
    banned_by text NOT NULL,
    banned_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT scene_bans_pkey PRIMARY KEY (place_id, banned_address)
);

-- The bans listing (ORDER BY banned_at DESC) with its count(*) OVER ().
CREATE INDEX scene_bans_place_time_idx ON social.scene_bans (place_id, banned_at DESC);

-- The RTMP ingress credential of a place's stream (/scene-stream-access).
-- One active row per place; a reset or a removal deactivates the row and
-- keeps it. `streaming`/`streaming_start_time` follow the LiveKit ingress
-- webhooks of the active row.
CREATE TABLE social.scene_stream_access (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    place_id text NOT NULL,
    streaming_url text NOT NULL,
    streaming_key text NOT NULL,
    ingress_id text,
    room_id text,
    generated_by text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    expiration_time timestamp without time zone,
    streaming boolean NOT NULL DEFAULT false,
    streaming_start_time timestamp without time zone,
    streaming_state_version bigint NOT NULL DEFAULT 0,
    streaming_checked_at timestamp without time zone,
    cleanup_retry_at timestamp without time zone,
    active boolean NOT NULL DEFAULT true,
    CONSTRAINT scene_stream_access_pkey PRIMARY KEY (id)
);

-- The per-place active-credential lookup, and the one-active invariant.
CREATE UNIQUE INDEX scene_stream_access_active_place_key
    ON social.scene_stream_access (place_id) WHERE active;

-- The ingress_started/ingress_ended webhook updates of the active row.
CREATE INDEX scene_stream_access_active_ingress_idx
    ON social.scene_stream_access (ingress_id) WHERE active;

-- Comms-owned streaming cleanup intents: no FK, so revocation survives place/access removal.
CREATE TABLE social.scene_stream_cleanup (
 access_id uuid PRIMARY KEY, place_id text NOT NULL, ingress_id text,
 notification_kind text CHECK (notification_kind IN ('streaming_key_reset', 'streaming_key_revoke', 'streaming_key_expired', 'streaming_time_exceeded')),
 occurred_at timestamp without time zone NOT NULL, due_at timestamp without time zone NOT NULL,
 claim_token uuid, claim_until timestamp without time zone,
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0), ingress_deleted boolean NOT NULL DEFAULT false,
 recipient text, metadata jsonb, source_context jsonb
);
-- Bounded ordered cleanup retries.
CREATE INDEX scene_stream_cleanup_due_idx ON social.scene_stream_cleanup (due_at, access_id);

-- Bounded expiry/time-limit selection; moving time predicates stay in the queries.
CREATE INDEX scene_stream_access_key_expiry_idx ON social.scene_stream_access ((COALESCE(expiration_time, created_at + interval '4 days')), id) WHERE active;
CREATE INDEX scene_stream_access_stream_start_idx ON social.scene_stream_access (streaming_start_time, id) WHERE active AND streaming;

-- ── platform moderation ────────────────────────────────────────────────────

-- A ban is in force iff lifted_at IS NULL AND (expires_at IS NULL OR
-- expires_at > $now). One in-force ban per address, enforced by the ban
-- write under pg_advisory_xact_lock(hashtext(address)) (the expiry half is
-- time-dependent, so no unique index can express it).
CREATE TABLE social.user_bans (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    banned_address text NOT NULL,
    banned_by text NOT NULL,
    reason text NOT NULL,
    custom_message text,
    banned_device_id text,
    banned_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    expires_at timestamp without time zone,
    lifted_at timestamp without time zone,
    lifted_by text,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT user_bans_pkey PRIMARY KEY (id),
    CONSTRAINT user_bans_lift_check
        CHECK ((lifted_at IS NULL AND lifted_by IS NULL) OR (lifted_at IS NOT NULL AND lifted_by IS NOT NULL))
);

-- The connection-ban probe (every LiveKit token), get_status (latest in
-- force) and the admin "active bans" listing; expiry is filtered per row.
CREATE INDEX user_bans_open_by_address_idx
    ON social.user_bans (banned_address, banned_at DESC) WHERE lifted_at IS NULL;
-- The device half of the probe (a banned device under a new wallet).
CREATE INDEX user_bans_open_by_device_idx
    ON social.user_bans (banned_device_id) WHERE lifted_at IS NULL AND banned_device_id IS NOT NULL;

-- Comms-owned retry state beside legacy moderation tables. Network I/O never
-- holds these claims' SQL transactions. Unfinished intents have no retention cap.
CREATE TABLE social.livekit_removal_intents (
    root_key text NOT NULL,
    target_key text NOT NULL DEFAULT '',
    generation uuid NOT NULL,
    cause_kind text NOT NULL,
    wallet text NOT NULL,
    actor text NOT NULL,
    ban_id uuid,
    place_id text,
    scene_version timestamp without time zone,
    scope jsonb NOT NULL DEFAULT '[]'::jsonb,
    room_name text,
    room_sid text,
    participant_identity text,
    cursor_room text NOT NULL DEFAULT '',
    cursor_sid text NOT NULL DEFAULT '',
    cursor_identity text NOT NULL DEFAULT '',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    claim_id uuid,
    claim_until timestamp without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT livekit_removal_intents_pkey PRIMARY KEY (root_key, target_key),
    CONSTRAINT livekit_removal_intents_cause CHECK (cause_kind IN ('platform', 'scene', 'block', 'connection')),
    CONSTRAINT livekit_removal_intents_attempts CHECK (attempts >= 0),
    CONSTRAINT livekit_removal_intents_scope CHECK (jsonb_typeof(scope) = 'array'),
    CONSTRAINT livekit_removal_intents_claim CHECK ((claim_id IS NULL) = (claim_until IS NULL)),
    CONSTRAINT livekit_removal_intents_target CHECK (target_key = '' OR (room_name IS NOT NULL AND room_sid IS NOT NULL AND participant_identity IS NOT NULL))
);
CREATE INDEX livekit_removal_intents_due_idx ON social.livekit_removal_intents (next_attempt_at, created_at, root_key, target_key);
CREATE INDEX livekit_removal_intents_room_idx ON social.livekit_removal_intents (room_name, room_sid) WHERE room_name IS NOT NULL;

CREATE TABLE social.user_warnings (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    warned_address text NOT NULL,
    warned_by text NOT NULL,
    reason text NOT NULL,
    warned_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT user_warnings_pkey PRIMARY KEY (id)
);

-- A wallet's warnings, newest first.
CREATE INDEX user_warnings_address_idx ON social.user_warnings (warned_address, warned_at DESC);

-- Latest connection facts per player, upserted best-effort on token minting
-- (0006); the device id is what a ban snapshots.
CREATE TABLE social.player_connections (
    address text NOT NULL,
    ip_address text,
    device_id text,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT player_connections_pkey PRIMARY KEY (address)
);

-- ── player reports ─────────────────────────────────────────────────────────

-- A report is inserted AFTER its evidence slots (presign mints the slots,
-- upload fills them, submit files the report), so no FK from the slots.
CREATE TABLE social.player_reports (
    id uuid NOT NULL,
    reporter_address text NOT NULL,
    reported_address text NOT NULL,
    reason text NOT NULL,
    description text NOT NULL,
    additional_comments text,
    evidence_keys text[] NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'open',
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT player_reports_pkey PRIMARY KEY (id)
);

-- The moderator listings: by reported wallet, by status, and unfiltered,
-- each newest first.
CREATE INDEX player_reports_reported_idx ON social.player_reports (reported_address, created_at DESC);
CREATE INDEX player_reports_status_idx ON social.player_reports (status, created_at DESC);
CREATE INDEX player_reports_created_idx ON social.player_reports (created_at DESC);
-- The hourly per-reporter limit.
CREATE INDEX player_reports_reporter_idx ON social.player_reports (reporter_address, created_at);

-- One slot per declared evidence file; `content_hash` is the CID of the
-- bytes in the ONE blob store, set with `uploaded_at` by the upload.
CREATE TABLE social.player_report_evidence (
    report_id uuid NOT NULL,
    evidence_key text NOT NULL,
    reporter_address text NOT NULL,
    filename text NOT NULL,
    content_type text NOT NULL,
    declared_size bigint NOT NULL,
    content_hash text,
    uploaded_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT player_report_evidence_pkey PRIMARY KEY (report_id, evidence_key),
    CONSTRAINT player_report_evidence_upload_check CHECK ((uploaded_at IS NULL) = (content_hash IS NULL))
);

-- The per-reporter pending-slot quota.
CREATE INDEX player_report_evidence_reporter_idx ON social.player_report_evidence (reporter_address, created_at);
-- The orphan sweep (oldest unsubmitted slots first, in batches).
CREATE INDEX player_report_evidence_created_idx ON social.player_report_evidence (created_at);

-- ── notifications ──────────────────────────────────────────────────────────

-- In-app delivery state is read/read_at on the row; occurred_at is the wire's
-- epoch-ms `timestamp`/`from` at ms precision, converted at the edge.
CREATE TABLE social.notifications (
    id uuid NOT NULL,
    address text NOT NULL,
    type text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    broadcast_key text,
    occurred_at timestamp(3) without time zone NOT NULL,
    read boolean NOT NULL DEFAULT false,
    read_at timestamp(3) without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT notifications_pkey PRIMARY KEY (id),
    CONSTRAINT notifications_read_check CHECK ((read AND read_at IS NOT NULL) OR (NOT read AND read_at IS NULL))
);

-- The bell's poll (`address = $me AND occurred_at > $from ORDER BY
-- occurred_at DESC LIMIT n`) and its keyset tie-breaker.
CREATE INDEX notifications_address_time_idx
    ON social.notifications (address, occurred_at DESC, id DESC);
-- The onlyUnread poll (`address = $1 AND NOT read ORDER BY occurred_at DESC,
-- id DESC`: the statement spells NOT read so the generic plan matches the
-- partial predicate, and carries the keyset tie-breaker).
CREATE INDEX notifications_unread_idx
    ON social.notifications (address, occurred_at DESC, id DESC) WHERE NOT read;
-- A re-run broadcast is idempotent per recipient.
CREATE UNIQUE INDEX notifications_broadcast_uidx
    ON social.notifications (address, broadcast_key) WHERE broadcast_key IS NOT NULL;

-- Explicit delivery state for the out-of-process channels; the worker claims
-- a bounded due set (`state = 'pending' AND next_attempt_at <= $now`) with
-- FOR UPDATE SKIP LOCKED, sends outside the transaction, records the outcome.
CREATE TABLE social.notification_deliveries (
    notification_id uuid NOT NULL REFERENCES social.notifications (id) ON DELETE CASCADE,
    channel text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    last_error text,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT notification_deliveries_pkey PRIMARY KEY (notification_id, channel),
    CONSTRAINT notification_deliveries_channel_check CHECK (channel IN ('email', 'push')),
    CONSTRAINT notification_deliveries_state_check CHECK (state IN ('pending', 'sent', 'failed', 'skipped')),
    CONSTRAINT notification_deliveries_attempts_check CHECK (attempts >= 0)
);

-- The worker's due set; `now` is the bind, never the predicate.
CREATE INDEX notification_deliveries_due_idx
    ON social.notification_deliveries (next_attempt_at, notification_id) WHERE state = 'pending';

-- details is the @dcl/schemas SubscriptionDetails document, read and written
-- as a unit; the 0001 unconfirmed_email/email_confirmation_token columns were
-- superseded by unconfirmed_emails (0002) and are not carried.
CREATE TABLE social.subscriptions (
    address text NOT NULL,
    email text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_credits_workflow boolean NOT NULL DEFAULT false,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT subscriptions_pkey PRIMARY KEY (address)
);

-- The cross-account "email already used" check.
CREATE INDEX subscriptions_email_idx ON social.subscriptions (lower(email)) WHERE email IS NOT NULL;

-- A pending email change never overwrites the confirmed one until its code
-- is presented.
CREATE TABLE social.unconfirmed_emails (
    address text NOT NULL,
    email text NOT NULL,
    code text NOT NULL,
    source text NOT NULL DEFAULT 'account',
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT unconfirmed_emails_pkey PRIMARY KEY (address)
);

-- Per-scope opt-outs (scope = 'community', scope_id = the community id).
CREATE TABLE social.subscription_opt_outs (
    address text NOT NULL,
    scope text NOT NULL,
    scope_id text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT subscription_opt_outs_pkey PRIMARY KEY (address, scope, scope_id)
);

-- "Online since" proxy: the most recent authenticated list fetch, upserted
-- with GREATEST so replicas never move it backwards.
CREATE TABLE social.notification_reader_seen (
    address text NOT NULL,
    last_fetch_at timestamp(3) without time zone NOT NULL,
    CONSTRAINT notification_reader_seen_pkey PRIMARY KEY (address)
);

-- ── badges ─────────────────────────────────────────────────────────────────

-- Reference data (seeded by the badges module/agent; schema.sql carries no
-- data rows). assets is the { "2d": {...}, "3d": {...} } document.
CREATE TABLE social.badge_definitions (
    id text NOT NULL,
    name text NOT NULL,
    description text,
    category text,
    is_tier boolean NOT NULL DEFAULT false,
    assets jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT badge_definitions_pkey PRIMARY KEY (id)
);

CREATE TABLE social.badge_tiers (
    badge_id text NOT NULL REFERENCES social.badge_definitions (id) ON DELETE CASCADE,
    tier_id text NOT NULL,
    tier_name text NOT NULL,
    description text,
    assets jsonb NOT NULL DEFAULT '{}'::jsonb,
    criteria_steps integer NOT NULL DEFAULT 0,
    ordinal integer NOT NULL DEFAULT 0,
    CONSTRAINT badge_tiers_pkey PRIMARY KEY (badge_id, tier_id)
);

-- granted_by distinguishes a console grant from an event-consumer write.
CREATE TABLE social.user_badge_progress (
    address text NOT NULL,
    badge_id text NOT NULL REFERENCES social.badge_definitions (id) ON DELETE CASCADE,
    steps_done integer NOT NULL DEFAULT 0,
    completed_at timestamp without time zone,
    last_completed_tier_id text,
    granted_by text,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT user_badge_progress_pkey PRIMARY KEY (address, badge_id),
    CONSTRAINT user_badge_progress_steps_check CHECK (steps_done >= 0)
);

CREATE TABLE social.user_achieved_tiers (
    address text NOT NULL,
    badge_id text NOT NULL REFERENCES social.badge_definitions (id) ON DELETE CASCADE,
    tier_id text NOT NULL,
    completed_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    granted_by text,
    granted_at timestamp without time zone,
    CONSTRAINT user_achieved_tiers_pkey PRIMARY KEY (address, badge_id, tier_id)
);

-- Append-only; revoke deletes the user rows, so provenance lands here.
CREATE TABLE social.badge_admin_audit (
    id bigint GENERATED ALWAYS AS IDENTITY,
    action text NOT NULL,
    address text NOT NULL,
    badge_id text NOT NULL,
    tier_id text,
    actor text NOT NULL,
    acted_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT badge_admin_audit_pkey PRIMARY KEY (id),
    CONSTRAINT badge_admin_audit_action_check CHECK (action IN ('grant', 'revoke'))
);

-- ── referral ───────────────────────────────────────────────────────────────

-- One row per invited wallet; first referrer wins (ON CONFLICT DO NOTHING).
CREATE TABLE social.referral_progress (
    invited_user text NOT NULL,
    referrer text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT referral_progress_pkey PRIMARY KEY (invited_user),
    CONSTRAINT referral_progress_status_check
        CHECK (status IN ('pending', 'signed_up', 'tier_granted', 'rejected_ip_match')),
    CONSTRAINT referral_progress_not_self_check CHECK (invited_user <> referrer)
);

-- The referrer's `COUNT(*) WHERE status = 'tier_granted'` and progress list.
CREATE INDEX referral_progress_referrer_idx ON social.referral_progress (referrer, status);

-- The invitedUsersAccepted count each referrer last saw.
CREATE TABLE social.referral_progress_viewed (
    referrer text NOT NULL,
    invites_accepted_viewed bigint NOT NULL DEFAULT 0,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT referral_progress_viewed_pkey PRIMARY KEY (referrer)
);

-- ── media: translation cache ───────────────────────────────────────────────

-- Cache of an external engine's answer per (backend, target_lang,
-- sha256(source text)); rebuild = ask the engine again.
CREATE TABLE social.translation_cache (
    backend text NOT NULL,
    target_lang text NOT NULL,
    text_sha256 bytea NOT NULL,
    translated_text text NOT NULL,
    detected_language text NOT NULL DEFAULT 'en',
    detected_confidence real NOT NULL DEFAULT 1.0,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT translation_cache_pkey PRIMARY KEY (backend, target_lang, text_sha256),
    CONSTRAINT translation_cache_hash_len_check CHECK (octet_length(text_sha256) = 32)
);

-- ── media: profile images ──────────────────────────────────────────────────

-- The faces and bodies /profile-images rendered or fetched from its origin
-- for a profile entity whose own snapshot files this node does not hold: a
-- keyed index into the ONE blob store (blob_cid, the bytes' CIDv1, which
-- storage::gc counts as a reference). A profile entity never changes, so a
-- row is current until PROFILE_IMAGES_CACHE_TTL_SECONDS ages it or the
-- PROFILE_IMAGES_CACHE_MAX_BYTES sweep drops it, oldest first; either way the
-- next request derives it again. entity_id names the profile, not a blob.
CREATE TABLE social.profile_images (
    entity_id text NOT NULL,
    kind text NOT NULL,
    blob_cid text NOT NULL,
    byte_size integer NOT NULL,
    source text NOT NULL,
    stored_at timestamp without time zone NOT NULL,
    CONSTRAINT profile_images_pkey PRIMARY KEY (entity_id, kind),
    CONSTRAINT profile_images_kind_check CHECK (kind IN ('face', 'body')),
    CONSTRAINT profile_images_source_check CHECK (source IN ('render', 'origin')),
    CONSTRAINT profile_images_size_check CHECK (byte_size > 0 AND byte_size <= 8388608)
);

-- ══ Domain-agent sections: append inside the matching block only ═══════════

-- ==== section: communities ====
-- The communities federation (src/social/communities/fed.rs): every
-- wallet-signed envelope of the DecentralandCommunities domain, once, keyed
-- by its digest and numbered by one identity seq, projected into the keyed
-- relations above in the transaction that logs it. The old communities_local
-- and six community_*_log tables are `kind`; community_role_current is not
-- recreated (community_members is the projection; its (signed_at,
-- signature_hash) tie-break is the write's probe on
-- community_signed_actions_member_idx). `community_id` is the federation's
-- hex id as signed, `community_uuid` its row; `target` the member (create,
-- role), the post digest (post_delete, post_like, post_unlike), the place
-- (place) or the request (request_status); `role` the old role-log text;
-- `action` a place's add|remove or a request's settled status;
-- `content_hash` a post body's sha256 (community_contents).
CREATE TABLE social.community_signed_actions (
    signature_hash text NOT NULL,
    seq bigint GENERATED ALWAYS AS IDENTITY,
    community_id text NOT NULL,
    community_uuid uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    kind text NOT NULL,
    signer text NOT NULL,
    target text,
    role text,
    action text,
    content_hash text,
    signed_at bigint NOT NULL,
    payload jsonb NOT NULL,
    received_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_signed_actions_pkey PRIMARY KEY (signature_hash),
    CONSTRAINT community_signed_actions_seq_key UNIQUE (seq),
    CONSTRAINT community_signed_actions_kind_check CHECK (kind IN ('create', 'update', 'delete', 'role', 'post', 'post_delete', 'post_like', 'post_unlike', 'place', 'request_status')),
    CONSTRAINT community_signed_actions_signer_lowercase CHECK (signer = lower(signer)),
    CONSTRAINT community_signed_actions_role_check CHECK ((kind IN ('create', 'role')) = (role IS NOT NULL) AND (role IS NULL OR role IN ('owner', 'mod', 'member', 'banned', 'none'))),
    CONSTRAINT community_signed_actions_action_check CHECK ((kind IN ('place', 'request_status')) = (action IS NOT NULL) AND (action IS NULL OR action IN ('add', 'remove', 'accepted', 'rejected', 'cancelled'))),
    CONSTRAINT community_signed_actions_post_check CHECK ((kind = 'post') = (content_hash IS NOT NULL)),
    CONSTRAINT community_signed_actions_target_check CHECK ((kind IN ('update', 'delete', 'post')) = (target IS NULL))
);
-- One create per community: the 409 of POST /v1/communities (envelope).
CREATE UNIQUE INDEX community_signed_actions_create_uidx
    ON social.community_signed_actions (community_uuid) WHERE kind = 'create';
-- The feeds: WHERE kind = ANY($1) AND seq > $2 ORDER BY seq LIMIT $3, and
-- the snapshot's max(seq) and digests per kind.
CREATE INDEX community_signed_actions_kind_seq_idx
    ON social.community_signed_actions (kind, seq);
-- The role tie-break: a newer role action on (community, member).
CREATE INDEX community_signed_actions_member_idx
    ON social.community_signed_actions (community_uuid, target, signed_at, signature_hash) WHERE role IS NOT NULL;
-- The content gc's "no post names this body": WHERE content_hash = $1.
CREATE INDEX community_signed_actions_content_idx
    ON social.community_signed_actions (content_hash) WHERE content_hash IS NOT NULL;

-- The federation's replay store (crate::fed::check_and_record): a signer's
-- nonce is refused while recorded; kept signed_at + 360 s.
CREATE TABLE social.community_seen_nonces (
    signer text NOT NULL,
    nonce text NOT NULL,
    expires_at bigint NOT NULL,
    CONSTRAINT community_seen_nonces_pkey PRIMARY KEY (signer, nonce)
);
-- The `community-nonce-sweep` job: DELETE ... WHERE expires_at < $1.
CREATE INDEX community_seen_nonces_expires_idx ON social.community_seen_nonces (expires_at);

-- The post bodies of the federation (POST /federation/communities/content):
-- the wire names a body by its sha256, the ONE blob store by its CID.
CREATE TABLE social.community_contents (
    content_hash text NOT NULL,
    cid text NOT NULL,
    size integer NOT NULL,
    stored_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT community_contents_pkey PRIMARY KEY (content_hash),
    CONSTRAINT community_contents_cid_key UNIQUE (cid),
    CONSTRAINT community_contents_hash_shape CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT community_contents_size_check CHECK (size >= 0 AND size <= 262144)
);

-- The upstream communities mirror (src/social/communities/mirror.rs), the
-- explore mirrors' pattern: an operator job pages an upstream social API's
-- public listing (~150 rows, hourly) into `communities` under the upstream
-- id (communities.origin = 'upstream'). No wallet owns or moderates it here:
-- a join or leave is a local member row, every other community-scoped write
-- answers 409 naming the upstream, and the pass only updates 'upstream'
-- communities. members_count and thumbnail_url are the
-- upstream's (the listing, detail and search add the local members to the
-- count and prefer the upstream thumbnail);
-- fetched_at is the page time of the last pass that listed it: a complete
-- pass deactivates (active = false, never deletes) the active mirrored rows
-- older than its start minus the grace, keeping their local members (inert
-- while inactive, deleted with the community). No index beyond the key: every read
-- is by community_id, and the sweep and thumbnail probe scan ~150 rows.
CREATE TABLE social.community_mirror (
    community_id uuid NOT NULL REFERENCES social.communities (id) ON DELETE CASCADE,
    source_url text NOT NULL,
    members_count bigint NOT NULL DEFAULT 0,
    thumbnail_url text,
    fetched_at timestamp without time zone NOT NULL,
    CONSTRAINT community_mirror_pkey PRIMARY KEY (community_id),
    CONSTRAINT community_mirror_members_count_check CHECK (members_count >= 0)
);

-- The mirror's pass markers per source (explore.mirror_state's shape): the
-- in-flight cursor (pass_started_at + next_offset, cleared when the pass
-- ends) lets a killed pass resume with the same sweep cutoff; the last
-- pass's outcome and the last complete pass are the freshness record.
CREATE TABLE social.community_mirror_state (
    source_url text NOT NULL,
    pass_started_at timestamp without time zone,
    next_offset bigint,
    pass_rows bigint NOT NULL DEFAULT 0,
    last_pass_at timestamp without time zone,
    last_pass_complete boolean,
    last_pass_rows bigint,
    last_pass_deactivated bigint,
    last_complete_pass_at timestamp without time zone,
    last_error text,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT community_mirror_state_pkey PRIMARY KEY (source_url)
);
-- ==== end section: communities ====

-- ==== section: comms ====
-- The MLS delivery service (src/social/comms/messaging.rs). The service
-- routes opaque MLS bytes and never holds group secrets: key packages stay
-- here (small, claimed once); commit, welcome and application ciphertext
-- bytes live in the ONE blob store under their CID, the rows keep the CIDs
-- and the wire's sha256 hashes. The v4 realm/lane authority
-- (archipelago_v4_*) closes the section. The gatekeeper's other tables are
-- above (scene moderation, platform moderation, player reports).

-- This catalyst's federation identity when FED_PEER_ID is unset: one random
-- id minted at schema apply, the epoch author of the groups created here.
CREATE TABLE social.fed_instance_identity (
    only_row boolean NOT NULL DEFAULT true,
    peer_id text NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT fed_instance_identity_pkey PRIMARY KEY (only_row),
    CONSTRAINT fed_instance_identity_only_row_check CHECK (only_row)
);
INSERT INTO social.fed_instance_identity (only_row, peer_id) VALUES (true, 'instance-' || gen_random_uuid());

-- Published key packages; `consumed_at` marks the single claim (the row is
-- kept so the same package, by `ref_hash`, is never published again).
CREATE TABLE social.mls_key_packages (
    id bigint GENERATED ALWAYS AS IDENTITY,
    owner text NOT NULL,
    ref_hash text NOT NULL,
    ciphersuite integer NOT NULL,
    key_package bytea NOT NULL,
    consumed_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT mls_key_packages_pkey PRIMARY KEY (id),
    CONSTRAINT mls_key_packages_ref_key UNIQUE (ref_hash)
);

-- The claim (an owner's oldest unconsumed package) and the count.
CREATE INDEX mls_key_packages_owner_unconsumed_idx
    ON social.mls_key_packages (owner, created_at) WHERE consumed_at IS NULL;

-- Routing metadata of a group (never its ratchet state). A 'channel' may
-- name a community (no FK: reconciled on community deletion, as
-- community_places).
CREATE TABLE social.mls_groups (
    group_id text NOT NULL,
    creator text NOT NULL,
    group_kind text NOT NULL,
    community_id uuid,
    epoch_author text NOT NULL,
    current_epoch bigint NOT NULL DEFAULT 0,
    ciphersuite integer NOT NULL,
    last_commit_hash text,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT mls_groups_pkey PRIMARY KEY (group_id),
    CONSTRAINT mls_groups_kind_check CHECK (group_kind IN ('dm', 'channel'))
);

-- The authorization roster (who fetches history, who receives fan-out),
-- not the MLS tree.
CREATE TABLE social.mls_group_members (
    group_id text NOT NULL,
    member text NOT NULL,
    added_epoch bigint NOT NULL,
    removed_epoch bigint,
    CONSTRAINT mls_group_members_pkey PRIMARY KEY (group_id, member),
    CONSTRAINT mls_group_members_group_fkey FOREIGN KEY (group_id) REFERENCES social.mls_groups (group_id) ON DELETE CASCADE
);

-- One row per epoch advance; the bytes are in the blob store.
CREATE TABLE social.mls_commits (
    group_id text NOT NULL,
    epoch bigint NOT NULL,
    commit_cid text NOT NULL,
    welcome_cid text,
    committer text NOT NULL,
    commit_hash text NOT NULL,
    signed_at bigint NOT NULL,
    received_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT mls_commits_pkey PRIMARY KEY (group_id, epoch),
    CONSTRAINT mls_commits_group_fkey FOREIGN KEY (group_id) REFERENCES social.mls_groups (group_id) ON DELETE CASCADE
);

-- The ordered index of a group's application messages; `ciphertext_hash` is
-- the wire's sha256, `ciphertext_cid` the blob-store key of the same bytes.
CREATE TABLE social.mls_message_refs (
    signature_hash text NOT NULL,
    group_id text NOT NULL,
    author text NOT NULL,
    epoch bigint NOT NULL,
    ciphertext_hash text NOT NULL,
    ciphertext_cid text NOT NULL,
    signed_at bigint NOT NULL,
    received_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT mls_message_refs_pkey PRIMARY KEY (signature_hash),
    CONSTRAINT mls_message_refs_group_fkey FOREIGN KEY (group_id) REFERENCES social.mls_groups (group_id) ON DELETE CASCADE
);

-- The history page (newest first, before a time).
CREATE INDEX mls_message_refs_group_time_idx ON social.mls_message_refs (group_id, received_at DESC);
-- GET /mls/blobs/{hash}.
CREATE INDEX mls_message_refs_ciphertext_idx ON social.mls_message_refs (ciphertext_hash);

-- The v4 control authority: archipelago's `/ws/v4` claims a lane per
-- (audience, wallet) and renews it; the comms cluster subscriber and
-- `/island-refresh` fence the realm lane's island assignment on the row.
CREATE TABLE social.archipelago_v4_meta (
    key text NOT NULL,
    value text NOT NULL,
    CONSTRAINT archipelago_v4_meta_pkey PRIMARY KEY (key)
);

-- The per-connection epoch a `/ws/v4` owner claims with.
CREATE SEQUENCE social.archipelago_v4_connection_epoch;

CREATE TABLE social.archipelago_v4_assignments (
    deployment_audience text NOT NULL,
    owner_address text NOT NULL,
    lane_key text NOT NULL,
    authority_incarnation text NOT NULL,
    assignment_revision bigint NOT NULL,
    owner_session text NOT NULL,
    owner_epoch bigint NOT NULL,
    fencing_token bigint NOT NULL,
    assignment_json jsonb,
    acknowledged_revision bigint NOT NULL DEFAULT 0,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT archipelago_v4_assignments_pkey PRIMARY KEY (deployment_audience, owner_address, lane_key)
);
-- ==== end section: comms ====

-- ==== section: notifications ====
-- The friend_first_wear producer's durable state and the
-- land_transfer_received producer's cursor.

-- Every marketplace URN a wallet has ever worn in a deployed profile; the PK
-- is the claim that stops a second notification for the same first wear.
CREATE TABLE social.worn_history (
    address text NOT NULL,
    urn text NOT NULL,
    first_seen timestamp(3) without time zone NOT NULL,
    CONSTRAINT worn_history_pkey PRIMARY KEY (address, urn)
);

-- A wallet with a row is eligible; its first profile seen seeds worn_history
-- silently (an empty outfit still gets a row, or its real first wear would be
-- swallowed by the seed path).
CREATE TABLE social.first_wear_baseline (
    address text NOT NULL,
    seeded_at timestamp(3) without time zone NOT NULL,
    CONSTRAINT first_wear_baseline_pkey PRIMARY KEY (address)
);

-- The entity_timestamp high-water mark over content.deployments profiles;
-- initialised to now on the first pass so history is never replayed.
CREATE TABLE social.first_wear_cursor (
    id integer NOT NULL DEFAULT 1,
    last_seen timestamp without time zone NOT NULL,
    CONSTRAINT first_wear_cursor_pkey PRIMARY KEY (id),
    CONSTRAINT first_wear_cursor_singleton_check CHECK (id = 1)
);

-- The last land_authz transfer event (chain position) the LAND-transfer
-- producer handled; seeded at the newest published transfer on first run.
CREATE TABLE social.land_transfer_cursor (
    id integer NOT NULL DEFAULT 1,
    block_number bigint NOT NULL,
    log_index integer NOT NULL,
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT land_transfer_cursor_pkey PRIMARY KEY (id),
    CONSTRAINT land_transfer_cursor_singleton_check CHECK (id = 1)
);
-- ==== end section: notifications ====

-- ==== section: badges ====
-- Reference rows: the old crate's fixture
-- (catalyrst-badges/migrations/0002_seed_fixture.sql with the 0005 asset
-- fix-up applied), so `catalyst schema apply` is the one bootstrap. Asset URLs
-- keep the upstream host; the badges module rewrites it to
-- BADGES_PUBLIC_ASSET_BASE_URL at serve time and serves the embedded PNGs at
-- /badges/assets/{path}. Never per-user rows.
INSERT INTO social.badge_definitions (id, name, description, category, is_tier, assets) VALUES
  ('open_for_business', 'Open for Business', 'Set up a store and publish a collection.', 'Builder', false,
   '{"2d":{"normal":"https://badges.decentraland.org/assets/open_for_business/2d/normal.png","hrm":"https://badges.decentraland.org/assets/open_for_business/2d/hrm.png","baseColor":"https://badges.decentraland.org/assets/open_for_business/2d/baseColor.png"},"3d":{"normal":"https://badges.decentraland.org/assets/open_for_business/3d/normal.png","hrm":"https://badges.decentraland.org/assets/open_for_business/3d/hrm.png","baseColor":"https://badges.decentraland.org/assets/open_for_business/3d/baseColor.png"}}'::jsonb),
  ('decentraland_citizen', 'Decentraland Citizen', 'Log in to Decentraland for the first time.', 'Explorer', false,
   '{"2d":{"normal":"https://badges.decentraland.org/assets/decentraland_citizen/2d/normal.png","hrm":"","baseColor":""},"3d":{"normal":"https://badges.decentraland.org/assets/decentraland_citizen/3d/normal.png","hrm":"https://badges.decentraland.org/assets/decentraland_citizen/3d/hrm.png","baseColor":"https://badges.decentraland.org/assets/decentraland_citizen/3d/baseColor.png"}}'::jsonb),
  ('walkabout', 'Walkabout', 'Walk a cumulative distance across Decentraland.', 'Explorer', true,
   '{"2d":{"normal":"https://badges.decentraland.org/assets/walkabout/2d/normal.png","hrm":"","baseColor":""},"3d":{"normal":"https://badges.decentraland.org/assets/walkabout/3d/normal.png","hrm":"https://badges.decentraland.org/assets/walkabout/3d/hrm.png","baseColor":"https://badges.decentraland.org/assets/walkabout/3d/baseColor.png"}}'::jsonb),
  ('emotionista', 'Emotionista', 'Play emotes in-world.', 'Socializer', true,
   '{"2d":{"normal":"https://badges.decentraland.org/assets/emotionista/2d/normal.png","hrm":"","baseColor":""},"3d":{"normal":"https://badges.decentraland.org/assets/emotionista/3d/normal.png","hrm":"https://badges.decentraland.org/assets/emotionista/3d/hrm.png","baseColor":"https://badges.decentraland.org/assets/emotionista/3d/baseColor.png"}}'::jsonb);

INSERT INTO social.badge_tiers (badge_id, tier_id, tier_name, description, assets, criteria_steps, ordinal) VALUES
  ('walkabout', 'walkabout-starter', 'Starter', 'Walk 1,000 meters.', '{"2d":{"normal":"https://badges.decentraland.org/assets/walkabout/starter/2d/normal.png"}}'::jsonb, 1000, 0),
  ('walkabout', 'walkabout-bronze', 'Bronze', 'Walk 10,000 meters.', '{"2d":{"normal":"https://badges.decentraland.org/assets/walkabout/bronze/2d/normal.png"}}'::jsonb, 10000, 1),
  ('walkabout', 'walkabout-silver', 'Silver', 'Walk 100,000 meters.', '{"2d":{"normal":"https://badges.decentraland.org/assets/walkabout/silver/2d/normal.png"}}'::jsonb, 100000, 2),
  ('walkabout', 'walkabout-gold', 'Gold', 'Walk 1,000,000 meters.', '{"2d":{"normal":"https://badges.decentraland.org/assets/walkabout/gold/2d/normal.png"}}'::jsonb, 1000000, 3),
  ('emotionista', 'emotionista-bronze', 'Bronze', 'Play 10 emotes.', '{"2d":{"normal":"https://badges.decentraland.org/assets/emotionista/bronze/2d/normal.png"}}'::jsonb, 10, 0),
  ('emotionista', 'emotionista-silver', 'Silver', 'Play 100 emotes.', '{"2d":{"normal":"https://badges.decentraland.org/assets/emotionista/silver/2d/normal.png"}}'::jsonb, 100, 1),
  ('emotionista', 'emotionista-gold', 'Gold', 'Play 1000 emotes.', '{"2d":{"normal":"https://badges.decentraland.org/assets/emotionista/gold/2d/normal.png"}}'::jsonb, 1000, 2);
-- ==== end section: badges ====

CREATE INDEX scene_stream_access_reconcile_idx ON social.scene_stream_access (streaming_checked_at NULLS FIRST, id) WHERE active AND streaming;

-- Workload: rare island cleanup transitions and exact-wallet credential checks;
-- no position writes. Historical wallet/attempt counts and write cost unmeasured.
-- The stable wallet key covers all Pulse sources and configured/unconfigured
-- ownership. It intentionally has no FK to replaceable assignment or ban rows.
CREATE TABLE social.island_admission_fences (
    wallet text PRIMARY KEY,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    token_floor bigint NOT NULL DEFAULT 0 CHECK (token_floor BETWEEN 0 AND 9007199254740),
    updated_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    CHECK (wallet <> '' AND wallet = lower(wallet))
);

-- One immutable remote attempt. A scheduling lease is never remote authority.
-- Policy intent deletion/lift/room_closed cannot delete these facts (no FK to
-- the policy source). Only positively settled attempts may release a hazard.
CREATE TABLE social.island_cleanup_dispatches (
    dispatch_id uuid PRIMARY KEY,
    wallet text NOT NULL REFERENCES social.island_admission_fences(wallet),
    purpose text NOT NULL CHECK (purpose IN ('pulse_takeover', 'island_enforcement')),
    pulse_source text,
    pulse_takeover_id text,
    enforcement_cause text,
    intent_root_key text,
    intent_target_key text,
    intent_generation uuid,
    room_name text NOT NULL CHECK (room_name <> ''),
    participant_identity text NOT NULL CHECK (participant_identity <> ''),
    cutoff bigint NOT NULL CHECK (cutoff BETWEEN 1 AND 9007199254740),
    fence_generation bigint NOT NULL CHECK (fence_generation > 0),
    phase text NOT NULL CHECK (phase IN ('initial', 'quarantine_final')),
    state text NOT NULL CHECK (state IN ('dispatched', 'quarantine', 'confirmed', 'rejected', 'settled_late', 'superseded')),
    quarantine_until timestamp without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    settled_at timestamp without time zone,
    UNIQUE (wallet, dispatch_id),
    CHECK ((purpose = 'pulse_takeover' AND pulse_source IS NOT NULL AND pulse_takeover_id IS NOT NULL
            AND pulse_source <> '' AND pulse_takeover_id <> '' AND enforcement_cause IS NULL AND intent_root_key IS NULL AND intent_target_key IS NULL AND intent_generation IS NULL)
        OR (purpose = 'island_enforcement' AND pulse_source IS NULL AND pulse_takeover_id IS NULL
            AND enforcement_cause IS NOT NULL AND enforcement_cause IN ('connection', 'platform', 'block', 'scene')
            AND intent_root_key IS NOT NULL AND intent_root_key <> '' AND intent_target_key IS NOT NULL AND intent_target_key <> '' AND intent_generation IS NOT NULL)),
    CHECK ((state = 'quarantine') = (quarantine_until IS NOT NULL)),
    CHECK ((state = 'dispatched') = (settled_at IS NULL))
);
-- Integrity: at most one unresolved destructive attempt for the wallet,
-- regardless of source incarnation, policy generation or optional audience.
CREATE UNIQUE INDEX island_cleanup_dispatches_active_wallet_idx
    ON social.island_cleanup_dispatches(wallet)
    WHERE state IN ('dispatched', 'quarantine', 'settled_late');
-- Exact logical-operation replay; attempt facts are never overwritten to retry.
CREATE INDEX island_cleanup_dispatches_takeover_idx
    ON social.island_cleanup_dispatches(wallet, pulse_source, pulse_takeover_id, fence_generation DESC)
    WHERE purpose = 'pulse_takeover';
CREATE INDEX island_cleanup_dispatches_enforcement_idx
    ON social.island_cleanup_dispatches(intent_root_key, intent_target_key, intent_generation, fence_generation DESC)
    WHERE purpose = 'island_enforcement';

-- Logical retained obligation, distinct from each RPC attempt. Predecessor is
-- immutable; acknowledged receipts retain the confirmed fence floor forever.
CREATE TABLE social.island_takeover_receipts (
    wallet text NOT NULL REFERENCES social.island_admission_fences(wallet),
    source_incarnation text NOT NULL,
    takeover_id text NOT NULL,
    predecessor_session text NOT NULL,
    predecessor_cluster text NOT NULL,
    predecessor_realm text NOT NULL,
    latest_dispatch uuid,
    acknowledged boolean NOT NULL DEFAULT false,
    -- Local confirmed-obligation disposal; never a Pulse acknowledgment.
    retired_by_source text,
    last_successor_session text NOT NULL,
    next_attempt_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    claim_id uuid,
    claim_until timestamp without time zone,
    created_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now()),
    PRIMARY KEY (wallet, source_incarnation, takeover_id),
    FOREIGN KEY (wallet, latest_dispatch) REFERENCES social.island_cleanup_dispatches(wallet, dispatch_id),
    CHECK (source_incarnation <> '' AND takeover_id <> '' AND predecessor_session <> '' AND predecessor_cluster <> ''),
    CHECK ((claim_id IS NULL) = (claim_until IS NULL)),
    CHECK (NOT acknowledged OR latest_dispatch IS NOT NULL),
    CONSTRAINT island_takeover_receipts_retirement_check CHECK (retired_by_source IS NULL OR
        (retired_by_source <> '' AND retired_by_source <> source_incarnation AND NOT acknowledged AND latest_dispatch IS NOT NULL))
);
-- Bounded prepared/completion scheduling; dispatched uncertainty is never due
-- for another RPC. Workers still inspect the immutable attempt before work.
CREATE INDEX island_takeover_receipts_due_idx
    ON social.island_takeover_receipts(next_attempt_at, wallet, source_incarnation, takeover_id)
    WHERE NOT acknowledged;
