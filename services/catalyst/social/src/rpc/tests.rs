//! The copied contract tests on the process's scratch database
//! (`db::scratch`, `CATALYST_TEST_DATABASE_URL`; skipped without it): the
//! friendship/block queries, the private-call busy and expiry semantics, the
//! soft-deleted community voice authority, the socket handshake, and the
//! end-to-end run unity-explorer does: two clients over real WebSockets, one
//! subscribes to friendship updates, the other sends a request, the event
//! arrives.

use std::sync::Arc;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use futures::{SinkExt, StreamExt};
use prost::Message;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use super::context::{Context, VoiceSettings};
use super::db::Db;
use super::drpc::client::{RpcClient, RpcClientModule, ServiceClient};
use super::drpc::service_module_definition::ProcedureContext;
use super::drpc::stream_protocol::Generator;
use super::drpc::transport::{Transport, TransportError, TransportMessage};
use super::handshake::test_support::{EPHEMERAL_KEY, ROOT_KEY, frame, frame_for, frame_signed};
use super::profiles::Profiles;
use super::proto::v2::*;
use super::pubsub::SocialEvent;
use super::service::{MODULE_NAME, SocialServiceImpl};
use super::voice::fake::FakeVoiceRooms;
use super::{Settings, State};
use crate::comms::gatekeeper::voice::db::{PendingPrivateCall, VoiceDb};
use crate::jobs::Jobs;

mod community_voice;
mod connectivity;
mod failures;
mod friends;
mod http;
mod private_voice;
mod settings;
mod socket;
mod upstream_reads;
mod voice_bugs;
mod voice_runtime;

const EXPIRATION_MS: i64 = 60_000;

/// A fresh lowercase address per test, derived from a uuid so parallel tests
/// on the shared scratch database never share rows.
fn addr(tag: &str) -> String {
    let hex = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let suffix = tag
        .bytes()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let head_len = 40usize.saturating_sub(suffix.len());
    format!("0x{}{}", &hex[..head_len], &suffix[..40 - head_len])
}

async fn scratch_pool() -> Option<PgPool> {
    crate::db::scratch::pool(6).await
}

async fn cleanup_pair(db: &Db, a: &str, b: &str) {
    let _ = sqlx::query("DELETE FROM social.blocks WHERE blocker_address IN ($1, $2) OR blocked_address IN ($1, $2)")
        .bind(a)
        .bind(b)
        .execute(db.pool())
        .await;
    let _ = sqlx::query(
        "DELETE FROM social.friendships WHERE LEAST(address_requester, address_requested) = LEAST($1, $2) \
         AND GREATEST(address_requester, address_requested) = GREATEST($1, $2)",
    )
    .bind(a)
    .bind(b)
    .execute(db.pool())
    .await;
}

fn test_ctx(db: Db, voice: Arc<dyn crate::comms::gatekeeper::voice::rooms::VoiceRooms>) -> Context {
    Context::new(
        VoiceSettings {
            private_voice_chat_expiration_ms: EXPIRATION_MS,
            private_voice_chat_expiration_batch_size: 20,
        },
        db,
        Profiles::new(None, Default::default()),
        voice,
    )
}

fn procedure_ctx(ctx: &Context, me: &str, transport_id: u32) -> ProcedureContext<Context> {
    ctx.register_identity(transport_id, me.to_string());
    ProcedureContext {
        server_context: Arc::new(ctx.clone()),
        transport_id,
    }
}

#[tokio::test]
async fn is_friendship_blocked_is_bidirectional() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb01"), addr("fb02"));
    assert!(!db.is_friendship_blocked(&a, &b).await.unwrap());
    db.block_user(&a, &b).await.unwrap();
    assert!(
        db.is_friendship_blocked(&a, &b).await.unwrap(),
        "blocker side"
    );
    assert!(
        db.is_friendship_blocked(&b, &a).await.unwrap(),
        "blocked side too"
    );
    db.unblock_user(&a, &b).await.unwrap();
    assert!(!db.is_friendship_blocked(&a, &b).await.unwrap());
    cleanup_pair(&db, &a, &b).await;
}

#[tokio::test]
async fn block_with_no_friendship_action_surfaces_via_blocks_table() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb03"), addr("fb04"));
    db.block_user(&a, &b).await.unwrap();
    assert!(
        db.last_friendship_action(&a, &b).await.unwrap().is_none(),
        "a block is not a friendship action"
    );
    assert!(db.is_blocked(&a, &b).await.unwrap());
    assert!(!db.is_blocked(&b, &a).await.unwrap());
    cleanup_pair(&db, &a, &b).await;
}

#[tokio::test]
async fn get_blocked_users_pages_and_counts_the_full_set() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let a = addr("fb05");
    let targets = [addr("fb06"), addr("fb07"), addr("fb08")];
    for t in &targets {
        db.block_user(&a, t).await.unwrap();
    }
    let (page, total) = db.get_blocked_users(&a, 2, 0).await.unwrap();
    assert_eq!((page.len(), total), (2, 3));
    let (rest, total) = db.get_blocked_users(&a, 2, 2).await.unwrap();
    assert_eq!((rest.len(), total), (1, 3));
    let (empty, total) = db.get_blocked_users(&a, 2, 9).await.unwrap();
    assert_eq!(
        (empty.len(), total),
        (0, 3),
        "an empty page past the end still reports the full count"
    );
    for t in &targets {
        cleanup_pair(&db, &a, t).await;
    }
}

#[tokio::test]
async fn friendship_action_outranks_a_block_row() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb09"), addr("fb0a"));
    let (id, _) = db
        .apply_friendship_action(&a, &b, "request", false, None, Some("hi"))
        .await
        .unwrap();
    db.apply_friendship_action(&b, &a, "accept", true, Some(id), None)
        .await
        .unwrap();
    db.block_user(&a, &b).await.unwrap();
    let last = db.last_friendship_action(&a, &b).await.unwrap().unwrap();
    assert_eq!(
        last.action, "accept",
        "the latest friendship action wins over the raw block row"
    );
    cleanup_pair(&db, &a, &b).await;
}

#[tokio::test]
async fn friendship_probe_reports_last_action_and_both_block_directions() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb0b"), addr("fb0c"));
    let probe = db.friendship_probe(&a, &b).await.unwrap();
    assert!(probe.last.is_none() && !probe.blocked && !probe.blocked_by);

    db.block_user(&b, &a).await.unwrap();
    let probe = db.friendship_probe(&a, &b).await.unwrap();
    assert!(
        probe.last.is_none() && !probe.blocked && probe.blocked_by,
        "B blocked A"
    );
    let probe = db.friendship_probe(&b, &a).await.unwrap();
    assert!(probe.blocked && !probe.blocked_by, "seen from B's side");
    db.unblock_user(&b, &a).await.unwrap();

    let (id, _) = db
        .apply_friendship_action(&a, &b, "request", false, None, Some("hi"))
        .await
        .unwrap();
    let probe = db.friendship_probe(&b, &a).await.unwrap();
    let last = probe.last.expect("the request is the last action");
    assert_eq!(
        (
            last.friendship_id,
            last.action.as_str(),
            last.acting_user.as_str(),
            last.is_active
        ),
        (id, "request", a.as_str(), false)
    );
    assert_eq!(
        db.last_friendship_action(&b, &a)
            .await
            .unwrap()
            .map(|l| (l.friendship_id, l.action)),
        Some((id, "request".to_string())),
        "the probe and the plain lookup agree"
    );
    cleanup_pair(&db, &a, &b).await;
}

#[tokio::test]
async fn apply_without_a_known_row_reuses_the_reversed_pair() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb0d"), addr("fb0e"));
    let (first, _) = db
        .apply_friendship_action(&a, &b, "request", false, None, None)
        .await
        .unwrap();
    let (second, _) = db
        .apply_friendship_action(&b, &a, "accept", true, None, None)
        .await
        .expect("a racing insert on the reversed pair resolves to the same row");
    assert_eq!(first, second, "one friendship row per unordered pair");
    let last = db.last_friendship_action(&a, &b).await.unwrap().unwrap();
    assert_eq!((last.action.as_str(), last.is_active), ("accept", true));
    // The projection on the row moves with the action, stamped with the same
    // statement time as the action row (the pending-requests page reads it).
    let (last_action, last_actor, is_active, projected_at, action_at): (
        String,
        String,
        bool,
        chrono::NaiveDateTime,
        chrono::NaiveDateTime,
    ) = sqlx::query_as(
        "SELECT f.last_action, f.last_actor, f.is_active, f.last_action_at, fa.timestamp \
         FROM social.friendships f JOIN social.friendship_actions fa ON fa.friendship_id = f.id \
         WHERE f.id = $1 ORDER BY fa.timestamp DESC, fa.id DESC LIMIT 1",
    )
    .bind(first)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        (last_action.as_str(), last_actor.as_str(), is_active),
        ("accept", b.as_str(), true)
    );
    assert_eq!(projected_at, action_at);
    let (friends, total) = db.get_friends(&a, 10, 0).await.unwrap();
    assert_eq!((friends, total), (vec![b.clone()], 1));
    let (friends, total) = db.get_friends(&a, 10, 5).await.unwrap();
    assert_eq!(
        (friends.len(), total),
        (0, 1),
        "an empty page still carries the count"
    );
    let (requests, total) = db.get_friendship_requests(&b, true, 10, 0).await.unwrap();
    assert_eq!(
        (requests.len(), total),
        (0, 0),
        "an accepted request is no longer pending"
    );
    cleanup_pair(&db, &a, &b).await;
}

#[tokio::test]
async fn pending_requests_list_the_latest_request_with_its_message() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b, c) = (addr("fb0f"), addr("fb10"), addr("fb11"));
    let (id_ab, _) = db
        .apply_friendship_action(&a, &b, "request", false, None, Some("hello b"))
        .await
        .unwrap();
    db.apply_friendship_action(&c, &b, "request", false, None, None)
        .await
        .unwrap();
    let (incoming, total) = db.get_friendship_requests(&b, true, 10, 0).await.unwrap();
    assert_eq!(total, 2);
    assert_eq!(incoming.len(), 2);
    let from_a = incoming
        .iter()
        .find(|r| r.address == a)
        .expect("a's request");
    assert_eq!(
        (from_a.id, from_a.message.as_deref()),
        (id_ab, Some("hello b"))
    );
    let (sent, total) = db.get_friendship_requests(&a, false, 10, 0).await.unwrap();
    assert_eq!((sent.len(), total), (1, 1));
    assert_eq!(sent[0].address, b);
    // A block in either direction hides the pending request.
    db.block_user(&b, &c).await.unwrap();
    let (incoming, total) = db.get_friendship_requests(&b, true, 10, 0).await.unwrap();
    assert_eq!((incoming.len(), total), (1, 1));
    assert_eq!(incoming[0].address, a);
    cleanup_pair(&db, &a, &b).await;
    cleanup_pair(&db, &c, &b).await;
}

#[tokio::test]
async fn block_and_unblock_return_the_latest_action_in_one_statement() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let (a, b) = (addr("fb12"), addr("fb13"));
    assert!(
        db.block_user_and_last(&a, &b).await.unwrap().is_none(),
        "no friendship row yet"
    );
    assert!(db.is_blocked(&a, &b).await.unwrap());
    db.block_user_and_last(&a, &b)
        .await
        .expect("blocking twice is idempotent");

    let (id, _) = db
        .apply_friendship_action(&a, &b, "request", false, None, None)
        .await
        .unwrap();
    let last = db.unblock_user_and_last(&a, &b).await.unwrap().unwrap();
    assert_eq!((last.friendship_id, last.action.as_str()), (id, "request"));
    assert!(!db.is_blocked(&a, &b).await.unwrap());

    let (blocked, blocked_by) = db.get_blocking_status(&a).await.unwrap();
    assert!(blocked.is_empty() && blocked_by.is_empty());
    db.block_user(&b, &a).await.unwrap();
    db.block_user(&a, &b).await.unwrap();
    let (blocked, blocked_by) = db.get_blocking_status(&a).await.unwrap();
    assert_eq!((blocked, blocked_by), (vec![b.clone()], vec![b.clone()]));
    cleanup_pair(&db, &a, &b).await;
}

/// Initial, live, and final queued-delivery eligibility use one both-direction rule.
#[tokio::test]
async fn connectivity_friend_pages_and_delivery_predicate_revoke_blocks_in_both_directions() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let actor = addr("cf1");
    let recipient = addr("cf2");
    db.apply_friendship_action(&actor, &recipient, "accept", true, None, None)
        .await
        .unwrap();
    for (blocker, blocked) in [(&actor, &recipient), (&recipient, &actor)] {
        assert!(
            db.connectivity_friend_allowed(&actor, &recipient)
                .await
                .unwrap()
        );
        assert_eq!(
            db.connectivity_friends_page(&recipient, "", true)
                .await
                .unwrap(),
            vec![actor.clone()]
        );
        let ctx = test_ctx(db.clone(), Arc::new(FakeVoiceRooms::new(pool.clone())));
        let bytes =
            crate::comms::protocol::pulse::decentraland::kernel::comms::v3::IslandStatusMessage {
                data: vec![
                    crate::comms::protocol::pulse::decentraland::kernel::comms::v3::IslandData {
                        peers: vec![actor.clone()],
                        ..Default::default()
                    },
                ],
            }
            .encode_to_vec();
        ctx.topology().receive(&bytes).unwrap();
        let generation = ctx.topology().snapshot().generation;
        let mut rx = ctx.pubsub().subscribe(&recipient);
        let queued_guard =
            ctx.friend_delivery_guard(recipient.clone(), actor.clone(), true, generation);
        assert_eq!(queued_guard().await, Ok(true));
        db.block_user(blocker, blocked).await.unwrap();
        assert!(
            queued_guard().await == Ok(false),
            "already queued profile/status is denied after committed block in either direction"
        );
        assert!(
            !db.connectivity_friend_allowed(&actor, &recipient)
                .await
                .unwrap()
        );
        assert!(
            !db.connectivity_friend_allowed(&recipient, &actor)
                .await
                .unwrap()
        );
        assert!(
            db.connectivity_friends_page(&recipient, "", true)
                .await
                .unwrap()
                .is_empty()
        );
        let _ = ctx
            .fan_topology_page(&actor, true, generation, Default::default())
            .await
            .unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        db.unblock_user(blocker, blocked).await.unwrap();
    }
    cleanup_pair(&db, &actor, &recipient).await;
    assert!(
        !db.connectivity_friend_allowed(&actor, &recipient)
            .await
            .unwrap(),
        "friendship revocation denies queued statuses"
    );
}

/// A wallet that never set its privacy reads `all` (upstream's absent-row
/// default), while a row a partial upsert creates stores the column default
/// `only_friends`; the row order and the friend/block logic are the old ones.
#[tokio::test]
async fn private_messages_settings_keeps_input_order_and_schema_defaults() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool);
    let me = addr("fb14");
    let (friend, stranger, blocked_friend) = (addr("fb15"), addr("fb16"), addr("fb17"));
    for t in [&friend, &blocked_friend] {
        db.apply_friendship_action(&me, t, "accept", true, None, None)
            .await
            .unwrap();
    }
    db.block_user(&blocked_friend, &me).await.unwrap();
    db.upsert_social_settings(&friend, Some("all"), None, None)
        .await
        .unwrap();

    let rows = db
        .private_messages_settings(
            &me,
            &[stranger.clone(), friend.clone(), blocked_friend.clone()],
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (stranger.clone(), "all".to_string(), false),
            (friend.clone(), "all".to_string(), true),
            (blocked_friend.clone(), "all".to_string(), false),
        ]
    );
    assert!(
        db.private_messages_settings(&me, &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.friend_addresses_unblocked(&me).await.unwrap(),
        vec![friend.clone()],
        "a block in either direction hides the friend"
    );

    // Settings: absent row reads as the defaults; a partial upsert keeps the rest
    // and creates the row with the column default.
    assert!(db.get_social_settings(&me).await.unwrap().is_none());
    assert_eq!(
        super::db::SocialSettingsRow::default().private_messages_privacy,
        "all"
    );
    let row = db
        .upsert_social_settings(&me, None, Some("do_not_show_messages"), None)
        .await
        .unwrap();
    assert_eq!(
        (
            row.private_messages_privacy.as_str(),
            row.blocked_users_messages_visibility.as_str(),
            row.show_situation_reactions.as_str()
        ),
        ("only_friends", "do_not_show_messages", "show")
    );
    let row = db
        .upsert_social_settings(&me, Some("all"), None, Some("hide"))
        .await
        .unwrap();
    assert_eq!(
        (
            row.private_messages_privacy.as_str(),
            row.blocked_users_messages_visibility.as_str(),
            row.show_situation_reactions.as_str()
        ),
        ("all", "do_not_show_messages", "hide")
    );
    assert!(db.reset_social_settings(&me).await.unwrap());
    assert!(!db.reset_social_settings(&me).await.unwrap());
    let _ = db.reset_social_settings(&friend).await;
    for t in [&friend, &stranger, &blocked_friend] {
        cleanup_pair(&db, &me, t).await;
    }
}

/// The expiry sweep is table-wide by design, so the tests that backdate and
/// reclaim calls on the shared scratch database take turns, and so does a test
/// whose server runs the sweep job against that database.
static VOICE_TABLE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn backdate(db: &Db, id: Uuid, ms: i64) {
    sqlx::query("UPDATE social.private_voice_chats SET created_at = created_at - ($2 * interval '1 millisecond') WHERE id = $1")
        .bind(id)
        .bind(ms)
        .execute(db.pool())
        .await
        .unwrap();
}

async fn call_exists(db: &Db, id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social.private_voice_chats WHERE id = $1)")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn expired_prior_call_does_not_block_new_one() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool);
    let (caller, callee) = (addr("pv01"), addr("pv02"));
    let stale_id = VoiceDb::new(db.pool().clone(), Default::default())
        .seed_pending_private_call(&caller, &callee)
        .await
        .unwrap();
    backdate(&db, stale_id, EXPIRATION_MS + 3_600_000).await;
    assert!(
        call_exists(&db, stale_id).await,
        "stale row is present (not yet swept)"
    );
    assert!(
        !db.are_users_being_called_or_calling_someone(
            &[caller.clone(), callee.clone()],
            EXPIRATION_MS
        )
        .await
        .unwrap(),
        "an expired prior call must not block a new one"
    );
    // The locked start reclaims the stale row (the UNIQUEs would otherwise
    // refuse the insert) and reports it expired.
    let outcome = VoiceDb::new(db.pool().clone(), Default::default())
        .create_pending_private_call(&caller, &callee, EXPIRATION_MS)
        .await
        .unwrap();
    assert!(outcome.started.is_some());
    assert_eq!(
        outcome.expired.iter().map(|c| c.id).collect::<Vec<_>>(),
        vec![stale_id]
    );
    assert!(!call_exists(&db, stale_id).await);
    VoiceDb::new(db.pool().clone(), Default::default())
        .finish_pending_private_call(PendingPrivateCall::Id(outcome.started.unwrap()))
        .await
        .unwrap();
}

#[tokio::test]
async fn live_call_still_blocks_symmetrically() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool);
    let (caller, callee, other) = (addr("pv03"), addr("pv04"), addr("pv05"));
    let live_id = VoiceDb::new(db.pool().clone(), Default::default())
        .seed_pending_private_call(&caller, &callee)
        .await
        .unwrap();
    for probe in [
        vec![caller.clone()],
        vec![callee.clone()],
        vec![other.clone(), caller.clone()],
        vec![callee.clone(), other.clone()],
    ] {
        assert!(
            db.are_users_being_called_or_calling_someone(&probe, EXPIRATION_MS)
                .await
                .unwrap(),
            "a live call must block (probe={probe:?})"
        );
    }
    let outcome = VoiceDb::new(db.pool().clone(), Default::default())
        .create_pending_private_call(&other, &callee, EXPIRATION_MS)
        .await
        .unwrap();
    assert!(
        outcome.started.is_none() && outcome.expired.is_empty(),
        "the callee is busy"
    );
    VoiceDb::new(db.pool().clone(), Default::default())
        .finish_pending_private_call(PendingPrivateCall::Id(live_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn sweep_reclaims_the_same_rows_the_busy_check_skips() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool);
    let (caller, callee) = (addr("pv06"), addr("pv07"));
    let stale_id = VoiceDb::new(db.pool().clone(), Default::default())
        .seed_pending_private_call(&caller, &callee)
        .await
        .unwrap();
    backdate(&db, stale_id, EXPIRATION_MS + 3_600_000).await;
    let reclaimed = VoiceDb::new(db.pool().clone(), Default::default())
        .expire_pending_private_calls(EXPIRATION_MS, 20)
        .await
        .unwrap();
    assert!(
        reclaimed.iter().any(|c| c.id == stale_id),
        "the sweep must reclaim the stale row"
    );
    assert!(!call_exists(&db, stale_id).await);
}

#[tokio::test]
async fn sweep_tick_issues_no_delete_until_a_call_is_due() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool.clone());
    let (caller, callee) = (addr("pv08"), addr("pv09"));
    let ctx = test_ctx(db.clone(), Arc::new(FakeVoiceRooms::new(pool.clone())));

    // A fresh process sweeps on its first tick; then the idle cadence arms.
    ctx.sweep_private_voice_chats_if_due(EXPIRATION_MS, 20)
        .await;
    let now = chrono::Utc::now().timestamp_millis();
    assert!(
        !db.private_voice_sweep_due(now + 1_000),
        "with nothing pending the next tick must not sweep"
    );

    let id = ctx
        .voice()
        .create_pending_private_call(&caller, &callee, EXPIRATION_MS)
        .await
        .unwrap()
        .started
        .expect("both free");
    db.note_private_voice_due(chrono::Utc::now().timestamp_millis() + EXPIRATION_MS);
    assert!(
        !db.private_voice_sweep_due(now + 1_000),
        "a call started here is not due on the next tick"
    );
    assert!(
        db.private_voice_sweep_due(now + EXPIRATION_MS + 1_000),
        "a call started here is due once its TTL elapses"
    );

    backdate(&db, id, EXPIRATION_MS + 5_000).await;
    assert_eq!(
        ctx.sweep_private_voice_chats_if_due(EXPIRATION_MS, 20)
            .await,
        0,
        "a tick before the noted due instant issues no DELETE"
    );
    db.note_private_voice_due(0);
    // Other tests' expired calls may be reclaimed in the same sweep; ours is among them.
    assert!(
        ctx.sweep_private_voice_chats_if_due(EXPIRATION_MS, 20)
            .await
            >= 1
    );
    assert!(!call_exists(&db, id).await);
}

#[tokio::test]
async fn self_call_is_forbidden_before_any_read() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let ctx = test_ctx(
        Db::new(pool.clone()),
        Arc::new(FakeVoiceRooms::new(pool.clone())),
    );
    let me = addr("pv0a");
    let response = SocialServiceImpl
        .start_private_voice_chat(
            StartPrivateVoiceChatPayload {
                callee: Some(User {
                    address: me.clone(),
                }),
            },
            procedure_ctx(&ctx, &me, 1),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(start_private_voice_chat_response::Response::ForbiddenError(
                _
            ))
        ),
        "{response:?}"
    );
}

#[tokio::test]
async fn a_missing_voice_runtime_refuses_the_call_explicitly() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let ctx = test_ctx(
        Db::new(pool),
        Arc::new(crate::comms::gatekeeper::voice::rooms::NoVoiceRuntime),
    );
    let (me, callee) = (addr("pv0b"), addr("pv0c"));
    let response = SocialServiceImpl
        .start_private_voice_chat(
            StartPrivateVoiceChatPayload {
                callee: Some(User { address: callee }),
            },
            procedure_ctx(&ctx, &me, 1),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(start_private_voice_chat_response::Response::InternalServerError(_))
        ),
        "{response:?}"
    );
}

#[tokio::test]
async fn blocked_pair_cannot_start_call_and_friends_can() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool.clone());
    let voice = Arc::new(FakeVoiceRooms::new(pool.clone()));
    let ctx = test_ctx(db.clone(), voice.clone());
    let (caller, callee) = (addr("pv0d"), addr("pv0e"));
    let start = |ctx: &Context, me: &str, other: &str| {
        let pctx = procedure_ctx(ctx, me, 1);
        let payload = StartPrivateVoiceChatPayload {
            callee: Some(User {
                address: other.to_string(),
            }),
        };
        async move {
            SocialServiceImpl
                .start_private_voice_chat(payload, pctx)
                .await
                .unwrap()
        }
    };

    db.block_user(&callee, &caller).await.unwrap();
    let response = start(&ctx, &caller, &callee).await;
    assert!(
        matches!(
            response.response,
            Some(start_private_voice_chat_response::Response::ForbiddenError(
                _
            ))
        ),
        "{response:?}"
    );
    db.unblock_user(&callee, &caller).await.unwrap();

    // Without settings rows both read `all`; a stranger the callee limited to
    // friends is refused, and friends pass.
    let pre = db
        .private_voice_preflight(&caller, &callee, 60_000)
        .await
        .unwrap();
    assert_eq!(
        (pre.caller_privacy.as_str(), pre.callee_privacy.as_str()),
        ("all", "all")
    );
    db.upsert_social_settings(&callee, Some("only_friends"), None, None)
        .await
        .unwrap();
    let response = start(&ctx, &caller, &callee).await;
    assert!(
        matches!(
            response.response,
            Some(start_private_voice_chat_response::Response::ForbiddenError(
                _
            ))
        ),
        "{response:?}"
    );
    db.apply_friendship_action(&caller, &callee, "accept", true, None, None)
        .await
        .unwrap();
    let mut callee_rx = ctx.pubsub().subscribe(&callee);
    let response = start(&ctx, &caller, &callee).await;
    let Some(start_private_voice_chat_response::Response::Ok(ok)) = response.response else {
        panic!("{response:?}");
    };
    let call_id = Uuid::parse_str(&ok.call_id).unwrap();
    let event = callee_rx.try_recv().expect("the callee is rung");
    assert!(
        matches!(&*event, super::pubsub::SocialEvent::PrivateVoice(u) if u.call_id == ok.call_id && u.status == PrivateVoiceChatStatus::VoiceChatRequested as i32)
    );

    // A second call by either party is busy.
    let response = start(&ctx, &callee, &caller).await;
    assert!(
        matches!(
            response.response,
            Some(start_private_voice_chat_response::Response::ConflictingError(_))
        ),
        "{response:?}"
    );

    // Only the callee accepts; the grants come from the runtime.
    let accept = |ctx: &Context, me: &str| {
        let pctx = procedure_ctx(ctx, me, 2);
        let payload = AcceptPrivateVoiceChatPayload {
            call_id: ok.call_id.clone(),
        };
        async move {
            SocialServiceImpl
                .accept_private_voice_chat(payload, pctx)
                .await
                .unwrap()
        }
    };
    let response = accept(&ctx, &caller).await;
    assert!(
        matches!(
            response.response,
            Some(accept_private_voice_chat_response::Response::ForbiddenError(_))
        ),
        "{response:?}"
    );
    let mut caller_rx = ctx.pubsub().subscribe(&caller);
    let response = accept(&ctx, &callee).await;
    let Some(accept_private_voice_chat_response::Response::Ok(accepted)) = response.response else {
        panic!("{response:?}");
    };
    assert!(
        accepted
            .credentials
            .unwrap()
            .connection_url
            .contains(&format!("voice-chat-private-{call_id}"))
    );
    let event = caller_rx
        .try_recv()
        .expect("the caller learns of the acceptance");
    assert!(
        matches!(&*event, super::pubsub::SocialEvent::PrivateVoice(u) if u.status == PrivateVoiceChatStatus::VoiceChatAccepted as i32 && u.credentials.is_some())
    );

    // Ending tears the room down through the runtime and tells both parties.
    let response = SocialServiceImpl
        .end_private_voice_chat(
            EndPrivateVoiceChatPayload {
                call_id: ok.call_id.clone(),
            },
            procedure_ctx(&ctx, &caller, 3),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(end_private_voice_chat_response::Response::Ok(_))
        ),
        "{response:?}"
    );
    assert!(
        voice
            .calls
            .lock()
            .iter()
            .any(|c| c.starts_with(&format!("end-private {call_id}")))
    );
    assert!(!call_exists(&db, call_id).await);
    cleanup_pair(&db, &caller, &callee).await;
}

#[tokio::test]
async fn reject_and_disconnect_delete_through_voice_owner_before_signalling() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let db = Db::new(pool.clone());
    let voice = Arc::new(FakeVoiceRooms::new(pool.clone()));
    let ctx = test_ctx(db.clone(), voice.clone());
    let (caller, callee) = (addr("pvow1"), addr("pvow2"));
    let store = VoiceDb::new(pool, Default::default());
    let mut caller_rx = ctx.pubsub().subscribe(&caller);
    let mut callee_rx = ctx.pubsub().subscribe(&callee);
    let rejected = store
        .seed_pending_private_call(&caller, &callee)
        .await
        .unwrap();
    let response = SocialServiceImpl
        .reject_private_voice_chat(
            RejectPrivateVoiceChatPayload {
                call_id: rejected.to_string(),
            },
            procedure_ctx(&ctx, &callee, 41),
        )
        .await
        .unwrap();
    assert!(matches!(
        response.response,
        Some(reject_private_voice_chat_response::Response::Ok(_))
    ));
    assert!(!call_exists(&db, rejected).await);
    let event = caller_rx.try_recv().unwrap();
    assert!(
        matches!(&*event, SocialEvent::PrivateVoice(u) if u.call_id == rejected.to_string() && u.status == PrivateVoiceChatStatus::VoiceChatRejected as i32)
    );
    assert!(callee_rx.try_recv().is_err());
    assert!(
        voice.calls.lock().is_empty(),
        "reject does not tear down a media room"
    );

    let dropped = store
        .seed_pending_private_call(&caller, &callee)
        .await
        .unwrap();
    ctx.end_private_voice_chat_on_disconnect(&callee.to_uppercase())
        .await;
    assert!(!call_exists(&db, dropped).await);
    for event in [caller_rx.try_recv().unwrap(), callee_rx.try_recv().unwrap()] {
        assert!(
            matches!(&*event, SocialEvent::PrivateVoice(u) if u.call_id == dropped.to_string() && u.status == PrivateVoiceChatStatus::VoiceChatEnded as i32)
        );
    }
    assert!(
        voice
            .calls
            .lock()
            .iter()
            .any(|c| c == &format!("end-private {dropped} {callee}"))
    );
}

async fn seed_community(pool: &PgPool, active: bool, address: &str, role: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO social.communities (id, name, description, owner_address, private, active, unlisted) \
         VALUES ($1, 'Voice Soft Delete Community', 'description', $2, FALSE, $3, FALSE)",
    )
    .bind(id)
    .bind(address)
    .bind(active)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(address)
        .bind(role)
        .execute(pool)
        .await
        .unwrap();
    id
}

async fn drop_community(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM social.communities WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

#[tokio::test]
async fn community_role_ignores_members_of_a_soft_deleted_community() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let owner = addr("cv01");
    let live = seed_community(&pool, true, &owner, "owner").await;
    assert_eq!(
        db.community_role(&live.to_string(), &owner).await.unwrap(),
        Some("owner".to_string())
    );
    let deleted = seed_community(&pool, false, &owner, "owner").await;
    assert_eq!(
        db.community_role(&deleted.to_string(), &owner)
            .await
            .unwrap(),
        None,
        "a soft-deleted community grants no role"
    );
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM social.community_members WHERE community_id = $1")
            .bind(deleted)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(rows, 1, "the membership row survives the soft delete");
    drop_community(&pool, live).await;
    drop_community(&pool, deleted).await;
}

/// Actual SQL paging fixture, guarded by the configured PostgreSQL scratch database.
#[tokio::test]
async fn connectivity_replay_members_pages_exclude_self_and_cover_multiple_communities() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let me = addr("rp0");
    let first = seed_community(&pool, true, &me, "member").await;
    let second = seed_community(&pool, true, &me, "member").await;
    let mut expected = std::collections::BTreeSet::new();
    for index in 0..300 {
        let member = addr(&format!("rp{index}"));
        let community = if index % 2 == 0 { first } else { second };
        sqlx::query("INSERT INTO social.community_members (community_id,member_address,role) VALUES ($1,$2,'member')")
            .bind(community).bind(&member).execute(&pool).await.unwrap();
        expected.insert((community, member));
    }
    let mut cursor = None;
    let mut actual = std::collections::BTreeSet::new();
    let mut sizes = Vec::new();
    loop {
        let page = db
            .connectivity_members_page(&me, cursor.take())
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.iter().all(|(_, member)| member != &me));
        assert!(page.len() <= 256);
        cursor = page.last().cloned();
        sizes.push(page.len());
        actual.extend(page);
    }
    assert_eq!(sizes, vec![256, 44]);
    assert_eq!(actual, expected);
    drop_community(&pool, first).await;
    drop_community(&pool, second).await;
}

/// A deactivated mirrored community keeps its local members, inert: none of
/// them is a co-member for the presence fan-out.
#[tokio::test]
async fn an_inactive_community_fans_out_to_no_co_member() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let (me, here, there) = (addr("cm01"), addr("cm02"), addr("cm03"));
    let live = seed_community(&pool, true, &me, "member").await;
    let gone = seed_community(&pool, false, &me, "member").await;
    sqlx::query("UPDATE social.communities SET origin = 'upstream' WHERE id = ANY($1)")
        .bind(vec![live, gone])
        .execute(&pool)
        .await
        .unwrap();
    for (community, member) in [(live, &here), (gone, &there)] {
        sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'member')")
            .bind(community)
            .bind(member)
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(
        db.community_co_members(&me).await.unwrap(),
        [(live.to_string(), here.clone())],
        "only the active community fans out"
    );
    drop_community(&pool, live).await;
    drop_community(&pool, gone).await;
}

#[tokio::test]
async fn a_former_owner_cannot_start_or_end_voice_chat_in_a_deleted_community() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let ctx = test_ctx(db, Arc::new(FakeVoiceRooms::new(pool.clone())));
    let owner = addr("cv02");
    let deleted = seed_community(&pool, false, &owner, "owner").await;
    let response = SocialServiceImpl
        .start_community_voice_chat(
            StartCommunityVoiceChatPayload {
                community_id: deleted.to_string(),
            },
            procedure_ctx(&ctx, &owner, 1),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(start_community_voice_chat_response::Response::ForbiddenError(_))
        ),
        "{response:?}"
    );
    let response = SocialServiceImpl
        .end_community_voice_chat(
            EndCommunityVoiceChatPayload {
                community_id: deleted.to_string(),
            },
            procedure_ctx(&ctx, &owner, 1),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(end_community_voice_chat_response::Response::ForbiddenError(
                _
            ))
        ),
        "{response:?}"
    );
    drop_community(&pool, deleted).await;
}

#[tokio::test]
async fn community_voice_start_join_and_ban_semantics() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let db = Db::new(pool.clone());
    let voice = Arc::new(FakeVoiceRooms::new(pool.clone()));
    let ctx = test_ctx(db.clone(), voice.clone());
    let (owner, member, banned, stranger) =
        (addr("cv03"), addr("cv04"), addr("cv05"), addr("cv06"));
    let community = seed_community(&pool, true, &owner, "owner").await;
    sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'member')")
        .bind(community)
        .bind(&member)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO social.community_bans (community_id, banned_address, banned_by) VALUES ($1, $2, $3)")
        .bind(community)
        .bind(&banned)
        .bind(&owner)
        .execute(&pool)
        .await
        .unwrap();
    let cid = community.to_string();

    let mut member_rx = ctx.pubsub().subscribe(&member);
    let response = SocialServiceImpl
        .start_community_voice_chat(
            StartCommunityVoiceChatPayload {
                community_id: cid.clone(),
            },
            procedure_ctx(&ctx, &owner, 1),
        )
        .await
        .unwrap();
    let Some(start_community_voice_chat_response::Response::Ok(ok)) = response.response else {
        panic!("{response:?}");
    };
    assert!(
        ok.credentials
            .unwrap()
            .connection_url
            .contains(&format!("voice-chat-community-{community}"))
    );
    let event = member_rx
        .try_recv()
        .expect("members are told the room started");
    assert!(
        matches!(&*event, super::pubsub::SocialEvent::CommunityVoice(u) if u.community_id == cid && u.community_name == "Voice Soft Delete Community")
    );

    let response = SocialServiceImpl
        .start_community_voice_chat(
            StartCommunityVoiceChatPayload {
                community_id: cid.clone(),
            },
            procedure_ctx(&ctx, &member, 2),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            response.response,
            Some(start_community_voice_chat_response::Response::ForbiddenError(_))
        ),
        "a member cannot start"
    );

    let join = |ctx: &Context, me: &str| {
        let pctx = procedure_ctx(ctx, me, 3);
        let payload = JoinCommunityVoiceChatPayload {
            community_id: cid.clone(),
        };
        async move {
            SocialServiceImpl
                .join_community_voice_chat(payload, pctx)
                .await
                .unwrap()
        }
    };
    assert!(matches!(
        join(&ctx, &member).await.response,
        Some(join_community_voice_chat_response::Response::Ok(_))
    ));
    // Recorded difference (RPC.md, community voice, "Join: a public non-member
    // joins"): upstream lets a non-member join a public community
    // (`src/logic/community-voice/validation.ts:49-53`); Catalyst requires membership.
    assert!(matches!(
        join(&ctx, &stranger).await.response,
        Some(join_community_voice_chat_response::Response::ForbiddenError(_))
    ));
    // A ban deletes the member row, so a banned wallet is refused as a
    // non-member; a banned wallet whose row survived (a race) is refused as
    // banned. Either way no grant is minted.
    let refused = join(&ctx, &banned).await;
    assert!(
        matches!(&refused.response, Some(join_community_voice_chat_response::Response::ForbiddenError(f)) if f.message.as_deref() == Some("not a community member")),
        "{refused:?}"
    );
    sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'member')")
        .bind(community)
        .bind(&banned)
        .execute(&pool)
        .await
        .unwrap();
    let refused = join(&ctx, &banned).await;
    assert!(
        matches!(&refused.response, Some(join_community_voice_chat_response::Response::ForbiddenError(f)) if f.message.as_deref() == Some("banned from this community")),
        "{refused:?}"
    );
    assert!(
        !voice.calls.lock().iter().any(|c| c.contains(&banned)),
        "no grant is minted for a banned wallet"
    );

    // Owner protection: a moderator cannot kick the owner; the owner can kick a member.
    sqlx::query("UPDATE social.community_members SET role = 'moderator' WHERE community_id = $1 AND member_address = $2")
        .bind(community)
        .bind(&member)
        .execute(&pool)
        .await
        .unwrap();
    let kick = |ctx: &Context, me: &str, target: &str| {
        let pctx = procedure_ctx(ctx, me, 4);
        let payload = KickPlayerFromCommunityVoiceChatPayload {
            community_id: cid.clone(),
            user_address: target.to_string(),
        };
        async move {
            SocialServiceImpl
                .kick_player_from_community_voice_chat(payload, pctx)
                .await
                .unwrap()
        }
    };
    assert!(matches!(
        kick(&ctx, &member, &owner).await.response,
        Some(kick_player_from_community_voice_chat_response::Response::ForbiddenError(_))
    ));
    assert!(matches!(
        kick(&ctx, &owner, &member).await.response,
        Some(kick_player_from_community_voice_chat_response::Response::Ok(_))
    ));
    assert!(
        voice
            .calls
            .lock()
            .iter()
            .any(|c| c == &format!("kick {community} {member}"))
    );

    drop_community(&pool, community).await;
}

struct Server {
    addr: std::net::SocketAddr,
    state: Arc<State>,
    jobs: Jobs,
}

async fn spawn_server(pool: Option<PgPool>, auth_window_secs: i64) -> Server {
    let settings = Settings {
        auth_window_secs,
        ..Settings::default()
    };
    spawn_server_with(pool, settings, None).await
}

/// [`spawn_server`] with its settings and the EIP-1654 validator the handshake uses.
async fn spawn_server_with(
    pool: Option<PgPool>,
    settings: Settings,
    eip1654: Option<Arc<dyn crate::auth::eip1654::Eip1654Validator>>,
) -> Server {
    let pool = pool.unwrap_or_else(|| {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap()
    });
    let ctx = test_ctx(
        Db::new(pool.clone()),
        Arc::new(FakeVoiceRooms::new(pool.clone())),
    );
    let state = Arc::new(State::with_context(settings, ctx, eip1654));
    let jobs = Jobs::new();
    state.start_topology(&jobs, Arc::new(crate::comms::topology::Topology::default()));
    state.spawn_jobs(&jobs);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = super::router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { addr, state, jobs }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(server: &Server, path: &str) -> WsStream {
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{}{path}", server.addr))
        .await
        .unwrap();
    socket
}

async fn next_message(socket: &mut WsStream) -> WsMessage {
    tokio::time::timeout(crate::shared::test_wait::HANG_GUARD, socket.next())
        .await
        .expect("the server never sent a frame")
        .expect("stream yields")
        .expect("frame ok")
}

fn close_code(msg: &WsMessage) -> Option<u16> {
    match msg {
        WsMessage::Close(Some(frame)) => Some(frame.code.into()),
        _ => None,
    }
}

#[tokio::test]
async fn ws_handshake_valid_chain_receives_server_ready() {
    let server = spawn_server(None, 5).await;
    let mut socket = connect(&server, "/social-rpc").await;
    let (_signer, frame) = frame("/", chrono::Utc::now().timestamp_millis()).await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    let msg = next_message(&mut socket).await;
    let WsMessage::Binary(b) = msg else {
        panic!("expected the binary SERVER_READY frame, got {msg:?}")
    };
    let (kind, _) = super::drpc::rpc_protocol::parse::parse_header(&b).expect("a protocol header");
    assert_eq!(
        kind,
        super::drpc::rpc_protocol::RpcMessageTypes::ServerReady
    );
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn ws_handshake_garbage_or_scene_signed_frame_gets_close_3003() {
    let server = spawn_server(None, 5).await;
    let mut socket = connect(&server, "/social-rpc").await;
    socket
        .send(WsMessage::Text("definitely not signed-fetch json".into()))
        .await
        .unwrap();
    assert_eq!(close_code(&next_message(&mut socket).await), Some(3003));

    let mut socket = connect(&server, "/social-rpc").await;
    let ts = chrono::Utc::now().timestamp_millis();
    let metadata = r#"{"Signer":"decentraland-kernel-scene"}"#;
    let payload =
        crate::shared::signed_fetch::build_payload_v6("get", "/", &ts.to_string(), metadata);
    let (_, frame) =
        super::handshake::test_support::frame_with(ROOT_KEY, ts, &payload, metadata).await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    assert_eq!(close_code(&next_message(&mut socket).await), Some(3003));
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn ws_handshake_timeout_closes_connection() {
    let server = spawn_server(None, 1).await;
    let mut socket = connect(&server, "/social-rpc").await;
    // The server's one-second handshake timeout closes the socket; the wait only guards a hang.
    let msg = tokio::time::timeout(crate::shared::test_wait::HANG_GUARD, socket.next())
        .await
        .expect("the handshake timeout never closed the socket");
    match msg {
        Some(Ok(WsMessage::Close(_))) | None => {}
        other => panic!("expected Close or stream end, got {other:?}"),
    }
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// The client half of the socket: binary frames are the transport, text,
/// ping and pong are skipped, a close ends it.
struct TungsteniteTransport {
    read: Mutex<futures::stream::SplitStream<WsStream>>,
    write: Mutex<futures::stream::SplitSink<WsStream, WsMessage>>,
}

impl TungsteniteTransport {
    fn new(socket: WsStream) -> Self {
        let (write, read) = socket.split();
        Self {
            read: Mutex::new(read),
            write: Mutex::new(write),
        }
    }
}

#[async_trait::async_trait]
impl Transport for TungsteniteTransport {
    async fn receive(&self) -> Result<TransportMessage, TransportError> {
        loop {
            match self.read.lock().await.next().await {
                Some(Ok(WsMessage::Binary(b))) => return Ok(b.to_vec()),
                Some(Ok(WsMessage::Ping(_)))
                | Some(Ok(WsMessage::Pong(_)))
                | Some(Ok(WsMessage::Text(_))) => continue,
                Some(Ok(WsMessage::Close(_))) | None => return Err(TransportError::Closed),
                Some(Ok(WsMessage::Frame(_))) => continue,
                Some(Err(e)) => return Err(TransportError::Internal(Box::new(e))),
            }
        }
    }

    async fn send(&self, message: TransportMessage) -> Result<(), TransportError> {
        self.write
            .lock()
            .await
            .send(WsMessage::Binary(message.into()))
            .await
            .map_err(|_| TransportError::Closed)
    }

    async fn close(&self) {
        let _ = self.write.lock().await.close().await;
    }
}

/// The explorer's view of the module: the procedures this test drives.
struct SocialClient {
    module: RpcClientModule<TungsteniteTransport>,
}

impl ServiceClient<TungsteniteTransport> for SocialClient {
    fn set_client_module(module: RpcClientModule<TungsteniteTransport>) -> Self {
        Self { module }
    }
}

impl SocialClient {
    async fn upsert_friendship(
        &self,
        payload: UpsertFriendshipPayload,
    ) -> UpsertFriendshipResponse {
        self.module
            .call_unary_procedure("UpsertFriendship", payload)
            .await
            .expect("UpsertFriendship")
    }

    async fn get_friendship_status(&self, other: &str) -> GetFriendshipStatusResponse {
        self.module
            .call_unary_procedure(
                "GetFriendshipStatus",
                GetFriendshipStatusPayload {
                    user: Some(User {
                        address: other.to_string(),
                    }),
                },
            )
            .await
            .expect("GetFriendshipStatus")
    }

    async fn get_friends(&self) -> PaginatedFriendsProfilesResponse {
        self.module
            .call_unary_procedure("GetFriends", GetFriendsPayload { pagination: None })
            .await
            .expect("GetFriends")
    }

    async fn subscribe_to_friendship_updates(&self) -> Generator<FriendshipUpdate> {
        self.module
            .call_server_streams_procedure("SubscribeToFriendshipUpdates", ())
            .await
            .expect("SubscribeToFriendshipUpdates")
    }

    async fn subscribe_to_community_member_connectivity_updates(
        &self,
    ) -> Generator<CommunityMemberConnectivityUpdate> {
        self.module
            .call_server_streams_procedure("SubscribeToCommunityMemberConnectivityUpdates", ())
            .await
            .expect("SubscribeToCommunityMemberConnectivityUpdates")
    }

    async fn subscribe_to_friend_connectivity_updates(
        &self,
    ) -> Generator<FriendConnectivityUpdate> {
        self.module
            .call_server_streams_procedure("SubscribeToFriendConnectivityUpdates", ())
            .await
            .expect("SubscribeToFriendConnectivityUpdates")
    }
}

/// Connects, authenticates as the wallet behind `root_key` and loads the module.
async fn explorer(
    server: &Server,
    root_key: &str,
) -> (String, RpcClient<TungsteniteTransport>, SocialClient) {
    let (signer, frame) = frame_for(
        root_key,
        "/social-rpc",
        chrono::Utc::now().timestamp_millis(),
    )
    .await;
    signed_in(server, signer, frame).await
}

/// [`explorer`] as a wallet the caller holds, with an ephemeral key made for
/// the call, so no key is written out as text.
async fn explorer_as(
    server: &Server,
    root: &PrivateKeySigner,
) -> (String, RpcClient<TungsteniteTransport>, SocialClient) {
    let ts = chrono::Utc::now().timestamp_millis();
    let payload =
        crate::shared::signed_fetch::build_payload_v6("get", "/social-rpc", &ts.to_string(), "{}");
    let (signer, frame) = frame_signed(root, &PrivateKeySigner::random(), ts, &payload, "{}").await;
    signed_in(server, signer, frame).await
}

async fn signed_in(
    server: &Server,
    signer: String,
    frame: String,
) -> (String, RpcClient<TungsteniteTransport>, SocialClient) {
    let mut socket = connect(server, "/social-rpc").await;
    socket.send(WsMessage::Text(frame.into())).await.unwrap();
    let mut client = RpcClient::new(TungsteniteTransport::new(socket))
        .await
        .expect("SERVER_READY");
    let port = client.create_port("social").await.expect("port");
    let social: SocialClient = port.load_module(MODULE_NAME).await.expect("module");
    (signer, client, social)
}

async fn next_update<T>(generator: &mut Generator<T>) -> T
where
    T: Send + Sync + 'static,
{
    tokio::time::timeout(crate::shared::test_wait::HANG_GUARD, generator.next())
        .await
        .expect("the subscription never yielded an update")
        .expect("stream open")
}

/// The server half of a stream subscribes only once it reads the client's
/// acknowledgement of the opening, after the client's call returned; an
/// event published before then is not the stream's.
async fn subscribed<T>(
    server: &Server,
    address: &str,
    subscribe: impl std::future::Future<Output = Generator<T>>,
) -> Generator<T> {
    let pubsub = server.state.ctx.pubsub();
    let before = pubsub.subscriber_count(address);
    let generator = subscribe.await;
    assert!(
        crate::shared::test_wait::until(async || pubsub.subscriber_count(address) > before).await,
        "the server never subscribed {address}"
    );
    generator
}

/// A Pulse island frame naming `wallets` as present.
fn pulse_frame(wallets: Vec<String>) -> Vec<u8> {
    use crate::comms::protocol::pulse::decentraland::kernel::comms::v3::{
        IslandData, IslandStatusMessage,
    };
    IslandStatusMessage {
        data: vec![IslandData {
            peers: wallets,
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// A second wallet for the two-client run.
const SECOND_ROOT_KEY: &str = "7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_explorers_exchange_a_friend_request_over_the_socket() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let server = spawn_server(Some(pool.clone()), 5).await;
    let db = Db::new(pool.clone());
    let _ = EPHEMERAL_KEY;

    let (alice, _alice_client, alice_social) = explorer(&server, ROOT_KEY).await;
    let (bob, _bob_client, bob_social) = explorer(&server, SECOND_ROOT_KEY).await;
    assert_ne!(alice, bob);
    cleanup_pair(&db, &alice, &bob).await;
    let (alice_addr, bob_addr) = (
        crate::Address::parse(&alice).unwrap(),
        crate::Address::parse(&bob).unwrap(),
    );

    assert!(
        !server.state.ctx.is_online(&alice) && !server.state.ctx.is_online(&bob),
        "handshakes do not establish Pulse presence"
    );
    server
        .state
        .ctx
        .topology()
        .receive(&pulse_frame(vec![alice.clone(), bob.clone()]))
        .unwrap();
    assert!(server.state.ctx.is_online(&alice) && server.state.ctx.is_online(&bob));

    // Bob listens; Alice requests; Bob's stream carries the request with Alice's address and message.
    let mut bob_updates =
        subscribed(&server, &bob, bob_social.subscribe_to_friendship_updates()).await;
    let mut alice_updates = subscribed(
        &server,
        &alice,
        alice_social.subscribe_to_friendship_updates(),
    )
    .await;
    let response = alice_social
        .upsert_friendship(UpsertFriendshipPayload {
            action: Some(upsert_friendship_payload::Action::Request(
                upsert_friendship_payload::RequestPayload {
                    user: Some(User {
                        address: bob_addr.checksummed(),
                    }),
                    message: Some("hi bob".into()),
                },
            )),
        })
        .await;
    let Some(upsert_friendship_response::Response::Accepted(accepted)) = response.response else {
        panic!("{response:?}");
    };
    assert_eq!(accepted.friend.as_ref().unwrap().address, bob);
    assert_eq!(accepted.message.as_deref(), Some("hi bob"));

    let update = next_update(&mut bob_updates).await;
    let Some(friendship_update::Update::Request(request)) = update.update else {
        panic!("{update:?}")
    };
    assert_eq!(request.friend.as_ref().unwrap().address, alice);
    assert_eq!(request.message.as_deref(), Some("hi bob"));
    assert_eq!(request.id, accepted.id);

    // Both sides read the pending state; Bob accepts; Alice's stream carries the acceptance.
    let status = bob_social.get_friendship_status(&alice).await;
    assert!(
        matches!(status.response, Some(get_friendship_status_response::Response::Accepted(get_friendship_status_response::Ok { status, .. })) if status == FriendshipStatus::RequestReceived as i32),
        "{status:?}"
    );
    let status = alice_social.get_friendship_status(&bob).await;
    assert!(
        matches!(status.response, Some(get_friendship_status_response::Response::Accepted(get_friendship_status_response::Ok { status, .. })) if status == FriendshipStatus::RequestSent as i32),
        "{status:?}"
    );

    // Alice cannot accept her own request: the state machine refuses it.
    let response = alice_social
        .upsert_friendship(UpsertFriendshipPayload {
            action: Some(upsert_friendship_payload::Action::Accept(
                upsert_friendship_payload::AcceptPayload {
                    user: Some(User {
                        address: bob.clone(),
                    }),
                },
            )),
        })
        .await;
    assert!(
        matches!(
            response.response,
            Some(upsert_friendship_response::Response::InvalidFriendshipAction(_))
        ),
        "{response:?}"
    );

    let response = bob_social
        .upsert_friendship(UpsertFriendshipPayload {
            action: Some(upsert_friendship_payload::Action::Accept(
                upsert_friendship_payload::AcceptPayload {
                    user: Some(User {
                        address: alice.clone(),
                    }),
                },
            )),
        })
        .await;
    assert!(
        matches!(
            response.response,
            Some(upsert_friendship_response::Response::Accepted(_))
        ),
        "{response:?}"
    );
    let update = next_update(&mut alice_updates).await;
    assert!(
        matches!(update.update, Some(friendship_update::Update::Accept(friendship_update::AcceptResponse { user: Some(User { ref address }) })) if *address == bob),
        "{update:?}"
    );

    let friends = alice_social.get_friends().await;
    assert_eq!(
        friends
            .friends
            .iter()
            .map(|f| f.address.as_str())
            .collect::<Vec<_>>(),
        vec![bob.as_str()]
    );
    assert_eq!(friends.pagination_data.unwrap().total, 1);

    // Alice's connectivity stream opens with her online friend; when Bob
    // disconnects she is told he went offline.
    server
        .state
        .ctx
        .topology()
        .receive(&pulse_frame(vec![alice.clone(), bob.clone()]))
        .unwrap();
    let mut alice_connectivity = alice_social
        .subscribe_to_friend_connectivity_updates()
        .await;
    let update = next_update(&mut alice_connectivity).await;
    assert_eq!(update.friend.as_ref().unwrap().address, bob);
    assert_eq!(update.status, ConnectivityStatus::Online as i32);
    drop(_bob_client);
    drop(bob_social);
    assert!(
        server.state.ctx.is_online(&bob),
        "social socket closure does not remove Pulse presence"
    );
    server
        .state
        .ctx
        .topology()
        .receive(&pulse_frame(vec![alice.clone()]))
        .unwrap();
    // A complete Pulse omission, rather than socket closure, fans out OFFLINE.
    let update = loop {
        let update = next_update(&mut alice_connectivity).await;
        assert_eq!(update.friend.as_ref().unwrap().address, bob);
        if update.status != ConnectivityStatus::Online as i32 {
            break update;
        }
    };
    assert_eq!(update.status, ConnectivityStatus::Offline as i32);
    assert!(!server.state.ctx.is_online(&bob));

    cleanup_pair(&db, alice_addr.as_str(), bob_addr.as_str()).await;
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// The procedure decode refuses a malformed body with a 400 instead of
/// leaving the caller waiting (the codegen fix the old crate carried).
#[tokio::test]
async fn a_malformed_request_body_is_a_400_remote_error() {
    let mut def = super::drpc::service_module_definition::ServiceModuleDefinition::<Context>::new();
    let svc = Arc::new(SocialServiceImpl);
    let ctx = test_ctx(
        Db::new(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .unwrap(),
        ),
        Arc::new(crate::comms::gatekeeper::voice::rooms::NoVoiceRuntime),
    );
    {
        let service = Arc::clone(&svc);
        def.add_unary("GetFriendshipStatus", move |request, context| {
            let service = service.clone();
            Box::pin(async move {
                let request =
                    GetFriendshipStatusPayload::decode(request.as_slice()).map_err(|e| {
                        super::drpc::rpc_protocol::RemoteError {
                            message_identifier: 0,
                            error_code: 400,
                            error_message: e.to_string(),
                        }
                    })?;
                service
                    .get_friendship_status(request, context)
                    .await
                    .map(|r| r.encode_to_vec())
                    .map_err(Into::into)
            })
        });
    }
    let handler = match def.get_definitions().get("GetFriendshipStatus").unwrap() {
        super::drpc::service_module_definition::ProcedureDefinition::Unary(h) => h.clone(),
        _ => unreachable!(),
    };
    let err = handler(vec![0xff, 0xff, 0xff], procedure_ctx(&ctx, "0xabc", 9))
        .await
        .unwrap_err();
    assert_eq!(err.error_code, 400);
}
