//! The accepted private call's lifecycle through the real voice runtime
//! (`CommsState`, not `FakeVoiceRooms`, so accepting records participant
//! rows) with LiveKit's room service replaced by a local stub that records
//! `DeleteRoom`: ending deletes the room, an accepted call never expires and
//! keeps its parties busy, and only a party ends it. Each test states what
//! upstream (`decentraland/social-service-ea` at `fb3dcb8`) does.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::broadcast::Receiver;

use super::*;
use crate::comms::gatekeeper::{self as gatekeeper, CommsState};
use crate::comms::interfaces::test_support::{StubDirectory, StubPolicy, StubScenes};
use crate::http::Outbound;

struct Rig {
    db: Db,
    ctx: Context,
    runtime: Arc<CommsState>,
    deleted_rooms: Arc<StdMutex<Vec<String>>>,
    /// Set, the stub answers every `DeleteRoom` with a 500 (still recorded).
    refuse_deletes: Arc<AtomicBool>,
    livekit: tokio::task::JoinHandle<()>,
}

impl Rig {
    async fn new() -> Option<Self> {
        let pool = scratch_pool().await?;
        let deleted_rooms = Arc::new(StdMutex::new(Vec::new()));
        let refuse_deletes = Arc::new(AtomicBool::new(false));
        let (seen, refuse) = (deleted_rooms.clone(), refuse_deletes.clone());
        let stub = axum::Router::new().route(
            "/twirp/livekit.RoomService/DeleteRoom",
            axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let (seen, refuse) = (seen.clone(), refuse.clone());
                async move {
                    seen.lock()
                        .unwrap()
                        .push(body["room"].as_str().unwrap_or_default().to_string());
                    let status = if refuse.load(Ordering::SeqCst) {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::OK
                    };
                    (status, axum::Json(serde_json::json!({})))
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut settings = gatekeeper::Settings::for_tests();
        settings.livekit.api_url = format!("http://{}", listener.local_addr().unwrap());
        let livekit = tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });
        let runtime = Arc::new(CommsState::new(
            pool.clone(),
            Outbound::for_tests(),
            settings,
            None,
            None,
            Arc::new(StubPolicy::default()),
            Arc::new(StubScenes::default()),
            Arc::new(crate::comms::presence::Presence::new()),
            Arc::new(StubDirectory::default()),
        ));
        let db = Db::new(pool);
        let ctx = test_ctx(db.clone(), runtime.clone());
        Some(Self {
            db,
            ctx,
            runtime,
            deleted_rooms,
            refuse_deletes,
            livekit,
        })
    }

    /// A call from `caller` that `callee` accepted; its id.
    async fn accepted_call(&self, caller: &str, callee: &str) -> Uuid {
        let call_id = match SocialServiceImpl
            .start_private_voice_chat(
                StartPrivateVoiceChatPayload {
                    callee: Some(User {
                        address: callee.to_string(),
                    }),
                },
                procedure_ctx(&self.ctx, caller, 81),
            )
            .await
            .unwrap()
            .response
        {
            Some(start_private_voice_chat_response::Response::Ok(ok)) => ok.call_id,
            other => panic!("{other:?}"),
        };
        match SocialServiceImpl
            .accept_private_voice_chat(
                AcceptPrivateVoiceChatPayload {
                    call_id: call_id.clone(),
                },
                procedure_ctx(&self.ctx, callee, 82),
            )
            .await
            .unwrap()
            .response
        {
            Some(accept_private_voice_chat_response::Response::Ok(_)) => {}
            other => panic!("{other:?}"),
        }
        call_id.parse().unwrap()
    }

    async fn end(
        &self,
        me: &str,
        call_id: Uuid,
    ) -> Option<end_private_voice_chat_response::Response> {
        SocialServiceImpl
            .end_private_voice_chat(
                EndPrivateVoiceChatPayload {
                    call_id: call_id.to_string(),
                },
                procedure_ctx(&self.ctx, me, 84),
            )
            .await
            .unwrap()
            .response
    }

    fn room_deleted(&self, call_id: Uuid) -> bool {
        let room = crate::comms::rooms::private_voice_chat_room_name(&call_id);
        self.deleted_rooms.lock().unwrap().contains(&room)
    }

    async fn finish(self, wallets: &[&str]) {
        for w in wallets {
            sqlx::query(
                "DELETE FROM social.private_voice_chats WHERE caller_address = $1 OR callee_address = $1",
            )
            .bind(w)
            .execute(self.db.pool())
            .await
            .unwrap();
        }
        self.livekit.abort();
    }
}

fn statuses(rx: &mut Receiver<Arc<SocialEvent>>) -> Vec<i32> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|e| match e.as_ref() {
            SocialEvent::PrivateVoice(u) => Some(u.status),
            _ => None,
        })
        .collect()
}

/// Ending an accepted call, or the caller dropping its socket, deletes the
/// LiveKit room: the runtime's end path (`end_private_call`) deletes the row
/// while membership is still checkable, then the room. Upstream ends the
/// room through the gatekeeper, which deletes it (`logic/voice/voice.ts`,
/// `endPrivateVoiceChat`).
#[tokio::test]
async fn ending_an_accepted_call_deletes_its_room() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("vb1a"), addr("vb1b"));
    // Control: asked while the call row still exists, the runtime deletes the
    // room, so the stub sees a delete when one is sent.
    let control = rig.accepted_call(&caller, &callee).await;
    let who = crate::shared::identity::Address::parse(&caller).unwrap();
    let control_ended =
        gatekeeper::voice::end_private_voice_chat_by(&rig.runtime, &control.to_string(), &who)
            .await;
    let control_deleted = rig.room_deleted(control);
    let ended = rig.accepted_call(&caller, &callee).await;
    let answer = rig.end(&caller, ended).await;
    let dropped = rig.accepted_call(&caller, &callee).await;
    rig.ctx.end_private_voice_chat_on_disconnect(&caller).await;
    let (ended_deleted, dropped_deleted) = (rig.room_deleted(ended), rig.room_deleted(dropped));
    // Cleaned up before asserting: a failure leaves no call row to the sweep.
    rig.finish(&[&caller, &callee]).await;
    assert!(control_ended.is_ok(), "{control_ended:?}");
    assert!(control_deleted, "the stub saw no delete");
    assert!(
        matches!(
            answer,
            Some(end_private_voice_chat_response::Response::Ok(_))
        ),
        "{answer:?}"
    );
    assert!(ended_deleted, "End left the room of {ended} open");
    assert!(
        dropped_deleted,
        "the disconnect left the room of {dropped} open"
    );
}

/// An accepted call is never swept as EXPIRED, however long it lasts: the
/// sweep skips a call with participant rows (`expire_pending_private_calls`).
/// Upstream deletes the pending call when it is accepted
/// (`logic/voice/voice.ts`, `acceptPrivateVoiceChat`), so the sweep cannot
/// reach it. Backdating stands in for the TTL passing.
#[tokio::test]
async fn an_accepted_call_is_never_swept_as_expired() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("vb2a"), addr("vb2b"));
    let call = rig.accepted_call(&caller, &callee).await;
    backdate(&rig.db, call, EXPIRATION_MS + 1_000).await;
    let (mut to_caller, mut to_callee) = (
        rig.ctx.pubsub().subscribe(&caller),
        rig.ctx.pubsub().subscribe(&callee),
    );
    rig.ctx.expire_private_voice_chats(EXPIRATION_MS, 20).await;
    let expired = PrivateVoiceChatStatus::VoiceChatExpired as i32;
    let (caller_heard, callee_heard) = (statuses(&mut to_caller), statuses(&mut to_callee));
    rig.finish(&[&caller, &callee]).await;
    assert!(!caller_heard.contains(&expired), "{caller_heard:?}");
    assert!(!callee_heard.contains(&expired), "{callee_heard:?}");
}

/// An accepted call older than the pending TTL keeps both parties busy: a new
/// call to one of them neither reclaims it as expired nor starts, as
/// upstream's busy check intends for a party in a live call.
#[tokio::test]
async fn an_old_accepted_call_keeps_its_parties_busy() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee, other) = (addr("vb3a"), addr("vb3b"), addr("vb3c"));
    let call = rig.accepted_call(&caller, &callee).await;
    backdate(&rig.db, call, EXPIRATION_MS + 1_000).await;
    let outcome = rig
        .ctx
        .voice()
        .create_pending_private_call(&other, &callee, EXPIRATION_MS)
        .await;
    let kept = call_exists(&rig.db, call).await;
    rig.finish(&[&caller, &callee, &other]).await;
    let outcome = outcome.unwrap();
    assert!(outcome.started.is_none(), "{outcome:?}");
    assert!(outcome.expired.is_empty(), "{outcome:?}");
    assert!(kept, "the accepted call was reclaimed");
}

/// An accepted call older than the pending TTL does not arm the sweep: the
/// scheduler's earliest expiry (`private_voice_next_expiry_ms`) ignores it,
/// so a long call does not make every tick sweep.
#[tokio::test]
async fn an_old_accepted_call_does_not_arm_the_sweep() {
    const YEAR_MS: i64 = 365 * 24 * 3_600_000;
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("vb5a"), addr("vb5b"));
    let call = rig.accepted_call(&caller, &callee).await;
    // Older than any other row the shared scratch database can hold.
    backdate(&rig.db, call, 10 * YEAR_MS).await;
    let next = rig.db.private_voice_next_expiry_ms(EXPIRATION_MS).await;
    rig.ctx
        .sweep_private_voice_chats_if_due(EXPIRATION_MS, 20)
        .await;
    let due_soon = rig
        .db
        .private_voice_sweep_due(chrono::Utc::now().timestamp_millis() + 1_000);
    rig.finish(&[&caller, &callee]).await;
    let next = next.unwrap();
    assert!(next.is_none_or(|ms| ms > -YEAR_MS), "{next:?}");
    assert!(
        !due_soon,
        "the sweep is due again before the fallback interval"
    );
}

/// Only a party ends an accepted call (upstream `voice.ts:189-191`): anyone
/// else holding the id gets the unknown call's not-found, and the call, its
/// room and both parties are left alone.
#[tokio::test]
async fn a_stranger_cannot_end_an_accepted_call() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee, stranger) = (addr("vb4a"), addr("vb4b"), addr("vb4c"));
    let call = rig.accepted_call(&caller, &callee).await;
    let (mut to_caller, mut to_callee) = (
        rig.ctx.pubsub().subscribe(&caller),
        rig.ctx.pubsub().subscribe(&callee),
    );
    let refused = rig.end(&stranger, call).await;
    let (kept, deleted) = (call_exists(&rig.db, call).await, rig.room_deleted(call));
    let heard = statuses(&mut to_caller).len() + statuses(&mut to_callee).len();
    rig.finish(&[&caller, &callee, &stranger]).await;
    assert!(
        matches!(
            refused,
            Some(end_private_voice_chat_response::Response::NotFound(_))
        ),
        "{refused:?}"
    );
    assert!(kept, "a stranger's end deleted the call");
    assert!(!deleted, "a stranger's end deleted the room");
    assert_eq!(heard, 0, "a stranger's end told a party");
}

/// Regression guard: a failed LiveKit room delete is logged, never leaves the
/// call row behind, and both parties still hear ENDED.
#[tokio::test]
async fn a_failed_room_delete_still_removes_the_call() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("vb6a"), addr("vb6b"));
    let call = rig.accepted_call(&caller, &callee).await;
    rig.refuse_deletes.store(true, Ordering::SeqCst);
    let (mut to_caller, mut to_callee) = (
        rig.ctx.pubsub().subscribe(&caller),
        rig.ctx.pubsub().subscribe(&callee),
    );
    let answer = rig.end(&callee, call).await;
    let (kept, asked) = (call_exists(&rig.db, call).await, rig.room_deleted(call));
    let ended = PrivateVoiceChatStatus::VoiceChatEnded as i32;
    let heard = (statuses(&mut to_caller), statuses(&mut to_callee));
    rig.finish(&[&caller, &callee]).await;
    assert!(
        matches!(
            answer,
            Some(end_private_voice_chat_response::Response::Ok(_))
        ),
        "{answer:?}"
    );
    assert!(!kept, "the refused room delete left the call row");
    assert!(asked, "the room delete was never asked for");
    assert!(
        heard.0.contains(&ended) && heard.1.contains(&ended),
        "{heard:?}"
    );
}
