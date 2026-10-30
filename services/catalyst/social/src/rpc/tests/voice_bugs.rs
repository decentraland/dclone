//! Two suspected defects of the private voice call, reproduced through the
//! real voice runtime (`CommsState`, not `FakeVoiceRooms`) with LiveKit's
//! room service replaced by a local stub that records `DeleteRoom`. Both
//! tests fail today and are ignored until the fix; each states what upstream
//! (`decentraland/social-service-ea` at `fb3dcb8`) does.

use std::sync::Mutex as StdMutex;

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
    livekit: tokio::task::JoinHandle<()>,
}

impl Rig {
    async fn new() -> Option<Self> {
        let pool = scratch_pool().await?;
        let deleted_rooms = Arc::new(StdMutex::new(Vec::new()));
        let seen = deleted_rooms.clone();
        let stub = axum::Router::new().route(
            "/twirp/livekit.RoomService/DeleteRoom",
            axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let seen = seen.clone();
                async move {
                    seen.lock()
                        .unwrap()
                        .push(body["room"].as_str().unwrap_or_default().to_string());
                    axum::Json(serde_json::json!({}))
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

/// Bug: ending an accepted call, or the caller dropping its socket, never
/// deletes the LiveKit room, so both parties stay connected to each other.
/// The RPC deletes the call row first (`finish_pending_private_call`), then
/// asks the runtime to end it; `end_private_voice_chat_by` finds no row
/// (`delete_private_call_if_member` → `RoomDoesNotExist`, a 404), the
/// runtime maps that 404 to success (`voice/rooms.rs`), and `delete_room`
/// is never reached (`voice/mod.rs`, `end_private_voice_chat_by`).
/// Upstream ends the room through the gatekeeper, which deletes it
/// (`logic/voice/voice.ts`, `endPrivateVoiceChat`).
#[tokio::test]
#[ignore = "bug: ending or dropping an accepted private call never deletes its LiveKit room"]
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
    let answer = SocialServiceImpl
        .end_private_voice_chat(
            EndPrivateVoiceChatPayload {
                call_id: ended.to_string(),
            },
            procedure_ctx(&rig.ctx, &caller, 83),
        )
        .await
        .unwrap()
        .response;
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

/// Bug: an accepted call is swept as EXPIRED once the pending-call TTL
/// passes, mid-conversation. Accepting re-inserts the row with a fresh
/// `created_at` (`create_private_call`, `ON CONFLICT ... created_at`) and
/// the sweep deletes by `created_at` alone (`expire_pending_private_calls`,
/// `voice/db.rs`). Upstream deletes the pending call when it is accepted
/// (`logic/voice/voice.ts`, `acceptPrivateVoiceChat`), so the sweep cannot
/// reach it. Backdating stands in for the TTL passing.
#[tokio::test]
#[ignore = "bug: an accepted private call is swept as EXPIRED after the pending TTL"]
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
