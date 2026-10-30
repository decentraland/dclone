//! Private voice calls through the RPC handlers, the cases of upstream's
//! `logic/voice-logic.spec.ts` and the private-voice `rpc-server/services`
//! specs (`decentraland/social-service-ea` at `fb3dcb8`) Catalyst implements
//! but the call-flow test does not reach: the caller's own privacy, a
//! self-call in another case, the not-found answers, whose grant each party
//! receives, the incoming request, a disconnect and the expiry sweep.

use tokio::sync::broadcast::Receiver;

use super::*;

struct Rig {
    db: Db,
    ctx: Context,
}

impl Rig {
    async fn new() -> Option<Self> {
        let pool = scratch_pool().await?;
        let db = Db::new(pool.clone());
        let ctx = test_ctx(db.clone(), Arc::new(FakeVoiceRooms::new(pool)));
        Some(Self { db, ctx })
    }

    async fn start(&self, me: &str, callee: &str) -> start_private_voice_chat_response::Response {
        SocialServiceImpl
            .start_private_voice_chat(
                StartPrivateVoiceChatPayload {
                    callee: Some(User {
                        address: callee.to_string(),
                    }),
                },
                procedure_ctx(&self.ctx, me, 51),
            )
            .await
            .unwrap()
            .response
            .unwrap()
    }

    async fn call(&self, caller: &str, callee: &str) -> String {
        match self.start(caller, callee).await {
            start_private_voice_chat_response::Response::Ok(ok) => ok.call_id,
            other => panic!("{other:?}"),
        }
    }

    async fn accept(
        &self,
        me: &str,
        call_id: &str,
    ) -> accept_private_voice_chat_response::Response {
        SocialServiceImpl
            .accept_private_voice_chat(
                AcceptPrivateVoiceChatPayload {
                    call_id: call_id.to_string(),
                },
                procedure_ctx(&self.ctx, me, 52),
            )
            .await
            .unwrap()
            .response
            .unwrap()
    }

    async fn reject(
        &self,
        me: &str,
        call_id: &str,
    ) -> reject_private_voice_chat_response::Response {
        SocialServiceImpl
            .reject_private_voice_chat(
                RejectPrivateVoiceChatPayload {
                    call_id: call_id.to_string(),
                },
                procedure_ctx(&self.ctx, me, 53),
            )
            .await
            .unwrap()
            .response
            .unwrap()
    }

    async fn end(&self, me: &str, call_id: &str) -> end_private_voice_chat_response::Response {
        SocialServiceImpl
            .end_private_voice_chat(
                EndPrivateVoiceChatPayload {
                    call_id: call_id.to_string(),
                },
                procedure_ctx(&self.ctx, me, 54),
            )
            .await
            .unwrap()
            .response
            .unwrap()
    }

    async fn incoming(
        &self,
        me: &str,
    ) -> get_incoming_private_voice_chat_request_response::Response {
        SocialServiceImpl
            .get_incoming_private_voice_chat_request(procedure_ctx(&self.ctx, me, 55))
            .await
            .unwrap()
            .response
            .unwrap()
    }

    async fn calls_between(&self, a: &str, b: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM social.private_voice_chats \
             WHERE (caller_address = $1 AND callee_address = $2) OR (caller_address = $2 AND callee_address = $1)",
        )
        .bind(a)
        .bind(b)
        .fetch_one(self.db.pool())
        .await
        .unwrap()
    }

    async fn forget(&self, wallets: &[&str]) {
        for w in wallets {
            sqlx::query("DELETE FROM social.private_voice_chats WHERE caller_address = $1 OR callee_address = $1")
                .bind(w)
                .execute(self.db.pool())
                .await
                .unwrap();
            sqlx::query("DELETE FROM social.user_settings WHERE address = $1")
                .bind(w)
                .execute(self.db.pool())
                .await
                .unwrap();
        }
    }
}

fn voice_updates(rx: &mut Receiver<Arc<SocialEvent>>) -> Vec<PrivateVoiceChatUpdate> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|e| match e.as_ref() {
            SocialEvent::PrivateVoice(u) => Some(u.clone()),
            _ => None,
        })
        .collect()
}

fn checksummed(a: &str) -> String {
    a.to_uppercase().replacen("0X", "0x", 1)
}

/// The caller's own `only_friends` refuses a stranger and admits a friend; a
/// self-call is refused in any letter case; a refused start writes no call.
#[tokio::test]
async fn a_start_follows_the_callers_privacy_and_refuses_yourself() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("pvc1"), addr("pvc2"));
    assert!(matches!(
        rig.start(&caller, &checksummed(&caller)).await,
        start_private_voice_chat_response::Response::ForbiddenError(_)
    ));
    rig.db
        .upsert_social_settings(&caller, Some("only_friends"), None, None)
        .await
        .unwrap();
    match rig.start(&caller, &callee).await {
        start_private_voice_chat_response::Response::ForbiddenError(e) => assert_eq!(
            e.message.as_deref(),
            Some(
                "The callee or the caller are not accepting voice calls from users that are not friends"
            )
        ),
        other => panic!("{other:?}"),
    }
    rig.db.block_user(&callee, &caller).await.unwrap();
    assert!(matches!(
        rig.start(&caller, &callee).await,
        start_private_voice_chat_response::Response::ForbiddenError(_)
    ));
    assert_eq!(
        rig.calls_between(&caller, &callee).await,
        0,
        "a refused start writes no call"
    );
    rig.db.unblock_user(&callee, &caller).await.unwrap();
    rig.db
        .apply_friendship_action(&caller, &callee, "accept", true, None, None)
        .await
        .unwrap();
    rig.call(&caller, &callee).await;
    rig.forget(&[&caller, &callee]).await;
    cleanup_pair(&rig.db, &caller, &callee).await;
}

/// Unknown ids answer not found (accept, reject, end); a second reject of
/// the same call too; a malformed id is an invalid request on accept and
/// reject.
#[tokio::test]
async fn an_unknown_call_is_not_found() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("pvn1"), addr("pvn2"));
    let unknown = Uuid::new_v4().to_string();
    assert!(matches!(
        rig.accept(&callee, &unknown).await,
        accept_private_voice_chat_response::Response::NotFound(_)
    ));
    assert!(matches!(
        rig.reject(&callee, &unknown).await,
        reject_private_voice_chat_response::Response::NotFound(_)
    ));
    assert!(matches!(
        rig.end(&callee, &unknown).await,
        end_private_voice_chat_response::Response::NotFound(_)
    ));
    assert!(matches!(
        rig.accept(&callee, "not-a-uuid").await,
        accept_private_voice_chat_response::Response::InvalidRequest(_)
    ));
    assert!(matches!(
        rig.reject(&callee, "not-a-uuid").await,
        reject_private_voice_chat_response::Response::InvalidRequest(_)
    ));

    let call_id = rig.call(&caller, &callee).await;
    assert!(matches!(
        rig.reject(&callee, &call_id).await,
        reject_private_voice_chat_response::Response::Ok(_)
    ));
    assert!(matches!(
        rig.reject(&callee, &call_id).await,
        reject_private_voice_chat_response::Response::NotFound(_)
    ));
    rig.forget(&[&caller, &callee]).await;
}

/// Only the callee rejects (upstream `voice.ts:156-157`) and only a party
/// ends (`voice.ts:189-191`): anyone else holding the id gets the unknown
/// call's not-found, the call stays and nobody is told. The callee rejects and
/// either party ends as before.
#[tokio::test]
async fn only_the_callee_rejects_and_only_a_party_ends() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee, stranger) = (addr("pvp1"), addr("pvp2"), addr("pvp3"));
    let call_id = rig.call(&caller, &callee).await;
    let (mut to_caller, mut to_callee) = (
        rig.ctx.pubsub().subscribe(&caller),
        rig.ctx.pubsub().subscribe(&callee),
    );
    let refused_rejects = [
        rig.reject(&stranger, &call_id).await,
        rig.reject(&caller, &call_id).await,
    ];
    let refused_end = rig.end(&stranger, &call_id).await;
    let left = rig.calls_between(&caller, &callee).await;
    let heard = voice_updates(&mut to_caller).len() + voice_updates(&mut to_callee).len();
    let rejected = rig.reject(&callee, &call_id).await;
    let mut ended = Vec::new();
    for party in [&caller, &callee] {
        let id = rig.call(&caller, &callee).await;
        ended.push(rig.end(party, &id).await);
    }
    let after = rig.calls_between(&caller, &callee).await;
    rig.forget(&[&caller, &callee, &stranger]).await;
    for refused in &refused_rejects {
        assert!(
            matches!(
                refused,
                reject_private_voice_chat_response::Response::NotFound(_)
            ),
            "{refused:?}"
        );
    }
    assert!(
        matches!(
            refused_end,
            end_private_voice_chat_response::Response::NotFound(_)
        ),
        "{refused_end:?}"
    );
    assert_eq!(left, 1, "a refused reject or end deleted the call");
    assert_eq!(heard, 0, "a refused reject or end told a party");
    assert!(
        matches!(
            rejected,
            reject_private_voice_chat_response::Response::Ok(_)
        ),
        "{rejected:?}"
    );
    for answer in &ended {
        assert!(
            matches!(answer, end_private_voice_chat_response::Response::Ok(_)),
            "{answer:?}"
        );
    }
    assert_eq!(after, 0);
}

/// Each party receives its own grant: the callee in the response, the caller
/// in ACCEPTED; the incoming request names the caller until it is answered.
#[tokio::test]
async fn each_party_gets_its_own_grant_and_the_callee_sees_the_incoming_call() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("pva1"), addr("pva2"));
    assert!(matches!(
        rig.incoming(&callee).await,
        get_incoming_private_voice_chat_request_response::Response::NotFound(_)
    ));
    let call_id = rig.call(&caller, &callee).await;
    match rig.incoming(&callee).await {
        get_incoming_private_voice_chat_request_response::Response::Ok(ok) => {
            assert_eq!(ok.call_id, call_id);
            assert_eq!(ok.caller.unwrap().address, caller);
        }
        other => panic!("{other:?}"),
    }
    let mut to_caller = rig.ctx.pubsub().subscribe(&caller);
    match rig.accept(&callee, &call_id).await {
        accept_private_voice_chat_response::Response::Ok(ok) => {
            assert_eq!(ok.call_id, call_id);
            let url = ok.credentials.unwrap().connection_url;
            assert!(url.ends_with(&format!("for={callee}")), "{url}");
        }
        other => panic!("{other:?}"),
    }
    let updates = voice_updates(&mut to_caller);
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].status,
        PrivateVoiceChatStatus::VoiceChatAccepted as i32
    );
    let url = &updates[0].credentials.as_ref().unwrap().connection_url;
    assert!(url.ends_with(&format!("for={caller}")), "{url}");
    rig.forget(&[&caller, &callee]).await;
}

/// The caller dropping its last socket ends the pending call and tells the callee.
#[tokio::test]
async fn a_caller_that_disconnects_ends_the_call_for_the_callee() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("pvd1"), addr("pvd2"));
    rig.ctx.end_private_voice_chat_on_disconnect(&caller).await;
    let call_id = rig.call(&caller, &callee).await;
    let mut to_callee = rig.ctx.pubsub().subscribe(&callee);
    rig.ctx.end_private_voice_chat_on_disconnect(&caller).await;
    let updates = voice_updates(&mut to_callee);
    assert!(
        updates
            .iter()
            .any(|u| u.call_id == call_id
                && u.status == PrivateVoiceChatStatus::VoiceChatEnded as i32),
        "{updates:?}"
    );
    assert_eq!(rig.calls_between(&caller, &callee).await, 0);
    rig.forget(&[&caller, &callee]).await;
}

/// The sweep pages through every expired call, one batch at a time, and
/// tells both parties of each.
#[tokio::test]
async fn the_expiry_sweep_pages_through_and_tells_both_parties() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let voice_db = VoiceDb::new(rig.db.pool().clone(), Default::default());
    let mut pairs = Vec::new();
    for i in 0..3 {
        let (caller, callee) = (addr(&format!("pve{i}a")), addr(&format!("pve{i}b")));
        let id = voice_db
            .seed_pending_private_call(&caller, &callee)
            .await
            .unwrap();
        backdate(&rig.db, id, EXPIRATION_MS + 3_600_000).await;
        let rx = (
            rig.ctx.pubsub().subscribe(&caller),
            rig.ctx.pubsub().subscribe(&callee),
        );
        pairs.push((id, caller, callee, rx));
    }
    let swept = rig.ctx.expire_private_voice_chats(EXPIRATION_MS, 1).await;
    assert!(swept >= 3, "{swept}");
    for (id, caller, callee, (mut to_caller, mut to_callee)) in pairs {
        for rx in [&mut to_caller, &mut to_callee] {
            let updates = voice_updates(rx);
            assert!(
                updates.iter().any(|u| u.call_id == id.to_string()
                    && u.status == PrivateVoiceChatStatus::VoiceChatExpired as i32),
                "{updates:?}"
            );
        }
        assert!(!call_exists(&rig.db, id).await);
        rig.forget(&[&caller, &callee]).await;
    }
}

/// Upstream's start handler (community rooms) and `isUserInAVoiceChat`
/// (another private room): either party already in a room is a conflict
/// that names it, and no call is written.
#[tokio::test]
async fn either_party_already_in_a_voice_room_is_a_conflict_naming_it() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let pool = rig.db.pool().clone();
    let (caller, callee, owner, third) = (addr("pvr1"), addr("pvr2"), addr("pvr3"), addr("pvr4"));
    let conflict = |answer: start_private_voice_chat_response::Response| match answer {
        start_private_voice_chat_response::Response::ConflictingError(e) => {
            e.message.unwrap_or_default()
        }
        other => panic!("{other:?}"),
    };

    let community = seed_community(&pool, true, &owner, "owner").await;
    for (who, message) in [
        (
            &caller,
            "Cannot start private voice chat while in a community voice chat",
        ),
        (
            &callee,
            "Cannot start private voice chat: the callee is in a community voice chat",
        ),
    ] {
        sqlx::query("INSERT INTO social.community_voice_chat_users (community_id, address, status) VALUES ($1, $2, 'connected')")
            .bind(community)
            .bind(who)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(conflict(rig.start(&caller, &callee).await), message);
        sqlx::query("DELETE FROM social.community_voice_chat_users WHERE community_id = $1")
            .bind(community)
            .execute(&pool)
            .await
            .unwrap();
    }
    drop_community(&pool, community).await;

    let voice_db = VoiceDb::new(pool.clone(), Default::default());
    for who in [&callee, &caller] {
        // An older call of `who`, past the busy window, whose room they are in.
        let id = voice_db
            .seed_pending_private_call(who, &third)
            .await
            .unwrap();
        backdate(&rig.db, id, EXPIRATION_MS + 3_600_000).await;
        sqlx::query("INSERT INTO social.private_voice_chat_participants (call_id, address, status) VALUES ($1, $2, 'connected')")
            .bind(id)
            .bind(who)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            conflict(rig.start(&caller, &callee).await),
            format!("One of the users is already in a voice chat: {who}")
        );
        sqlx::query("DELETE FROM social.private_voice_chats WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(rig.calls_between(&caller, &callee).await, 0);
    rig.forget(&[&caller, &callee, &third]).await;
}

/// The start's refusal messages. Recorded difference (RPC.md, private voice,
/// "start-private-voice-chat (7)"): upstream answers a self-call and a blocked
/// pair with the not-friends message (`VoiceChatNotAllowedError`,
/// `src/logic/voice/voice.ts:36-45`, `src/logic/voice/errors.ts:1-5`);
/// Catalyst names each refusal.
#[tokio::test]
async fn a_refused_start_says_why() {
    let Some(rig) = Rig::new().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let (caller, callee) = (addr("pvm1"), addr("pvm2"));
    let message = |answer: start_private_voice_chat_response::Response| match answer {
        start_private_voice_chat_response::Response::InvalidRequest(e) => e.message,
        start_private_voice_chat_response::Response::ForbiddenError(e) => e.message,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        message(rig.start(&caller, " ").await).as_deref(),
        Some("Callee address is missing in the request payload")
    );
    assert_eq!(
        message(rig.start(&caller, &caller).await).as_deref(),
        Some("Cannot start a private voice chat with yourself")
    );
    rig.db.block_user(&caller, &callee).await.unwrap();
    assert_eq!(
        message(rig.start(&caller, &callee).await).as_deref(),
        Some(
            "This action is not allowed because either you blocked this user or this user blocked you"
        )
    );
    cleanup_pair(&rig.db, &caller, &callee).await;
    rig.forget(&[&caller, &callee]).await;
}
