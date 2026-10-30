//! Community voice through the RPC handlers, the cases of upstream's
//! `logic/community-voice.spec.ts` and the community-voice
//! `rpc-server/services` specs (`decentraland/social-service-ea` at
//! `fb3dcb8`) Catalyst implements but the start/join test does not reach:
//! who may end, mute, raise a hand, promote, demote, reject and kick, in a
//! public and a private community, and what reaches the runtime.

use super::*;

/// The variant a response holds, by name (`Ok`, `ForbiddenError`, ...).
fn variant<T: std::fmt::Debug>(response: &Option<T>) -> String {
    let debug = format!("{response:?}");
    let inner = debug.strip_prefix("Some(").unwrap_or(&debug);
    inner.split(['(', ' ', '{']).next().unwrap().to_string()
}

struct Rig {
    pool: PgPool,
    ctx: Context,
    voice: Arc<FakeVoiceRooms>,
    community: Uuid,
    cid: String,
    owner: String,
    moderator: String,
    member: String,
    banned: String,
    guest: String,
}

impl Rig {
    /// A community (`private` or not) with an owner, a moderator, a member,
    /// a banned wallet and a guest with no role.
    async fn new(private: bool) -> Option<Self> {
        let pool = scratch_pool().await?;
        let voice = Arc::new(FakeVoiceRooms::new(pool.clone()));
        let ctx = test_ctx(Db::new(pool.clone()), voice.clone());
        let (owner, moderator, member, banned, guest) = (
            addr("cvo"),
            addr("cvm"),
            addr("cvb"),
            addr("cvx"),
            addr("cvg"),
        );
        let community = seed_community(&pool, true, &owner, "owner").await;
        sqlx::query("UPDATE social.communities SET private = $2 WHERE id = $1")
            .bind(community)
            .bind(private)
            .execute(&pool)
            .await
            .unwrap();
        for (who, role) in [(&moderator, "moderator"), (&member, "member")] {
            sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, $3)")
                .bind(community)
                .bind(who)
                .bind(role)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO social.community_bans (community_id, banned_address, banned_by) VALUES ($1, $2, $3)")
            .bind(community)
            .bind(&banned)
            .bind(&owner)
            .execute(&pool)
            .await
            .unwrap();
        Some(Self {
            pool,
            ctx,
            voice,
            cid: community.to_string(),
            community,
            owner,
            moderator,
            member,
            banned,
            guest,
        })
    }

    fn as_(&self, me: &str) -> ProcedureContext<Context> {
        procedure_ctx(&self.ctx, me, 61)
    }

    fn runtime_saw(&self, line: &str) -> bool {
        self.voice.calls.lock().iter().any(|c| c == line)
    }

    async fn mute(&self, me: &str, target: &str, muted: bool) -> String {
        let r = SocialServiceImpl
            .mute_speaker_from_community_voice_chat(
                MuteSpeakerFromCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    user_address: target.to_string(),
                    muted,
                },
                self.as_(me),
            )
            .await
            .unwrap();
        if let Some(mute_speaker_from_community_voice_chat_response::Response::Ok(ok)) = &r.response
        {
            assert_eq!(ok.muted, muted);
        }
        variant(&r.response)
    }

    async fn hand(&self, me: &str, raising: bool) -> String {
        let r = SocialServiceImpl
            .request_to_speak_in_community_voice_chat(
                RequestToSpeakInCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    is_raising_hand: raising,
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn promote(&self, me: &str, target: &str) -> String {
        let r = SocialServiceImpl
            .promote_speaker_in_community_voice_chat(
                PromoteSpeakerInCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    user_address: target.to_string(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn demote(&self, me: &str, target: &str) -> String {
        let r = SocialServiceImpl
            .demote_speaker_in_community_voice_chat(
                DemoteSpeakerInCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    user_address: target.to_string(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn reject(&self, me: &str, target: &str) -> String {
        let r = SocialServiceImpl
            .reject_speak_request_in_community_voice_chat(
                RejectSpeakRequestInCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    user_address: target.to_string(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn kick(&self, me: &str, target: &str) -> String {
        let r = SocialServiceImpl
            .kick_player_from_community_voice_chat(
                KickPlayerFromCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                    user_address: target.to_string(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn start(&self, me: &str) -> String {
        let r = SocialServiceImpl
            .start_community_voice_chat(
                StartCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn end(&self, me: &str) -> String {
        let r = SocialServiceImpl
            .end_community_voice_chat(
                EndCommunityVoiceChatPayload {
                    community_id: self.cid.clone(),
                },
                self.as_(me),
            )
            .await
            .unwrap();
        variant(&r.response)
    }

    async fn drop(self) {
        drop_community(&self.pool, self.community).await;
    }
}

fn checksummed(a: &str) -> String {
    a.to_uppercase().replacen("0X", "0x", 1)
}

fn voice_updates(
    rx: &mut tokio::sync::broadcast::Receiver<Arc<SocialEvent>>,
) -> Vec<CommunityVoiceChatUpdate> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|e| match e.as_ref() {
            SocialEvent::CommunityVoice(u) => Some(u.clone()),
            _ => None,
        })
        .collect()
}

/// A moderator starts like the owner; the owner or a moderator ends and the
/// members hear ENDED; a member can do neither. As upstream's `updates.spec.ts`
/// (`src/logic/updates.ts:335-337`, `:396`), the starter is not told of its own
/// STARTED and a wallet with no role hears neither. Recorded difference: the
/// second end answers ok with no room left to end, and neither start nor end
/// reads whether a room is live (RPC.md, community voice, "start-, end-, join-, raise a hand, self-unmute": upstream answers not found without a live room (`src/logic/community-voice/validation.ts:41-42`, `community-voice.ts:309-312`); Catalyst has no room-live check.)
#[tokio::test]
async fn moderators_start_and_end_and_members_cannot() {
    let Some(rig) = Rig::new(false).await else {
        return;
    };
    let mut to_member = rig.ctx.pubsub().subscribe(&rig.member);
    let mut to_starter = rig.ctx.pubsub().subscribe(&rig.moderator);
    let mut to_guest = rig.ctx.pubsub().subscribe(&rig.guest);
    assert_eq!(rig.start(&rig.moderator).await, "Ok");
    assert!(voice_updates(&mut to_starter).is_empty());
    let started = voice_updates(&mut to_member);
    assert!(
        started.iter().any(|u| u.community_id == rig.cid
            && u.status == CommunityVoiceChatStatus::CommunityVoiceChatStarted as i32),
        "{started:?}"
    );
    assert_eq!(rig.end(&rig.member).await, "ForbiddenError");
    assert_eq!(rig.end(&rig.guest).await, "ForbiddenError");
    for ender in [&rig.moderator, &rig.owner] {
        assert_eq!(rig.end(ender).await, "Ok", "{ender}");
        let ended = voice_updates(&mut to_member);
        assert!(
            ended.iter().any(|u| u.community_id == rig.cid
                && u.status == CommunityVoiceChatStatus::CommunityVoiceChatEnded as i32
                && u.ended_at.is_some()),
            "{ended:?}"
        );
        assert!(rig.runtime_saw(&format!("end-community {} {ender}", rig.community)));
    }
    assert!(voice_updates(&mut to_guest).is_empty());
    rig.drop().await;
}

/// Muting another speaker needs a moderator who is not targeting the owner;
/// the refused ones never reach the runtime. Muting yourself is free,
/// unmuting yourself needs to be in the room's audience.
#[tokio::test]
async fn who_may_mute_whom() {
    let Some(rig) = Rig::new(false).await else {
        return;
    };
    let (owner, moderator, member, guest, banned) = (
        &rig.owner,
        &rig.moderator,
        &rig.member,
        &rig.guest,
        &rig.banned,
    );
    let c = rig.community;
    for (me, target, muted) in [
        (moderator, member, true),
        (moderator, member, false),
        (owner, moderator, true),
        (moderator, moderator, true),
        (moderator, banned, true),
    ] {
        assert_eq!(rig.mute(me, target, muted).await, "Ok", "{me} -> {target}");
        assert!(rig.runtime_saw(&format!("mute {c} {target} {muted}")));
    }
    // a checksummed target reaches the runtime lowercased
    assert_eq!(rig.mute(moderator, &checksummed(member), true).await, "Ok");
    assert!(rig.runtime_saw(&format!("mute {c} {member} true")));
    for (me, target) in [(moderator, owner), (member, guest), (guest, member)] {
        rig.voice.calls.lock().clear();
        assert_eq!(
            rig.mute(me, target, true).await,
            "ForbiddenError",
            "{me} -> {target}"
        );
        assert!(rig.voice.calls.lock().is_empty(), "{me} -> {target}");
    }
    // yourself: muting is free; unmuting in a public community too, but not banned
    for me in [member, guest, banned] {
        assert_eq!(rig.mute(me, me, true).await, "Ok", "{me}");
    }
    // Recorded difference: no room is live here, which upstream answers not
    // found (RPC.md, community voice, "start-, end-, join-, raise a hand, self-unmute": upstream answers not found without a live room (`src/logic/community-voice/validation.ts:41-42`, `community-voice.ts:309-312`); Catalyst has no room-live check.)
    assert_eq!(rig.mute(guest, guest, false).await, "Ok");
    assert_eq!(rig.mute(banned, banned, false).await, "ForbiddenError");
    rig.drop().await;

    // a private community admits its members only
    let Some(rig) = Rig::new(true).await else {
        return;
    };
    assert_eq!(rig.mute(&rig.member, &rig.member, false).await, "Ok");
    assert_eq!(
        rig.mute(&rig.guest, &rig.guest, false).await,
        "ForbiddenError"
    );
    assert_eq!(rig.mute(&rig.guest, &rig.guest, true).await, "Ok");
    rig.drop().await;
}

/// Raising a hand needs to be in the audience; lowering it never does.
#[tokio::test]
async fn who_may_raise_a_hand() {
    let Some(rig) = Rig::new(false).await else {
        return;
    };
    let c = rig.community;
    // Recorded difference: no room is live here, which upstream answers not
    // found (RPC.md, community voice, "start-, end-, join-, raise a hand, self-unmute": upstream answers not found without a live room (`src/logic/community-voice/validation.ts:41-42`, `community-voice.ts:309-312`); Catalyst has no room-live check.)
    assert_eq!(rig.hand(&rig.guest, true).await, "Ok");
    assert!(rig.runtime_saw(&format!("request-to-speak {c} {} true", rig.guest)));
    rig.voice.calls.lock().clear();
    assert_eq!(rig.hand(&rig.banned, true).await, "ForbiddenError");
    assert!(rig.voice.calls.lock().is_empty());
    assert_eq!(rig.hand(&rig.banned, false).await, "Ok");
    rig.drop().await;

    let Some(rig) = Rig::new(true).await else {
        return;
    };
    assert_eq!(rig.hand(&rig.member, true).await, "Ok");
    rig.voice.calls.lock().clear();
    assert_eq!(rig.hand(&rig.guest, true).await, "ForbiddenError");
    assert!(rig.voice.calls.lock().is_empty());
    assert_eq!(rig.hand(&rig.guest, false).await, "Ok");
    rig.drop().await;
}

/// Promote, demote, reject and kick: a moderator or the owner acts on the
/// audience, never a moderator on the owner, never a member or a guest; in
/// a private community the target must be a member.
#[tokio::test]
async fn who_may_moderate_the_room() {
    let Some(rig) = Rig::new(false).await else {
        return;
    };
    let (owner, moderator, member, guest) = (&rig.owner, &rig.moderator, &rig.member, &rig.guest);
    let c = rig.community;
    assert_eq!(rig.promote(moderator, member).await, "Ok");
    assert!(rig.runtime_saw(&format!("set-speaker {c} {member} true")));
    assert_eq!(rig.promote(moderator, guest).await, "Ok");
    assert_eq!(rig.promote(owner, moderator).await, "Ok");
    assert_eq!(rig.demote(moderator, member).await, "Ok");
    assert!(rig.runtime_saw(&format!("set-speaker {c} {member} false")));
    assert_eq!(rig.demote(owner, moderator).await, "Ok");
    assert_eq!(rig.reject(moderator, guest).await, "Ok");
    assert!(rig.runtime_saw(&format!("reject-speak-request {c} {guest}")));
    assert_eq!(rig.reject(owner, moderator).await, "Ok");
    for kicked in [member, guest, &rig.banned] {
        assert_eq!(rig.kick(moderator, kicked).await, "Ok", "{kicked}");
        assert!(rig.runtime_saw(&format!("kick {c} {kicked}")));
    }
    for (me, target) in [(moderator, owner), (member, guest), (guest, member)] {
        assert_eq!(
            rig.promote(me, target).await,
            "ForbiddenError",
            "promote {me} {target}"
        );
        assert_eq!(
            rig.demote(me, target).await,
            "ForbiddenError",
            "demote {me} {target}"
        );
        assert_eq!(
            rig.reject(me, target).await,
            "ForbiddenError",
            "reject {me} {target}"
        );
    }
    for me in [member, guest] {
        assert_eq!(rig.kick(me, owner).await, "ForbiddenError", "kick {me}");
    }
    rig.drop().await;

    let Some(rig) = Rig::new(true).await else {
        return;
    };
    assert_eq!(rig.promote(&rig.moderator, &rig.member).await, "Ok");
    assert_eq!(
        rig.promote(&rig.moderator, &rig.guest).await,
        "ForbiddenError"
    );
    assert_eq!(
        rig.demote(&rig.moderator, &rig.guest).await,
        "ForbiddenError"
    );
    assert_eq!(
        rig.reject(&rig.moderator, &rig.guest).await,
        "ForbiddenError"
    );
    rig.drop().await;
}
