//! The state every procedure shares: the queries, the fan-out, the voice
//! runtime, profile enrichment, the connection identities and the presence
//! counters. Socket counters are transport bookkeeping; online state comes from
//! the complete Pulse topology shared by every serving replica.

use crate::comms::gatekeeper::voice::rooms::VoiceRooms;
use crate::rpc::db::Db;
use crate::rpc::limits::FriendshipMutationLimiter;
use crate::rpc::profiles::Profiles;
use crate::rpc::proto::v2::{
    CommunityMemberConnectivityUpdate, ConnectivityStatus, FriendConnectivityUpdate,
    PrivateVoiceChatStatus, PrivateVoiceChatUpdate, User,
};
use crate::rpc::pubsub::{PubSub, SocialEvent};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

pub use crate::rpc::limits::PairScope;

/// Idle cadence of the private-voice expiry DELETE when no call started here is pending.
const PRIVATE_VOICE_SWEEP_FALLBACK_MS: i64 = 30_000;

/// The knobs the procedures read; the process-wide `Settings` carries them
/// plus the socket's own (window, caps, admin token).
#[derive(Clone, Debug)]
pub struct VoiceSettings {
    pub private_voice_chat_expiration_ms: i64,
    pub private_voice_chat_expiration_batch_size: i64,
}

impl Default for VoiceSettings {
    fn default() -> Self {
        Self {
            private_voice_chat_expiration_ms: 60_000,
            private_voice_chat_expiration_batch_size: 20,
        }
    }
}

pub struct ContextInner {
    pub voice_settings: VoiceSettings,
    pub db: Db,
    pub pubsub: PubSub,
    pub voice: Arc<dyn VoiceRooms>,
    pub profiles: Profiles,
    friendship_limiter: FriendshipMutationLimiter,
    identities: DashMap<u32, String>,
    presence: DashMap<String, u32>,
    topology: std::sync::OnceLock<Arc<crate::comms::topology::Topology>>,
    connectivity_operations: tokio::sync::Semaphore,
    kill_handles: DashMap<u32, Arc<Notify>>,
    connections: AtomicUsize,
    /// `SOCIAL_WRITES_ENABLED` (`social::writes`): false refuses every
    /// mutating procedure with `SocialError::WritesDisabled`.
    writes_enabled: AtomicBool,
}

/// Resume only after a completed bounded recipient page. A status reversal resets it.
#[derive(Clone, Default)]
pub(crate) struct ConnectivityCursor {
    pub online: Option<bool>,
    pub friend_after: String,
    pub member_after: Option<(uuid::Uuid, String)>,
    pub friends_done: bool,
}

#[derive(Clone)]
pub struct Context(Arc<ContextInner>);

impl Context {
    pub fn new(
        voice_settings: VoiceSettings,
        db: Db,
        profiles: Profiles,
        voice: Arc<dyn VoiceRooms>,
    ) -> Self {
        Self(Arc::new(ContextInner {
            voice_settings,
            db,
            pubsub: PubSub::new(),
            voice,
            profiles,
            friendship_limiter: FriendshipMutationLimiter::new(),
            identities: DashMap::new(),
            presence: DashMap::new(),
            topology: std::sync::OnceLock::new(),
            connectivity_operations: tokio::sync::Semaphore::new(4),
            kill_handles: DashMap::new(),
            connections: AtomicUsize::new(0),
            writes_enabled: AtomicBool::new(true),
        }))
    }

    pub fn set_writes_enabled(&self, enabled: bool) {
        self.0.writes_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn writes_enabled(&self) -> bool {
        self.0.writes_enabled.load(Ordering::Relaxed)
    }

    pub fn voice_settings(&self) -> &VoiceSettings {
        &self.0.voice_settings
    }
    pub fn db(&self) -> &Db {
        &self.0.db
    }
    pub fn pubsub(&self) -> &PubSub {
        &self.0.pubsub
    }
    pub fn voice(&self) -> &dyn VoiceRooms {
        self.0.voice.as_ref()
    }
    pub fn profiles(&self) -> &Profiles {
        &self.0.profiles
    }
    pub fn friendship_limiter(&self) -> &FriendshipMutationLimiter {
        &self.0.friendship_limiter
    }

    /// The identity of a transport is fixed at the handshake; a procedure
    /// call on a transport with none is unauthenticated.
    pub fn register_identity(&self, transport_id: u32, address: String) {
        self.0
            .identities
            .insert(transport_id, address.to_lowercase());
    }

    pub fn connection_opened(&self) {
        self.0.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.0.connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn live_connections(&self) -> usize {
        self.0.connections.load(Ordering::Relaxed)
    }

    pub fn register_kill_handle(&self, transport_id: u32, kill: Arc<Notify>) {
        self.0.kill_handles.insert(transport_id, kill);
    }

    pub fn forget_identity(&self, transport_id: u32) {
        self.0.identities.remove(&transport_id);
        self.0.kill_handles.remove(&transport_id);
    }

    pub fn identity(&self, transport_id: u32) -> Option<String> {
        self.0.identities.get(&transport_id).map(|r| r.clone())
    }

    pub fn presence_snapshot(&self) -> Vec<(String, u32)> {
        self.0
            .presence
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub fn online_count(&self) -> usize {
        self.topology().snapshot().wallets.len()
    }

    /// Kills every socket of `address`; returns how many were told.
    pub fn disconnect_address(&self, address: &str) -> usize {
        let addr = address.to_lowercase();
        let ids: Vec<u32> = self
            .0
            .identities
            .iter()
            .filter(|e| *e.value() == addr)
            .map(|e| *e.key())
            .collect();
        let mut kicked = 0;
        for id in ids {
            if let Some(handle) = self.0.kill_handles.get(&id) {
                handle.notify_waiters();
                kicked += 1;
            }
        }
        kicked
    }

    /// True when this socket took the address from offline to online.
    pub fn mark_online(&self, address: &str) -> bool {
        let mut entry = self.0.presence.entry(address.to_lowercase()).or_insert(0);
        *entry += 1;
        *entry == 1
    }

    /// True when this socket was the address's last.
    pub fn mark_offline(&self, address: &str) -> bool {
        let addr = address.to_lowercase();
        let mut became_offline = false;
        if let Some(mut entry) = self.0.presence.get_mut(&addr) {
            if *entry > 0 {
                *entry -= 1;
            }
            became_offline = *entry == 0;
        }
        if became_offline {
            self.0.presence.remove(&addr);
        }
        became_offline
    }

    pub fn topology(&self) -> &Arc<crate::comms::topology::Topology> {
        self.0
            .topology
            .get_or_init(|| Arc::new(crate::comms::topology::Topology::default()))
    }

    pub(crate) fn set_topology(&self, topology: Arc<crate::comms::topology::Topology>) {
        assert!(
            self.0.topology.set(topology).is_ok(),
            "topology must be injected before serving"
        );
    }

    pub fn is_online(&self, address: &str) -> bool {
        self.topology().snapshot().is_online(address)
    }

    pub(crate) fn friend_delivery_guard(
        &self,
        recipient: String,
        actor: String,
        online: bool,
        generation: u64,
    ) -> crate::rpc::drpc::stream_protocol::DeliveryGuard {
        self.delivery_guard(recipient, actor, online, generation, None)
    }

    pub(crate) fn community_delivery_guard(
        &self,
        recipient: String,
        actor: String,
        online: bool,
        generation: u64,
        community: uuid::Uuid,
    ) -> crate::rpc::drpc::stream_protocol::DeliveryGuard {
        self.delivery_guard(recipient, actor, online, generation, Some(community))
    }

    fn delivery_guard(
        &self,
        recipient: String,
        actor: String,
        online: bool,
        generation: u64,
        community: Option<uuid::Uuid>,
    ) -> crate::rpc::drpc::stream_protocol::DeliveryGuard {
        let ctx = self.clone();
        Arc::new(move || {
            let ctx = ctx.clone();
            let recipient = recipient.clone();
            let actor = actor.clone();
            Box::pin(async move {
                let Ok(_permit) = ctx.0.connectivity_operations.acquire().await else {
                    return Err(());
                };
                let allowed = match community {
                    Some(community) => {
                        ctx.db()
                            .connectivity_member_allowed(&actor, &recipient, community)
                            .await
                    }
                    None => {
                        ctx.db()
                            .connectivity_friend_allowed(&actor, &recipient)
                            .await
                    }
                };
                let allowed = allowed.map_err(|_| ())?;
                Ok(allowed
                    && ctx
                        .topology()
                        .queued_status_is_current(generation, &actor, online))
            })
        })
    }

    pub(crate) fn event_delivery_guard(
        &self,
        recipient: &str,
        event: &SocialEvent,
    ) -> Option<crate::rpc::drpc::stream_protocol::DeliveryGuard> {
        match event {
            SocialEvent::TopologyCommunityMember(update, generation) => Some(self.delivery_guard(
                recipient.to_string(),
                update.member.as_ref()?.address.clone(),
                update.status == ConnectivityStatus::Online as i32,
                *generation,
                Some(update.community_id.parse().ok()?),
            )),
            SocialEvent::CommunityMember(update) => Some(self.delivery_guard(
                recipient.to_string(),
                update.member.as_ref()?.address.clone(),
                update.status == ConnectivityStatus::Online as i32,
                self.topology().snapshot().generation,
                Some(update.community_id.parse().ok()?),
            )),
            _ => None,
        }
    }

    /// Administrative broadcasts also cannot publish a status contrary to current topology.
    pub async fn fan_connectivity(&self, address: &str, status: ConnectivityStatus) {
        let snapshot = self.topology().snapshot();
        let _ = tokio::time::timeout(crate::comms::topology::TOPOLOGY_RECEIPT_TTL, async {
            let mut cursor = ConnectivityCursor::default();
            loop {
                if !self.topology().deliver_if_current(
                    snapshot.generation,
                    address,
                    status == ConnectivityStatus::Online,
                    || {},
                ) {
                    break;
                }
                match self
                    .fan_topology_page(
                        address,
                        status == ConnectivityStatus::Online,
                        snapshot.generation,
                        cursor,
                    )
                    .await
                {
                    Ok((_, true)) | Err(_) => break,
                    Ok((next, false)) => {
                        cursor = next;
                    }
                }
            }
        })
        .await;
    }

    pub(crate) async fn fan_topology_page(
        &self,
        address: &str,
        online: bool,
        generation: u64,
        mut cursor: ConnectivityCursor,
    ) -> Result<(ConnectivityCursor, bool), crate::rpc::db::DbError> {
        let _permit = self
            .0
            .connectivity_operations
            .acquire()
            .await
            .expect("connectivity budget stays open");
        let address = address.to_lowercase();
        let status = if online {
            ConnectivityStatus::Online
        } else {
            ConnectivityStatus::Offline
        } as i32;
        if cursor.online != Some(online) {
            cursor = ConnectivityCursor {
                online: Some(online),
                ..Default::default()
            };
        }
        if !self
            .topology()
            .deliver_if_current(generation, &address, online, || {})
        {
            return Ok((cursor, false));
        }
        if !cursor.friends_done {
            let friends = self
                .0
                .db
                .connectivity_friends_page(&address, &cursor.friend_after, true)
                .await?;
            if !friends.is_empty() {
                let profile = crate::rpc::profiles::connectivity_profile(
                    self.0.profiles.friend_profile(&address).await,
                );
                for friend in &friends {
                    if !self
                        .topology()
                        .deliver_if_current(generation, &address, online, || {
                            self.0.pubsub.publish(
                                friend,
                                SocialEvent::TopologyFriendConnectivity(
                                    FriendConnectivityUpdate {
                                        friend: Some(profile.clone()),
                                        status,
                                    },
                                    generation,
                                ),
                            );
                        })
                    {
                        return Ok((cursor, false));
                    }
                }
                cursor.friend_after = friends.last().unwrap().clone();
            }
            cursor.friends_done = friends.len() < 256;
            return Ok((cursor, false));
        }
        let members = self
            .0
            .db
            .connectivity_members_page(&address, cursor.member_after.clone())
            .await?;
        for (community_id, member) in &members {
            if !self
                .topology()
                .deliver_if_current(generation, &address, online, || {
                    self.0.pubsub.publish(
                        member,
                        SocialEvent::TopologyCommunityMember(
                            CommunityMemberConnectivityUpdate {
                                community_id: community_id.to_string(),
                                member: Some(User {
                                    address: address.clone(),
                                }),
                                status,
                            },
                            generation,
                        ),
                    );
                })
            {
                return Ok((cursor, false));
            }
        }
        if let Some(last) = members.last() {
            cursor.member_after = Some(last.clone());
        }
        let completed = members.len() < 256;
        Ok((cursor, completed))
    }

    /// Upstream #479: without this only the periodic sweep clears a dropped
    /// user's call, so the other party keeps ringing until the TTL elapses.
    /// The ENDED notification is symmetric, so the still-online party is told
    /// regardless of which side dropped.
    /// An accepted call ends through the voice runtime (row, then room);
    /// otherwise the address's latest pending call is deleted.
    pub async fn end_private_voice_chat_on_disconnect(&self, address: &str) {
        let voice = &self.0.voice;
        let ended = match voice
            .end_private_voice_chat(None, &address.to_lowercase())
            .await
        {
            Ok(None) => {
                let pending = crate::comms::gatekeeper::voice::db::PendingPrivateCall::Address(
                    address.to_string(),
                );
                voice.finish_pending_private_call(pending).await
            }
            accepted => accepted,
        };
        let chat = match ended {
            Ok(Some(c)) => c,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, %address, "disconnect voice-cleanup failed");
                return;
            }
        };
        self.publish_private_voice(&chat, PrivateVoiceChatStatus::VoiceChatEnded);
    }

    /// The same update to both parties of `chat`.
    pub fn publish_private_voice(
        &self,
        chat: &crate::comms::gatekeeper::voice::db::PrivateVoiceChatRow,
        status: PrivateVoiceChatStatus,
    ) {
        let update = PrivateVoiceChatUpdate {
            call_id: chat.id.to_string(),
            status: status as i32,
            caller: Some(User {
                address: chat.caller_address.clone(),
            }),
            callee: Some(User {
                address: chat.callee_address.clone(),
            }),
            credentials: None,
        };
        self.0.pubsub.publish(
            &chat.caller_address,
            SocialEvent::PrivateVoice(update.clone()),
        );
        self.0
            .pubsub
            .publish(&chat.callee_address, SocialEvent::PrivateVoice(update));
    }

    /// The periodic tick. The DELETE runs only once a call can have expired
    /// (the earliest due instant is noted on local starts and re-read from the
    /// table after each sweep) or every [`PRIVATE_VOICE_SWEEP_FALLBACK_MS`]
    /// for rows another process wrote, so a call is still reclaimed within
    /// one tick of its TTL.
    pub async fn sweep_private_voice_chats_if_due(
        &self,
        expiration_ms: i64,
        batch_size: i64,
    ) -> usize {
        let now = chrono::Utc::now().timestamp_millis();
        if !self.0.db.private_voice_sweep_due(now) {
            return 0;
        }
        self.0.db.claim_private_voice_sweep();
        let total = self
            .expire_private_voice_chats(expiration_ms, batch_size)
            .await;
        let next = match self.0.db.private_voice_next_expiry_ms(expiration_ms).await {
            Ok(Some(remaining)) => now + remaining.max(0),
            Ok(None) => now + PRIVATE_VOICE_SWEEP_FALLBACK_MS,
            Err(_) => now,
        };
        self.0
            .db
            .note_private_voice_due(next.min(now + PRIVATE_VOICE_SWEEP_FALLBACK_MS));
        total
    }

    pub async fn expire_private_voice_chats(&self, expiration_ms: i64, batch_size: i64) -> usize {
        let mut total = 0usize;
        loop {
            let expired = match self
                .0
                .voice
                .expire_pending_private_calls(expiration_ms, batch_size)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, "private voice chat expiry sweep failed");
                    break;
                }
            };
            if expired.is_empty() {
                break;
            }
            for chat in &expired {
                self.publish_private_voice(chat, PrivateVoiceChatStatus::VoiceChatExpired);
            }
            total += expired.len();
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn presence_counts_sockets_per_address() {
        let ctx = Context::new(
            VoiceSettings::default(),
            Db::new(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://localhost/unused")
                    .unwrap(),
            ),
            Profiles::new(None, Default::default()),
            Arc::new(crate::comms::gatekeeper::voice::rooms::NoVoiceRuntime),
        );
        assert!(ctx.mark_online("0xAbC"));
        assert!(
            !ctx.mark_online("0xabc"),
            "a second socket does not re-announce"
        );
        assert!(
            !ctx.is_online("0xABC"),
            "sockets do not establish Pulse presence"
        );
        assert!(!ctx.mark_offline("0xabc"));
        assert!(
            ctx.mark_offline("0xabc"),
            "the last socket announces offline"
        );
        assert!(!ctx.is_online("0xabc"));
        assert!(!ctx.mark_offline("0xnever"));
        assert_eq!(ctx.online_count(), 0);

        ctx.register_identity(7, "0xAbC".into());
        assert_eq!(ctx.identity(7).as_deref(), Some("0xabc"));
        let kill = Arc::new(Notify::new());
        ctx.register_kill_handle(7, kill);
        assert_eq!(ctx.disconnect_address("0xABC"), 1);
        ctx.forget_identity(7);
        assert_eq!(ctx.disconnect_address("0xabc"), 0);
        assert!(ctx.identity(7).is_none());
    }
}
