//! The social RPC's voice seam, [`VoiceRooms`] (call persistence, LiveKit
//! room grants and live-room actions: end, kick, mute, speaker changes), and its implementation over the gatekeeper in this
//! process: the old social RPC asked the comms gatekeeper over HTTP
//! (`COMMS_GATEKEEPER_URL` + the service token) for every room grant and room
//! action; here the same logic functions are a local call on `CommsState`,
//! so `Arc<CommsState>` is the runtime `social::rpc::State::new` takes. The
//! RPC has already decided who may do what (membership, role, ban); this
//! side re-checks only what the gatekeeper always re-checked (the platform
//! ban) and mints. Pending call persistence also belongs here; social keeps
//! privacy/membership decisions and signalling. Production always attaches
//! `CommsState`; without LiveKit credentials it reports not configured.
//! `NoVoiceRuntime` is available only for tests and `test-support`.
//!
//! `is_configured` is the LiveKit credential state: on the dev placeholders
//! the call RPCs answer their explicit "not configured" error as they did
//! when the gatekeeper answered 503. Identifiers reach the runtime already
//! validated (a uuid, a lowercase address): the old client re-checked them
//! before interpolating into a privileged path (upstream #446), which a
//! local call makes unnecessary.

use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use crate::{Address, CommunityId};

use super::db::{PendingPrivateCall, PrivateVoiceChatRow, StartOutcome};

use super::{
    CommunityVoiceChatBody, community_voice_credentials_for, destroy_community_voice_chat,
    end_private_call, get_private_voice_chat_room_credentials, kick_from_community_room,
    merge_metadata, patch, require_livekit, set_speaker,
};
use crate::gatekeeper::CommsState;
use crate::gatekeeper::errors::ApiError;

#[derive(Debug, Error)]
pub enum VoiceUnavailable {
    #[error("voice runtime not configured (no LiveKit adapter attached)")]
    NotConfigured,
    #[error("voice runtime failed: {0}")]
    Failed(String),
    #[error(transparent)]
    Persistence(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommunityVoiceAction {
    Create,
    Join,
}

impl CommunityVoiceAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Join => "join",
        }
    }
}

#[async_trait]
pub trait VoiceRooms: Send + Sync {
    /// Atomically reclaim stale calls and create a pending call unless either party is busy.
    async fn create_pending_private_call(
        &self,
        caller: &str,
        callee: &str,
        expiration_ms: i64,
    ) -> Result<StartOutcome, VoiceUnavailable>;

    /// Commit deletion before social signals or attempts media teardown.
    async fn finish_pending_private_call(
        &self,
        call: PendingPrivateCall,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable>;

    /// One oldest-first bounded batch; social publishes the expired event.
    async fn expire_pending_private_calls(
        &self,
        expiration_ms: i64,
        limit: i64,
    ) -> Result<Vec<PrivateVoiceChatRow>, VoiceUnavailable>;

    /// False for the degraded runtime: the call RPCs refuse up front instead
    /// of ringing a party nobody can talk to.
    fn is_configured(&self) -> bool {
        true
    }

    /// Grants for the room `voice-chat-private-<call_id>`, keyed by lowercase
    /// address; the adapter records both participants as `not_connected`.
    async fn private_voice_credentials(
        &self,
        call_id: Uuid,
        callee: &str,
        caller: &str,
    ) -> Result<HashMap<String, String>, VoiceUnavailable>;

    /// Ends the accepted call `call_id` (or else the latest one `address` is
    /// in) on `address`'s behalf, `address` being one of its participants:
    /// the row, then the room. `None` when there is no such accepted call.
    async fn end_private_voice_chat(
        &self,
        call_id: Option<Uuid>,
        address: &str,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable>;

    /// A grant for `voice-chat-community-<community_id>`; `role` is the stored
    /// community role text, `Create` opens the room, `Join` enters it.
    async fn community_voice_credentials(
        &self,
        community_id: Uuid,
        address: &str,
        role: &str,
        action: CommunityVoiceAction,
    ) -> Result<String, VoiceUnavailable>;

    async fn end_community_voice_chat(
        &self,
        community_id: Uuid,
        address: &str,
    ) -> Result<(), VoiceUnavailable>;
    async fn request_to_speak(
        &self,
        community_id: Uuid,
        address: &str,
        raising_hand: bool,
    ) -> Result<(), VoiceUnavailable>;
    async fn reject_speak_request(
        &self,
        community_id: Uuid,
        address: &str,
    ) -> Result<(), VoiceUnavailable>;
    async fn set_speaker(
        &self,
        community_id: Uuid,
        address: &str,
        promote: bool,
    ) -> Result<(), VoiceUnavailable>;
    async fn kick_player(&self, community_id: Uuid, address: &str) -> Result<(), VoiceUnavailable>;
    async fn mute_speaker(
        &self,
        community_id: Uuid,
        address: &str,
        muted: bool,
    ) -> Result<(), VoiceUnavailable>;
}

/// A test double with no attached LiveKit adapter.
#[cfg(any(test, feature = "test-support"))]
pub struct NoVoiceRuntime;

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl VoiceRooms for NoVoiceRuntime {
    async fn create_pending_private_call(
        &self,
        _: &str,
        _: &str,
        _: i64,
    ) -> Result<StartOutcome, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }
    async fn finish_pending_private_call(
        &self,
        _: PendingPrivateCall,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }
    async fn expire_pending_private_calls(
        &self,
        _: i64,
        _: i64,
    ) -> Result<Vec<PrivateVoiceChatRow>, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    fn is_configured(&self) -> bool {
        false
    }

    async fn private_voice_credentials(
        &self,
        _call_id: Uuid,
        _callee: &str,
        _caller: &str,
    ) -> Result<HashMap<String, String>, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn end_private_voice_chat(
        &self,
        _call_id: Option<Uuid>,
        _address: &str,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn community_voice_credentials(
        &self,
        _community_id: Uuid,
        _address: &str,
        _role: &str,
        _action: CommunityVoiceAction,
    ) -> Result<String, VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn end_community_voice_chat(
        &self,
        _community_id: Uuid,
        _address: &str,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn request_to_speak(
        &self,
        _community_id: Uuid,
        _address: &str,
        _raising_hand: bool,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn reject_speak_request(
        &self,
        _community_id: Uuid,
        _address: &str,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn set_speaker(
        &self,
        _community_id: Uuid,
        _address: &str,
        _promote: bool,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn kick_player(
        &self,
        _community_id: Uuid,
        _address: &str,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }

    async fn mute_speaker(
        &self,
        _community_id: Uuid,
        _address: &str,
        _muted: bool,
    ) -> Result<(), VoiceUnavailable> {
        Err(VoiceUnavailable::NotConfigured)
    }
}

fn unavailable(e: ApiError) -> VoiceUnavailable {
    if e.code == 503 {
        VoiceUnavailable::NotConfigured
    } else {
        VoiceUnavailable::Failed(e.to_string())
    }
}

fn address(raw: &str) -> Result<Address, VoiceUnavailable> {
    Address::parse(raw).map_err(|e| VoiceUnavailable::Failed(format!("{raw}: {e}")))
}

#[async_trait]
impl VoiceRooms for CommsState {
    async fn create_pending_private_call(
        &self,
        caller: &str,
        callee: &str,
        expiration_ms: i64,
    ) -> Result<StartOutcome, VoiceUnavailable> {
        Ok(self
            .voice_db
            .create_pending_private_call(caller, callee, expiration_ms)
            .await?)
    }
    async fn finish_pending_private_call(
        &self,
        call: PendingPrivateCall,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable> {
        Ok(self.voice_db.finish_pending_private_call(call).await?)
    }
    async fn expire_pending_private_calls(
        &self,
        expiration_ms: i64,
        limit: i64,
    ) -> Result<Vec<PrivateVoiceChatRow>, VoiceUnavailable> {
        Ok(self
            .voice_db
            .expire_pending_private_calls(expiration_ms, limit)
            .await?)
    }

    fn is_configured(&self) -> bool {
        self.livekit.configured
    }

    async fn private_voice_credentials(
        &self,
        call_id: Uuid,
        callee: &str,
        caller: &str,
    ) -> Result<HashMap<String, String>, VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        let participants = [address(caller)?, address(callee)?];
        let grants =
            get_private_voice_chat_room_credentials(self, &call_id.to_string(), &participants)
                .await
                .map_err(unavailable)?;
        Ok(grants
            .into_iter()
            .filter_map(|(addr, v)| {
                v.get("connection_url")
                    .and_then(|u| u.as_str())
                    .map(|u| (addr, u.to_string()))
            })
            .collect())
    }

    async fn end_private_voice_chat(
        &self,
        call_id: Option<Uuid>,
        address_raw: &str,
    ) -> Result<Option<PrivateVoiceChatRow>, VoiceUnavailable> {
        let who = address(address_raw)?;
        Ok(end_private_call(self, call_id, &who)
            .await
            .map_err(unavailable)?
            .map(|ended| ended.call))
    }

    async fn community_voice_credentials(
        &self,
        community_id: Uuid,
        address_raw: &str,
        role: &str,
        action: CommunityVoiceAction,
    ) -> Result<String, VoiceUnavailable> {
        let body = CommunityVoiceChatBody {
            community_id: community_id.to_string(),
            user_address: address_raw.to_string(),
            user_role: Some(role.to_string()),
            action: Some(action.as_str().to_string()),
            profile_data: None,
        };
        community_voice_credentials_for(self, body)
            .await
            .map_err(unavailable)
    }

    async fn end_community_voice_chat(
        &self,
        community_id: Uuid,
        _address: &str,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        destroy_community_voice_chat(
            self,
            CommunityId::from_uuid(community_id),
            "livekit community voice room",
        )
        .await
        .map_err(unavailable)
    }

    async fn request_to_speak(
        &self,
        community_id: Uuid,
        address_raw: &str,
        raising_hand: bool,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        let room =
            crate::rooms::community_voice_chat_room_name(&CommunityId::from_uuid(community_id));
        merge_metadata(
            self,
            &room,
            &address_raw.to_lowercase(),
            patch(&[("isRequestingToSpeak", serde_json::json!(raising_hand))]),
        )
        .await
        .map_err(unavailable)
    }

    async fn reject_speak_request(
        &self,
        community_id: Uuid,
        address_raw: &str,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        let room =
            crate::rooms::community_voice_chat_room_name(&CommunityId::from_uuid(community_id));
        merge_metadata(
            self,
            &room,
            &address_raw.to_lowercase(),
            patch(&[("isRequestingToSpeak", serde_json::json!(false))]),
        )
        .await
        .map_err(unavailable)
    }

    async fn set_speaker(
        &self,
        community_id: Uuid,
        address_raw: &str,
        promote: bool,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        set_speaker(
            self,
            CommunityId::from_uuid(community_id),
            &address_raw.to_lowercase(),
            promote,
        )
        .await
        .map_err(unavailable)
    }

    async fn kick_player(
        &self,
        community_id: Uuid,
        address_raw: &str,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        kick_from_community_room(
            self,
            CommunityId::from_uuid(community_id),
            &address_raw.to_lowercase(),
        )
        .await
        .map_err(unavailable)
    }

    async fn mute_speaker(
        &self,
        community_id: Uuid,
        address_raw: &str,
        muted: bool,
    ) -> Result<(), VoiceUnavailable> {
        require_livekit(self).map_err(unavailable)?;
        let room =
            crate::rooms::community_voice_chat_room_name(&CommunityId::from_uuid(community_id));
        merge_metadata(
            self,
            &room,
            &address_raw.to_lowercase(),
            patch(&[("muted", serde_json::json!(muted))]),
        )
        .await
        .map_err(unavailable)
    }
}
