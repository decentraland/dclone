//! Test voice transport backed by comms-owned call persistence.

/// A runtime that mints a recognisable URL per grant and records every call;
/// the tests drive the voice RPCs through it.
#[cfg(test)]
pub mod fake {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use uuid::Uuid;

    use crate::comms::gatekeeper::voice::rooms::{
        CommunityVoiceAction, VoiceRooms, VoiceUnavailable,
    };

    pub struct FakeVoiceRooms {
        db: crate::comms::gatekeeper::voice::db::VoiceDb,
        pub calls: Mutex<Vec<String>>,
    }

    impl FakeVoiceRooms {
        pub fn new(pool: sqlx::PgPool) -> Self {
            Self {
                db: crate::comms::gatekeeper::voice::db::VoiceDb::new(pool, Default::default()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn note(&self, line: String) {
            self.calls.lock().push(line);
        }
    }

    #[async_trait]
    impl VoiceRooms for FakeVoiceRooms {
        async fn create_pending_private_call(
            &self,
            caller: &str,
            callee: &str,
            expiration_ms: i64,
        ) -> Result<crate::comms::gatekeeper::voice::db::StartOutcome, VoiceUnavailable> {
            Ok(self
                .db
                .create_pending_private_call(caller, callee, expiration_ms)
                .await?)
        }
        async fn finish_pending_private_call(
            &self,
            call: crate::comms::gatekeeper::voice::db::PendingPrivateCall,
        ) -> Result<
            Option<crate::comms::gatekeeper::voice::db::PrivateVoiceChatRow>,
            VoiceUnavailable,
        > {
            Ok(self.db.finish_pending_private_call(call).await?)
        }
        async fn expire_pending_private_calls(
            &self,
            expiration_ms: i64,
            limit: i64,
        ) -> Result<Vec<crate::comms::gatekeeper::voice::db::PrivateVoiceChatRow>, VoiceUnavailable>
        {
            Ok(self
                .db
                .expire_pending_private_calls(expiration_ms, limit)
                .await?)
        }

        async fn private_voice_credentials(
            &self,
            call_id: Uuid,
            callee: &str,
            caller: &str,
        ) -> Result<HashMap<String, String>, VoiceUnavailable> {
            self.note(format!("private-credentials {call_id} {callee} {caller}"));
            Ok([callee, caller]
                .into_iter()
                .map(|a| {
                    (
                        a.to_string(),
                        format!("wss://livekit.test/voice-chat-private-{call_id}?for={a}"),
                    )
                })
                .collect())
        }

        /// Records the ask; a fake call has no participant rows, so it is
        /// never accepted and ends as a pending call.
        async fn end_private_voice_chat(
            &self,
            call_id: Option<Uuid>,
            address: &str,
        ) -> Result<
            Option<crate::comms::gatekeeper::voice::db::PrivateVoiceChatRow>,
            VoiceUnavailable,
        > {
            let call = call_id.map_or_else(|| "latest".to_string(), |id| id.to_string());
            self.note(format!("end-private {call} {address}"));
            Ok(None)
        }

        async fn community_voice_credentials(
            &self,
            community_id: Uuid,
            address: &str,
            role: &str,
            action: CommunityVoiceAction,
        ) -> Result<String, VoiceUnavailable> {
            self.note(format!(
                "community-credentials {community_id} {address} {role} {}",
                action.as_str()
            ));
            Ok(format!(
                "wss://livekit.test/voice-chat-community-{community_id}?for={address}"
            ))
        }

        async fn end_community_voice_chat(
            &self,
            community_id: Uuid,
            address: &str,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!("end-community {community_id} {address}"));
            Ok(())
        }

        async fn request_to_speak(
            &self,
            community_id: Uuid,
            address: &str,
            raising_hand: bool,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!(
                "request-to-speak {community_id} {address} {raising_hand}"
            ));
            Ok(())
        }

        async fn reject_speak_request(
            &self,
            community_id: Uuid,
            address: &str,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!("reject-speak-request {community_id} {address}"));
            Ok(())
        }

        async fn set_speaker(
            &self,
            community_id: Uuid,
            address: &str,
            promote: bool,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!("set-speaker {community_id} {address} {promote}"));
            Ok(())
        }

        async fn kick_player(
            &self,
            community_id: Uuid,
            address: &str,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!("kick {community_id} {address}"));
            Ok(())
        }

        async fn mute_speaker(
            &self,
            community_id: Uuid,
            address: &str,
            muted: bool,
        ) -> Result<(), VoiceUnavailable> {
            self.note(format!("mute {community_id} {address} {muted}"));
            Ok(())
        }
    }
}
