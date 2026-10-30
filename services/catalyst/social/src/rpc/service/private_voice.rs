use super::SocialServiceImpl;
use super::helpers::{
    SocialError, internal_err, invalid_req, normalize, start_voice_conflict, start_voice_forbidden,
    start_voice_internal, start_voice_invalid,
};
use crate::comms::gatekeeper::voice::db::PendingPrivateCall;
use crate::rpc::context::Context;
use crate::rpc::drpc::service_module_definition::ProcedureContext;
use crate::rpc::proto::errors::{ForbiddenError, InternalServerError, NotFoundError};
use crate::rpc::proto::v2::*;
use crate::rpc::pubsub::SocialEvent;
use uuid::Uuid;

const VOICE_RUNTIME_UNAVAILABLE: &str = "voice runtime unavailable";

impl SocialServiceImpl {
    /// The gates run in the old precedence, all from one read: self-call,
    /// block, either party in a community room, the privacy settings
    /// (`all` on both sides or friends), busy, either party in a private
    /// room; then the locked insert. With no voice runtime the call is refused
    /// up front rather than rung with nothing to connect to.
    pub(crate) async fn start_private_voice_chat(
        &self,
        request: StartPrivateVoiceChatPayload,
        context: ProcedureContext<Context>,
    ) -> Result<StartPrivateVoiceChatResponse, SocialError> {
        let me = Self::caller(&context)?;
        let callee = match request.callee.as_ref() {
            Some(u) if !u.address.trim().is_empty() => normalize(&u.address),
            _ => {
                return Ok(start_voice_invalid(
                    "Callee address is missing in the request payload",
                ));
            }
        };
        let ctx = &context.server_context;
        if callee == me {
            return Ok(start_voice_forbidden(
                "Cannot start a private voice chat with yourself",
            ));
        }
        if !ctx.voice().is_configured() {
            return Ok(start_voice_internal(VOICE_RUNTIME_UNAVAILABLE));
        }
        let expiration_ms = ctx.voice_settings().private_voice_chat_expiration_ms;
        let pre = ctx
            .db()
            .private_voice_preflight(&me, &callee, expiration_ms)
            .await?;
        if pre.blocked {
            return Ok(start_voice_forbidden(
                "This action is not allowed because either you blocked this user or this user blocked you",
            ));
        }
        if pre.caller_in_community_voice {
            return Ok(start_voice_conflict(
                "Cannot start private voice chat while in a community voice chat",
            ));
        }
        if pre.callee_in_community_voice {
            return Ok(start_voice_conflict(
                "Cannot start private voice chat: the callee is in a community voice chat",
            ));
        }
        if (pre.callee_privacy != "all" || pre.caller_privacy != "all")
            && !pre.friends.unwrap_or(false)
        {
            return Ok(start_voice_forbidden(
                "The callee or the caller are not accepting voice calls from users that are not friends",
            ));
        }
        if pre.busy {
            return Ok(start_voice_conflict(
                "One of the users is busy calling someone else",
            ));
        }
        if pre.callee_in_voice {
            return Ok(start_voice_conflict(format!(
                "One of the users is already in a voice chat: {callee}"
            )));
        }
        if pre.caller_in_voice {
            return Ok(start_voice_conflict(format!(
                "One of the users is already in a voice chat: {me}"
            )));
        }

        let outcome = match ctx
            .voice()
            .create_pending_private_call(&me, &callee, expiration_ms)
            .await
        {
            Ok(o) => o,
            Err(e) => return Ok(start_voice_internal(e.to_string())),
        };
        if outcome.started.is_some() {
            ctx.db()
                .note_private_voice_due(chrono::Utc::now().timestamp_millis() + expiration_ms);
        }
        for expired in &outcome.expired {
            ctx.publish_private_voice(expired, PrivateVoiceChatStatus::VoiceChatExpired);
        }
        match outcome.started {
            Some(id) => {
                ctx.pubsub().publish(
                    &callee,
                    SocialEvent::PrivateVoice(PrivateVoiceChatUpdate {
                        call_id: id.to_string(),
                        status: PrivateVoiceChatStatus::VoiceChatRequested as i32,
                        caller: Some(User {
                            address: me.clone(),
                        }),
                        callee: Some(User {
                            address: callee.clone(),
                        }),
                        credentials: None,
                    }),
                );
                Ok(StartPrivateVoiceChatResponse {
                    response: Some(start_private_voice_chat_response::Response::Ok(
                        start_private_voice_chat_response::Ok {
                            call_id: id.to_string(),
                        },
                    )),
                })
            }
            None => Ok(start_voice_conflict(
                "One of the users is busy calling someone else",
            )),
        }
    }

    /// Only the callee accepts; the grants come from the voice runtime (a
    /// missing one is an explicit internal error, never an empty URL) and the
    /// caller receives ACCEPTED with their own grant.
    pub(crate) async fn accept_private_voice_chat(
        &self,
        request: AcceptPrivateVoiceChatPayload,
        context: ProcedureContext<Context>,
    ) -> Result<AcceptPrivateVoiceChatResponse, SocialError> {
        let me = Self::caller(&context)?;
        let Ok(id) = Uuid::parse_str(&request.call_id) else {
            return Ok(AcceptPrivateVoiceChatResponse {
                response: Some(
                    accept_private_voice_chat_response::Response::InvalidRequest(invalid_req(
                        "invalid call_id",
                    )),
                ),
            });
        };
        let ctx = &context.server_context;
        let Some(chat) = ctx.db().get_private_voice_chat(id).await? else {
            return Ok(AcceptPrivateVoiceChatResponse {
                response: Some(accept_private_voice_chat_response::Response::NotFound(
                    NotFoundError {
                        message: Some("call not found".into()),
                    },
                )),
            });
        };
        if chat.callee_address != me {
            return Ok(AcceptPrivateVoiceChatResponse {
                response: Some(
                    accept_private_voice_chat_response::Response::ForbiddenError(ForbiddenError {
                        message: Some("not the callee".into()),
                    }),
                ),
            });
        }

        let urls = match ctx
            .voice()
            .private_voice_credentials(id, &chat.callee_address, &chat.caller_address)
            .await
        {
            Ok(urls) => urls,
            Err(e) => {
                return Ok(AcceptPrivateVoiceChatResponse {
                    response: Some(
                        accept_private_voice_chat_response::Response::InternalServerError(
                            internal_err(format!("private voice credentials: {e}")),
                        ),
                    ),
                });
            }
        };
        let caller_url = urls
            .get(&chat.caller_address.to_lowercase())
            .cloned()
            .unwrap_or_default();
        let callee_url = urls
            .get(&chat.callee_address.to_lowercase())
            .cloned()
            .unwrap_or_default();

        ctx.pubsub().publish(
            &chat.caller_address,
            SocialEvent::PrivateVoice(PrivateVoiceChatUpdate {
                call_id: request.call_id.clone(),
                status: PrivateVoiceChatStatus::VoiceChatAccepted as i32,
                caller: Some(User {
                    address: chat.caller_address.clone(),
                }),
                callee: Some(User {
                    address: chat.callee_address.clone(),
                }),
                credentials: Some(PrivateVoiceChatCredentials {
                    connection_url: caller_url,
                }),
            }),
        );

        Ok(AcceptPrivateVoiceChatResponse {
            response: Some(accept_private_voice_chat_response::Response::Ok(
                accept_private_voice_chat_response::Ok {
                    call_id: request.call_id,
                    credentials: Some(PrivateVoiceChatCredentials {
                        connection_url: callee_url,
                    }),
                },
            )),
        })
    }

    /// Deletes the pending call and tells the caller. Only the callee rejects,
    /// as upstream (`voice.ts:156-157`); anyone else, and an accepted call,
    /// get the unknown call's not-found and nothing changes.
    pub(crate) async fn reject_private_voice_chat(
        &self,
        request: RejectPrivateVoiceChatPayload,
        context: ProcedureContext<Context>,
    ) -> Result<RejectPrivateVoiceChatResponse, SocialError> {
        let me = Self::caller(&context)?;
        let Ok(id) = Uuid::parse_str(&request.call_id) else {
            return Ok(RejectPrivateVoiceChatResponse {
                response: Some(
                    reject_private_voice_chat_response::Response::InvalidRequest(invalid_req(
                        "invalid call_id",
                    )),
                ),
            });
        };
        let ctx = &context.server_context;
        let Some(chat) = ctx
            .voice()
            .finish_pending_private_call(PendingPrivateCall::Rejected { id, callee: me })
            .await?
        else {
            return Ok(RejectPrivateVoiceChatResponse {
                response: Some(reject_private_voice_chat_response::Response::NotFound(
                    NotFoundError {
                        message: Some("call not found".into()),
                    },
                )),
            });
        };
        ctx.pubsub().publish(
            &chat.caller_address,
            SocialEvent::PrivateVoice(PrivateVoiceChatUpdate {
                call_id: request.call_id.clone(),
                status: PrivateVoiceChatStatus::VoiceChatRejected as i32,
                caller: Some(User {
                    address: chat.caller_address.clone(),
                }),
                callee: Some(User {
                    address: chat.callee_address.clone(),
                }),
                credentials: None,
            }),
        );
        Ok(RejectPrivateVoiceChatResponse {
            response: Some(reject_private_voice_chat_response::Response::Ok(
                reject_private_voice_chat_response::Ok {
                    call_id: request.call_id,
                },
            )),
        })
    }

    /// Ends the call and tells both parties. Only a party ends it, as upstream
    /// (`voice.ts:189-191`); anyone else gets the unknown call's not-found and
    /// nothing changes. An accepted call ends through the voice runtime, which
    /// deletes the row and then the room; a pending one has no room.
    pub(crate) async fn end_private_voice_chat(
        &self,
        request: EndPrivateVoiceChatPayload,
        context: ProcedureContext<Context>,
    ) -> Result<EndPrivateVoiceChatResponse, SocialError> {
        let me = Self::caller(&context)?;
        let Ok(id) = Uuid::parse_str(&request.call_id) else {
            return Ok(EndPrivateVoiceChatResponse {
                response: Some(end_private_voice_chat_response::Response::NotFound(
                    NotFoundError {
                        message: Some("invalid call_id".into()),
                    },
                )),
            });
        };
        let ctx = &context.server_context;
        let chat = match ctx.voice().end_private_voice_chat(Some(id), &me).await? {
            Some(chat) => chat,
            None => {
                let pending = PendingPrivateCall::Ended { id, party: me };
                match ctx.voice().finish_pending_private_call(pending).await? {
                    Some(chat) => chat,
                    None => {
                        return Ok(EndPrivateVoiceChatResponse {
                            response: Some(end_private_voice_chat_response::Response::NotFound(
                                NotFoundError {
                                    message: Some("call not found".into()),
                                },
                            )),
                        });
                    }
                }
            }
        };
        ctx.publish_private_voice(&chat, PrivateVoiceChatStatus::VoiceChatEnded);
        Ok(EndPrivateVoiceChatResponse {
            response: Some(end_private_voice_chat_response::Response::Ok(
                end_private_voice_chat_response::Ok {
                    call_id: request.call_id,
                },
            )),
        })
    }

    pub(crate) async fn get_incoming_private_voice_chat_request(
        &self,
        context: ProcedureContext<Context>,
    ) -> Result<GetIncomingPrivateVoiceChatRequestResponse, SocialError> {
        let me = Self::caller(&context)?;
        match context
            .server_context
            .db()
            .incoming_private_voice_chat(&me)
            .await
        {
            Ok(Some(chat)) => Ok(GetIncomingPrivateVoiceChatRequestResponse {
                response: Some(
                    get_incoming_private_voice_chat_request_response::Response::Ok(
                        get_incoming_private_voice_chat_request_response::Ok {
                            caller: Some(User {
                                address: chat.caller_address,
                            }),
                            call_id: chat.id.to_string(),
                        },
                    ),
                ),
            }),
            Ok(None) => Ok(GetIncomingPrivateVoiceChatRequestResponse {
                response: Some(
                    get_incoming_private_voice_chat_request_response::Response::NotFound(
                        NotFoundError {
                            message: Some("no incoming call".into()),
                        },
                    ),
                ),
            }),
            Err(e) => Ok(GetIncomingPrivateVoiceChatRequestResponse {
                response: Some(
                    get_incoming_private_voice_chat_request_response::Response::InternalServerError(
                        InternalServerError {
                            message: Some(internal_err(e.to_string()).message.unwrap_or_default()),
                        },
                    ),
                ),
            }),
        }
    }
}
