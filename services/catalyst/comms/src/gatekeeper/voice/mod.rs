//! Voice: the private and community voice rooms (the private-messages room
//! token, private calls, community rooms and their expiry sweep), their
//! store (`db`) and the rooms trait social consumes (`rooms`); their
//! presence comes from the LiveKit webhook (`gatekeeper::webhook`).
//!
//! Token minting is offline. Webhook lifecycle cleanup must succeed before
//! acknowledgement; token and maintenance paths retain their existing policies.
//!
//! # Trust boundary of the bearer-gated routes (intentional)
//!
//! The `/private-voice-chat*`, `/community-voice-chat*`, `/users/{address}/
//! *voice-chat-status` and `/users/{address}/private-messages-privacy` routes
//! establish no per-user identity: their only gate is the platform service
//! token (`COMMS_GATEKEEPER_AUTH_TOKEN`, checked by the router layer), and the
//! wallet, the community and the claimed role come verbatim from the body.
//! Upstream puts community-role resolution in the calling platform service;
//! comms is the downstream executor. A leak of that one token is moderator
//! authority over every community at once. The friends/communities modules
//! calling these in-process inherit the same contract until they call the
//! logic functions directly.

pub mod db;
pub mod rooms;

use crate::shared::now_utc;
use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Extension, OriginalUri, Path, State};
use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{Address, AuthorityNotEstablished, CommunityId};

use super::auth::{EXPLORER_SIGNER, device_identifier, get_request_ip, verify_signed_fetch};
use super::errors::{ApiError, forbidden, service_unavailable};
use super::livekit::{AccessToken, KICK_CONCURRENCY, build_adapter_url, join_grants};
use super::moderation::store::UpsertPlayerConnection;
use super::{CommsState, SharedState};
use crate::rooms::private_voice_chat_room_name;
use crate::shared::signed_fetch::SignedPath;
use db::{CommunityJoinError, DeleteRoomError, EndedPrivateCall, PrivateJoin, VoiceChatUserStatus};

const STALE_LEAVE_SKEW_MS: i64 = 1000;

pub(super) fn now_ms() -> i64 {
    now_utc().and_utc().timestamp_millis()
}

pub(super) fn require_livekit(state: &CommsState) -> Result<(), ApiError> {
    if state.livekit.configured {
        Ok(())
    } else {
        Err(service_unavailable(
            "LiveKit is not configured (LIVEKIT_API_KEY / LIVEKIT_API_SECRET unset)",
        ))
    }
}

fn mint(
    state: &CommsState,
    identity: &str,
    grants: super::livekit::VideoGrants,
    metadata: Option<String>,
) -> Result<String, ApiError> {
    let mut token = AccessToken::new(
        &state.livekit.api_key,
        &state.livekit.api_secret,
        identity,
        grants,
    );
    if let Some(m) = metadata {
        token = token.with_metadata(m);
    }
    let jwt = token
        .to_jwt()
        .map_err(|e| ApiError::internal(format!("livekit token: {e}")))?;
    Ok(build_adapter_url(&state.livekit.client_url, &jwt))
}

/// GET /private-messages/token: an explorer-signed fetch; the platform ban
/// probe (device-widened) refuses before minting; the token is subscribe-only
/// on the private-messages room and carries the wallet's DM privacy. The
/// connection record is a social write, skipped while the social switch is
/// off or held; the mint is admission and still answers.
pub async fn private_messages_token(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    switch: Option<Extension<crate::writes::Switch>>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let sf = verify_signed_fetch(
        &headers,
        "get",
        &SignedPath::at(&uri, "/private-messages/token"),
        &[EXPLORER_SIGNER],
        state.eip1654(),
    )
    .await?;
    let identity = sf.signer;
    if state.deny_list.is_denied(identity.as_str()).await {
        return Err(super::errors::unauthorized(
            "Access denied, wallet is deny-listed.",
        ));
    }
    let ip_address = get_request_ip(&headers);
    let device_id = device_identifier(&sf.metadata);
    let record = crate::writes::side_refusal(switch).is_none();
    let upsert = async {
        if !record {
            return;
        }
        if let Err(e) = state
            .moderation
            .upsert_connection(UpsertPlayerConnection {
                address: identity.clone(),
                ip_address,
                device_id: device_id.clone(),
            })
            .await
        {
            tracing::warn!(error = %e, address = %identity, "failed to store player connection info");
        }
    };
    let ((), banned, privacy) = tokio::join!(
        upsert,
        state
            .moderation
            .is_banned_for_connection(&identity, device_id.as_deref()),
        state.social.private_messages_privacy(&identity)
    );
    let map_error = |e: sqlx::Error| {
        ApiError::from(AuthorityNotEstablished::store_unavailable(
            "social.user_bans",
            &e,
        ))
    };
    let banned = banned.map_err(map_error)?;
    let privacy = privacy.map_err(map_error)?;
    if banned {
        return Err(forbidden(super::moderation::store::PLATFORM_BANNED_MSG));
    }
    let metadata = serde_json::json!({ "private_messages_privacy": privacy }).to_string();
    let mut grants = join_grants(&state.settings.private_messages_room_id);
    grants.can_publish = false;
    grants.can_update_own_metadata = false;
    let adapter = mint(&state, identity.as_str(), grants, Some(metadata))?;
    Ok(Json(serde_json::json!({ "adapter": adapter })))
}

#[derive(Debug, Deserialize)]
pub struct PrivateMessagesPrivacyBody {
    pub private_messages_privacy: Option<String>,
}

/// PATCH /users/{address}/private-messages-privacy (service token): writes
/// the ONE `social.user_settings` row the friends module reads too.
pub async fn patch_private_messages_privacy(
    State(state): State<SharedState>,
    Path(address): Path<String>,
    Json(body): Json<PrivateMessagesPrivacyBody>,
) -> Result<Json<Value>, ApiError> {
    let address = Address::parse(&address).map_err(|_| ApiError::bad_request("Invalid address"))?;
    let privacy = body
        .private_messages_privacy
        .as_deref()
        .map(|s| s.to_lowercase())
        .filter(|s| s == "all" || s == "only_friends")
        .ok_or_else(|| ApiError::bad_request("Invalid private_messages_privacy"))?;
    state
        .social
        .set_private_messages_privacy(&address, &privacy)
        .await?;
    Ok(Json(
        serde_json::json!({ "address": address.as_str(), "private_messages_privacy": privacy }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct PrivateVoiceChatBody {
    pub room_id: String,
    pub user_addresses: Vec<String>,
}

/// The credentials of a private call: one token per participant, both sides
/// checked against the platform ban first, then the call row opened with
/// both `not_connected`. `room_id` is the call id (`social.private_voice_chats`
/// keys it as a uuid, so the friends module's ids are the wire's).
pub async fn get_private_voice_chat_room_credentials(
    state: &CommsState,
    room_id: &str,
    user_addresses: &[Address],
) -> Result<BTreeMap<String, Value>, ApiError> {
    let call_id = Uuid::parse_str(room_id.trim())
        .map_err(|_| ApiError::bad_request("Invalid request body, room_id must be a uuid"))?;
    let [caller, callee] = user_addresses else {
        return Err(ApiError::bad_request(
            "Invalid request body, user_addresses must name the two participants",
        ));
    };
    if caller == callee {
        return Err(ApiError::bad_request(
            "Invalid request body, user_addresses must name two different wallets",
        ));
    }
    state
        .moderation
        .platform_ban_decision(user_addresses)
        .await
        .into_result()?;
    let room_name = private_voice_chat_room_name(&call_id);
    let mut out: BTreeMap<String, Value> = BTreeMap::new();
    for addr in user_addresses {
        let mut grants = join_grants(&room_name);
        grants.can_update_own_metadata = false;
        let connection_url = mint(state, addr.as_str(), grants, None)?;
        out.insert(
            addr.as_str().to_string(),
            serde_json::json!({ "connection_url": connection_url }),
        );
    }
    let displaced = state
        .voice_db
        .create_private_call(call_id, caller, callee)
        .await?;
    if !displaced.is_empty() {
        let rooms: Vec<String> = displaced.iter().map(private_voice_chat_room_name).collect();
        delete_livekit_rooms(state, &rooms, "displaced private voice room").await;
    }
    Ok(out)
}

/// POST /private-voice-chat (service token).
pub async fn create_private_voice_chat(
    State(state): State<SharedState>,
    Json(body): Json<PrivateVoiceChatBody>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    if body.room_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "Invalid request body, missing room_id",
        ));
    }
    if body.user_addresses.is_empty() {
        return Err(ApiError::bad_request(
            "Invalid request body, missing user_addresses",
        ));
    }
    let addresses = body
        .user_addresses
        .iter()
        .map(|a| Address::parse(a))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ApiError::bad_request("Invalid request body, invalid user_addresses"))?;
    let out = get_private_voice_chat_room_credentials(&state, &body.room_id, &addresses).await?;
    Ok(Json(
        serde_json::to_value(out).unwrap_or(serde_json::json!({})),
    ))
}

#[derive(Debug, Deserialize)]
pub struct EndPrivateVoiceChatBody {
    pub address: Option<String>,
}

pub async fn end_private_voice_chat_by(
    state: &CommsState,
    room_id: &str,
    address: &Address,
) -> Result<EndedPrivateCall, ApiError> {
    let not_found = || ApiError::not_found(format!("Room {room_id} does not exist"));
    let call_id = Uuid::parse_str(room_id.trim()).map_err(|_| not_found())?;
    end_private_call(state, Some(call_id), address)
        .await?
        .ok_or_else(not_found)
}

/// Ends the accepted call `call_id` (or else the latest one `address` is in)
/// when `address` is one of its participants: the row goes first, then the
/// LiveKit room. A failed room delete is logged; the row is gone either way.
/// `None` when `address` is in no such call.
pub async fn end_private_call(
    state: &CommsState,
    call_id: Option<Uuid>,
    address: &Address,
) -> Result<Option<EndedPrivateCall>, ApiError> {
    let ended = match state
        .voice_db
        .delete_private_call_if_member(call_id, address)
        .await
    {
        Ok(ended) => ended,
        Err(DeleteRoomError::RoomDoesNotExist) => return Ok(None),
        Err(DeleteRoomError::Db(e)) => return Err(e.into()),
    };
    let room_name = private_voice_chat_room_name(&ended.call.id);
    if let Err(e) = state.room_service().delete_room(&room_name).await {
        tracing::warn!(error = %e, room = %room_name, "failed to delete livekit private voice room");
    }
    Ok(Some(ended))
}

/// DELETE /private-voice-chat/{id} (service token).
pub async fn end_private_voice_chat(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(body): Json<EndPrivateVoiceChatBody>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let address = body
        .address
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("Invalid request body, missing address"))?;
    let address = Address::parse(address)
        .map_err(|_| ApiError::bad_request("Invalid request body, invalid address"))?;
    let users_in_room = end_private_voice_chat_by(&state, &id, &address)
        .await?
        .users;
    Ok(Json(
        serde_json::json!({ "users_in_voice_chat": users_in_room }),
    ))
}

#[derive(Debug, serde::Serialize)]
pub struct VoiceChatStatusResponse {
    pub is_user_in_voice_chat: bool,
}

/// GET /users/{address}/voice-chat-status (service token).
pub async fn get_voice_chat_status(
    State(state): State<SharedState>,
    Path(address): Path<String>,
) -> Result<Json<VoiceChatStatusResponse>, ApiError> {
    let address = Address::parse(&address).map_err(|_| ApiError::bad_request("Invalid address"))?;
    let is_user_in_voice_chat = state.voice_db.active_call_of(&address).await?.is_some();
    Ok(Json(VoiceChatStatusResponse {
        is_user_in_voice_chat,
    }))
}

#[derive(Debug, Deserialize)]
pub struct CommunityVoiceChatBody {
    pub community_id: String,
    /// A claim: nothing here verifies the caller controls this wallet.
    pub user_address: String,
    /// A claim: nothing here verifies this role was ever stored.
    pub user_role: Option<String>,
    pub action: Option<String>,
    pub profile_data: Option<Value>,
}

/// The role name assumed when the body omits `user_role`; still a claim.
const CLAIMED_ROLE_WHEN_OMITTED: &str = "none";

/// Whether the role the platform service **claimed** is one this handler
/// grants publish rights and the persisted moderator flag for. Exactly
/// `owner | moderator` (lowercased): not `mod`, not `admin`.
fn claimed_role_grants_speaker_rights(claimed_role_lowercased: &str) -> bool {
    matches!(claimed_role_lowercased, "owner" | "moderator")
}

fn community_join_metadata(
    claimed_role: &str,
    is_speaker: bool,
    profile_data: Option<&Value>,
) -> serde_json::Map<String, Value> {
    let mut metadata = serde_json::Map::new();
    metadata.insert("role".into(), serde_json::json!(claimed_role));
    metadata.insert("isSpeaker".into(), serde_json::json!(is_speaker));
    metadata.insert("muted".into(), serde_json::json!(false));
    if let Some(profile) = profile_data.and_then(|v| v.as_object()) {
        if let Some(name) = profile.get("name") {
            metadata.insert("name".into(), name.clone());
        }
        if let Some(claimed) = profile.get("has_claimed_name") {
            metadata.insert("hasClaimedName".into(), claimed.clone());
        }
        if let Some(pic) = profile.get("profile_picture_url") {
            metadata.insert("profilePictureUrl".into(), pic.clone());
        }
    }
    metadata
}

fn parse_community_id(raw: &str) -> Result<CommunityId, ApiError> {
    if raw.trim().is_empty() {
        return Err(ApiError::bad_request(
            "The parameter communityId is required",
        ));
    }
    CommunityId::parse(raw)
        .map_err(|_| ApiError::bad_request("The parameter communityId is invalid"))
}

/// POST /community-voice-chat (service token). See the module note: the
/// claimed role decides speaker rights; the platform ban is re-checked.
pub async fn community_voice_chat_create_or_join(
    State(state): State<SharedState>,
    Json(body): Json<CommunityVoiceChatBody>,
) -> Result<Json<Value>, ApiError> {
    let connection_url = community_voice_credentials_for(&state, body).await?;
    Ok(Json(
        serde_json::json!({ "connection_url": connection_url }),
    ))
}

/// The grant of a community room for `body.user_address` with the role the
/// caller states (the route's body, or the social RPC's stored role through
/// `rooms`): the platform ban re-checked, the room row joined.
pub async fn community_voice_credentials_for(
    state: &CommsState,
    body: CommunityVoiceChatBody,
) -> Result<String, ApiError> {
    require_livekit(state)?;
    if body.community_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "The property community_id is required",
        ));
    }
    let community_id = CommunityId::parse(&body.community_id)
        .map_err(|_| ApiError::bad_request("The property community_id is invalid"))?;
    let claimed_wallet = Address::parse(&body.user_address)
        .map_err(|_| ApiError::bad_request("The property user_address is invalid"))?;
    let claimed_role = body
        .user_role
        .as_deref()
        .unwrap_or(CLAIMED_ROLE_WHEN_OMITTED)
        .to_lowercase();
    state
        .moderation
        .platform_ban_decision(std::slice::from_ref(&claimed_wallet))
        .await
        .into_result()?;
    let speaker_rights = claimed_role_grants_speaker_rights(&claimed_role);
    let action = body.action.as_deref().unwrap_or("join").to_lowercase();
    let is_speaker = action == "create" && speaker_rights;
    let room_name = crate::rooms::community_voice_chat_room_name(&community_id);
    let metadata = community_join_metadata(&claimed_role, is_speaker, body.profile_data.as_ref());
    let mut grants = join_grants(&room_name);
    grants.can_publish = is_speaker;
    grants.can_update_own_metadata = false;
    let connection_url = mint(
        state,
        claimed_wallet.as_str(),
        grants,
        Some(Value::Object(metadata).to_string()),
    )?;
    match state
        .voice_db
        .join_user_to_community_room(community_id, &claimed_wallet, speaker_rights)
        .await
    {
        Ok(()) => {}
        Err(CommunityJoinError::CommunityNotFound) => {
            return Err(ApiError::not_found("Community not found"));
        }
        Err(CommunityJoinError::Db(e)) => return Err(e.into()),
    }
    Ok(connection_url)
}

#[derive(Debug, serde::Serialize)]
pub struct CommunityVoiceChatStatusResponse {
    pub active: bool,
    pub participant_count: i64,
    pub moderator_count: i64,
}

async fn community_status(
    state: &CommsState,
    community_id: CommunityId,
) -> Result<(bool, i64, i64), ApiError> {
    let users = state.voice_db.community_users_in_room(community_id).await?;
    let now = now_ms();
    let active_participants = users
        .iter()
        .filter(|u| state.voice_db.is_active_community_user(u, now))
        .count() as i64;
    let active_moderators = users
        .iter()
        .filter(|u| u.is_moderator && state.voice_db.is_active_community_user(u, now))
        .count() as i64;
    Ok((
        active_moderators > 0,
        active_participants,
        active_moderators,
    ))
}

/// GET /community-voice-chat/{id}/status (service token).
pub async fn community_voice_chat_status(
    State(state): State<SharedState>,
    Path(community_id): Path<String>,
) -> Result<Json<CommunityVoiceChatStatusResponse>, ApiError> {
    let community_id = parse_community_id(&community_id)?;
    let (active, participant_count, moderator_count) =
        community_status(&state, community_id).await?;
    let (participant_count, moderator_count) = if active {
        (participant_count, moderator_count)
    } else {
        (0, 0)
    };
    Ok(Json(CommunityVoiceChatStatusResponse {
        active,
        participant_count,
        moderator_count,
    }))
}

#[derive(Debug, Deserialize)]
pub struct BulkCommunityStatusBody {
    pub community_ids: Vec<String>,
}

const MAX_BULK_COMMUNITY_IDS: usize = 100;

fn validate_bulk_community_ids(ids: &[String]) -> Result<(), ApiError> {
    if ids.len() > MAX_BULK_COMMUNITY_IDS {
        return Err(ApiError::bad_request(format!(
            "community_ids must contain at most {MAX_BULK_COMMUNITY_IDS} items"
        )));
    }
    Ok(())
}

/// POST /community-voice-chat/status (service token): one statement for the
/// whole set; an id that is not a uuid answers inactive.
pub async fn community_voice_chat_bulk_status(
    State(state): State<SharedState>,
    Json(body): Json<BulkCommunityStatusBody>,
) -> Result<Json<Value>, ApiError> {
    validate_bulk_community_ids(&body.community_ids)?;
    let parsed: Vec<Option<CommunityId>> = body
        .community_ids
        .iter()
        .map(|id| CommunityId::parse(id).ok())
        .collect();
    let ids: Vec<CommunityId> = parsed.iter().flatten().copied().collect();
    let by_id = state.voice_db.bulk_community_status(&ids).await?;
    let data: Vec<Value> = body
        .community_ids
        .iter()
        .zip(&parsed)
        .map(|(raw, id)| {
            let (participant_count, moderator_count) =
                id.and_then(|id| by_id.get(&id).copied()).unwrap_or((0, 0));
            serde_json::json!({
                "community_id": raw,
                "active": moderator_count > 0,
                "participant_count": participant_count,
                "moderator_count": moderator_count,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "data": data })))
}

/// GET /community-voice-chat/active (service token).
pub async fn community_voice_chat_active(
    State(state): State<SharedState>,
) -> Result<Json<Value>, ApiError> {
    let active = state.voice_db.all_active_community_voice_chats().await?;
    let data: Vec<Value> = active
        .into_iter()
        .map(|c| serde_json::json!({ "communityId": c.community_id.to_string(), "participantCount": c.participant_count, "moderatorCount": c.moderator_count }))
        .collect();
    let total = data.len();
    Ok(Json(serde_json::json!({ "data": data, "total": total })))
}

#[derive(Debug, Deserialize)]
pub struct EndCommunityVoiceChatBody {
    pub user_address: Option<String>,
}

/// DELETE /community-voice-chat/{id} (service token).
pub async fn community_voice_chat_end(
    State(state): State<SharedState>,
    Path(community_id): Path<String>,
    Json(body): Json<EndCommunityVoiceChatBody>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let community_id = parse_community_id(&community_id)?;
    if body
        .user_address
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(ApiError::bad_request(
            "The property user_address is required",
        ));
    }
    destroy_community_voice_chat(&state, community_id, "livekit community voice room").await?;
    Ok(Json(
        serde_json::json!({ "message": "Community voice chat ended successfully" }),
    ))
}

/// The shape check of the moderation handlers' path parameters: non-empty,
/// the address lowercased. Not an authorization (see the module note).
fn community_and_claimed_target(
    community_id: &str,
    claimed_user_address: &str,
) -> Result<(CommunityId, String), ApiError> {
    let community_id = parse_community_id(community_id)?;
    let addr = claimed_user_address.to_lowercase();
    if addr.trim().is_empty() {
        return Err(ApiError::bad_request(
            "The parameter userAddress is required",
        ));
    }
    Ok((community_id, addr))
}

pub(super) async fn merge_metadata(
    state: &CommsState,
    room_name: &str,
    address: &str,
    patch: serde_json::Map<String, Value>,
) -> Result<(), ApiError> {
    state
        .room_service()
        .merge_participant_metadata(room_name, address, patch)
        .await
        .map_err(|e| ApiError::internal(format!("livekit update participant metadata: {e}")))
}

pub(super) fn patch(entries: &[(&str, Value)]) -> serde_json::Map<String, Value> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

pub async fn community_request_to_speak(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    merge_metadata(
        &state,
        &crate::rooms::community_voice_chat_room_name(&community_id),
        &target,
        patch(&[("isRequestingToSpeak", serde_json::json!(true))]),
    )
    .await?;
    Ok(Json(
        serde_json::json!({ "message": "Request to speak sent successfully" }),
    ))
}

pub async fn community_reject_speak_request(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    merge_metadata(
        &state,
        &crate::rooms::community_voice_chat_room_name(&community_id),
        &target,
        patch(&[("isRequestingToSpeak", serde_json::json!(false))]),
    )
    .await?;
    Ok(Json(
        serde_json::json!({ "message": "Speak request rejected successfully" }),
    ))
}

pub(super) async fn set_speaker(
    state: &CommsState,
    community_id: CommunityId,
    target: &str,
    speaker: bool,
) -> Result<(), ApiError> {
    state
        .room_service()
        .merge_participant_metadata_with_permission(
            &crate::rooms::community_voice_chat_room_name(&community_id),
            target,
            patch(&[("isRequestingToSpeak", serde_json::json!(false)), ("isSpeaker", serde_json::json!(speaker))]),
            serde_json::json!({ "canPublish": speaker, "canSubscribe": true, "canPublishData": true }),
        )
        .await
        .map_err(|e| ApiError::internal(format!("livekit update participant permissions: {e}")))
}

pub async fn community_promote_speaker(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    set_speaker(&state, community_id, &target, true).await?;
    Ok(Json(
        serde_json::json!({ "message": "User promoted to speaker successfully" }),
    ))
}

pub async fn community_demote_speaker(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    set_speaker(&state, community_id, &target, false).await?;
    Ok(Json(
        serde_json::json!({ "message": "User demoted to listener successfully" }),
    ))
}

pub async fn community_kick_player(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    kick_from_community_room(&state, community_id, &target).await?;
    Ok(Json(
        serde_json::json!({ "message": "User kicked from voice chat successfully" }),
    ))
}

/// Removes `target` from the community room: the LiveKit participant
/// (best-effort, logged) and the presence row (the failure that counts).
pub async fn kick_from_community_room(
    state: &CommsState,
    community_id: CommunityId,
    target: &str,
) -> Result<(), ApiError> {
    let room_name = crate::rooms::community_voice_chat_room_name(&community_id);
    let room_service = state.room_service();
    let (removed, deleted) = tokio::join!(
        room_service.remove_participant(&room_name, target),
        state.voice_db.remove_community_user(community_id, target)
    );
    if let Err(e) = removed {
        tracing::warn!(error = %e, room = %room_name, addr = %target, "failed to remove community voice participant");
    }
    deleted?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct MuteSpeakerBody {
    pub muted: bool,
}

pub async fn community_mute_speaker(
    State(state): State<SharedState>,
    Path((community_id, user_address)): Path<(String, String)>,
    Json(body): Json<MuteSpeakerBody>,
) -> Result<Json<Value>, ApiError> {
    require_livekit(&state)?;
    let (community_id, target) = community_and_claimed_target(&community_id, &user_address)?;
    merge_metadata(
        &state,
        &crate::rooms::community_voice_chat_room_name(&community_id),
        &target,
        patch(&[("muted", serde_json::json!(body.muted))]),
    )
    .await?;
    let action = if body.muted { "muted" } else { "unmuted" };
    Ok(Json(
        serde_json::json!({ "message": format!("User {action} successfully") }),
    ))
}

/// GET /users/{address}/community-voice-chat-status (service token).
pub async fn check_user_community_status(
    State(state): State<SharedState>,
    Path(user_address): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let address =
        Address::parse(&user_address).map_err(|_| ApiError::bad_request("Invalid address"))?;
    let is_in = state
        .voice_db
        .is_user_in_any_community_voice_chat(&address)
        .await?;
    Ok(Json(
        serde_json::json!({ "userAddress": address.as_str(), "isInCommunityVoiceChat": is_in }),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    UnknownReason,
    ClientInitiated,
    DuplicateIdentity,
    ServerShutdown,
    ParticipantRemoved,
    RoomDeleted,
    StateMismatch,
    JoinFailure,
    Migration,
    SignalClose,
    RoomClosed,
    UserUnavailable,
    UserRejected,
    SipTrunkFailure,
    Other(i64),
}

impl DisconnectReason {
    fn from_i64(v: i64) -> Self {
        match v {
            0 => Self::UnknownReason,
            1 => Self::ClientInitiated,
            2 => Self::DuplicateIdentity,
            3 => Self::ServerShutdown,
            4 => Self::ParticipantRemoved,
            5 => Self::RoomDeleted,
            6 => Self::StateMismatch,
            7 => Self::JoinFailure,
            8 => Self::Migration,
            9 => Self::SignalClose,
            10 => Self::RoomClosed,
            11 => Self::UserUnavailable,
            12 => Self::UserRejected,
            13 => Self::SipTrunkFailure,
            other => Self::Other(other),
        }
    }

    fn from_name(s: &str) -> Self {
        match s {
            "UNKNOWN_REASON" => Self::UnknownReason,
            "CLIENT_INITIATED" => Self::ClientInitiated,
            "DUPLICATE_IDENTITY" => Self::DuplicateIdentity,
            "SERVER_SHUTDOWN" => Self::ServerShutdown,
            "PARTICIPANT_REMOVED" => Self::ParticipantRemoved,
            "ROOM_DELETED" => Self::RoomDeleted,
            "STATE_MISMATCH" => Self::StateMismatch,
            "JOIN_FAILURE" => Self::JoinFailure,
            "MIGRATION" => Self::Migration,
            "SIGNAL_CLOSE" => Self::SignalClose,
            "ROOM_CLOSED" => Self::RoomClosed,
            "USER_UNAVAILABLE" => Self::UserUnavailable,
            "USER_REJECTED" => Self::UserRejected,
            "SIP_TRUNK_FAILURE" => Self::SipTrunkFailure,
            _ => Self::UnknownReason,
        }
    }

    pub fn parse(v: Option<&Value>) -> Self {
        match v {
            Some(Value::Number(n)) => Self::from_i64(n.as_i64().unwrap_or(0)),
            Some(Value::String(s)) => Self::from_name(s),
            _ => Self::UnknownReason,
        }
    }
}

async fn delete_livekit_rooms(state: &CommsState, room_names: &[String], what: &'static str) {
    use futures::StreamExt;
    if !state.livekit.configured || room_names.is_empty() {
        return;
    }
    let room_service = state.room_service();
    futures::stream::iter(room_names)
        .for_each_concurrent(KICK_CONCURRENCY, |room_name| {
            let room_service = &room_service;
            async move {
                if let Err(e) = room_service.delete_room(room_name).await {
                    tracing::warn!(error = %e, room = %room_name, "failed to delete {what}");
                }
            }
        })
        .await;
}

// Bound webhook media operations independently of the client's other call
// sites. DeleteRoom treats an already absent room as success.
pub(super) async fn delete_webhook_room(state: &CommsState, room: &str) -> Result<(), ApiError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.room_service().delete_room(room),
    )
    .await
    .map_err(|_| ApiError::internal("webhook room deletion timed out"))?
    .map_err(|_| ApiError::internal("webhook room deletion failed"))
}

// Persist the notification before deleting its source rows, using the provider
// event ID as a stable broadcast key. Failed notification/DB work retains the
// participant count and moderator state for a complete retry.
async fn finish_webhook_community(
    state: &CommsState,
    community_id: CommunityId,
    event_id: &str,
    delete_media: bool,
) -> Result<(), ApiError> {
    let users = state.voice_db.community_users_in_room(community_id).await?;
    if delete_media {
        delete_webhook_room(
            state,
            &crate::rooms::community_voice_chat_room_name(&community_id),
        )
        .await?;
    }
    if !users.is_empty() {
        let now = now_utc();
        let key = format!("community-streaming-ended-{event_id}");
        let event = community_streaming_ended_event(
            &community_id.to_string(),
            users.len() as i64,
            &key,
            now.and_utc().timestamp_millis(),
        );
        state
            .social
            .notify_community_voice_ended(community_id, &key, &event, now)
            .await?;
    }
    state
        .voice_db
        .delete_community_voice_chat(community_id)
        .await?;
    Ok(())
}

pub async fn handle_private_participant_joined(
    state: &CommsState,
    user_address: &str,
    call_id: Uuid,
) -> Result<(), ApiError> {
    let prepared = state
        .voice_db
        .prepare_private_join(user_address, call_id)
        .await?;
    match &prepared.outcome {
        PrivateJoin::Inactive => {
            delete_webhook_room(state, &private_voice_chat_room_name(&call_id)).await?;
        }
        PrivateJoin::NotInRoom => {
            return Err(
                sqlx::Error::Protocol(format!("User {user_address} is not in a room")).into(),
            );
        }
        PrivateJoin::Joined { old_call } if *old_call != call_id => {
            delete_webhook_room(state, &private_voice_chat_room_name(old_call)).await?;
        }
        PrivateJoin::Joined { .. } => {}
    }
    prepared.commit().await?;
    Ok(())
}

pub async fn handle_private_participant_left(
    state: &CommsState,
    user_address: &str,
    call_id: Uuid,
    reason: DisconnectReason,
) -> Result<(), ApiError> {
    let room_name = private_voice_chat_room_name(&call_id);
    match reason {
        DisconnectReason::DuplicateIdentity => Ok(()),
        DisconnectReason::ClientInitiated => {
            tracing::debug!(user = user_address, room = %room_name, "user left a private room willingly, destroying it");
            delete_webhook_room(state, &room_name).await?;
            Ok(state
                .voice_db
                .mark_participant_disconnected(call_id, user_address)
                .await?)
        }
        DisconnectReason::RoomDeleted => Ok(state.voice_db.delete_private_call(call_id).await?),
        _ => Ok(state
            .voice_db
            .mark_participant_interrupted(call_id, user_address)
            .await?),
    }
}

pub async fn expire_private_voice_chats(state: &CommsState) -> Result<(), ApiError> {
    let expired = state.voice_db.delete_expired_private_calls().await?;
    let rooms: Vec<String> = expired.iter().map(private_voice_chat_room_name).collect();
    delete_livekit_rooms(state, &rooms, "expired private voice room").await;
    if !rooms.is_empty() {
        tracing::info!(count = rooms.len(), "deleted expired private voice chats");
    }
    Ok(())
}

const EVENT_TYPE_STREAMING: &str = "streaming";
const EVENT_SUBTYPE_COMMUNITY_STREAMING_ENDED: &str = "community-streaming-ended";

/// The @dcl/schemas `CommunityStreamingEnded` event document (the old SNS
/// payload), kept as the notification's metadata.
fn community_streaming_ended_event(
    community_id: &str,
    participant_count: i64,
    key: &str,
    timestamp_ms: i64,
) -> Value {
    serde_json::json!({
        "type": EVENT_TYPE_STREAMING,
        "subType": EVENT_SUBTYPE_COMMUNITY_STREAMING_ENDED,
        "key": key,
        "timestamp": timestamp_ms,
        "metadata": { "communityId": community_id, "totalParticipants": participant_count },
    })
}

/// The old crate wrote an SNS outbox row the notifications service fanned
/// out; on one database the in-app notification lands in
/// `social.notifications` directly, one row per community member, idempotent
/// per recipient on `broadcast_key` (the old event key). Rooms already
/// deleted (count 0) are skipped. The notifications module owns the
/// out-of-process channels; none is enqueued here.
async fn notify_community_streaming_ended(state: &CommsState, rooms: &[(CommunityId, i64)]) {
    let now = now_utc();
    let now_ms = now.and_utc().timestamp_millis();
    let mut count = 0usize;
    for (community_id, participant_count) in rooms {
        if *participant_count == 0 {
            tracing::debug!(%community_id, "skipping CommunityStreamingEnded since voice chat was already deleted");
            continue;
        }
        let key = format!("community-streaming-ended-{community_id}-{now_ms}");
        let event = community_streaming_ended_event(
            &community_id.to_string(),
            *participant_count,
            &key,
            now_ms,
        );
        let res = state
            .social
            .notify_community_voice_ended(*community_id, &key, &event, now)
            .await;
        match res {
            Ok(_) => count += 1,
            Err(e) => {
                tracing::error!(error = %e, %community_id, "failed to write the CommunityStreamingEnded notifications")
            }
        }
    }
    if count > 0 {
        tracing::info!(
            rooms = count,
            "published CommunityStreamingEnded notifications"
        );
    }
}

/// Drops the LiveKit room and the presence rows together; the row count is
/// the participant count the ended notification reports.
pub(super) async fn destroy_community_voice_chat(
    state: &CommsState,
    community_id: CommunityId,
    what: &'static str,
) -> Result<(), ApiError> {
    let room_name = crate::rooms::community_voice_chat_room_name(&community_id);
    let room_service = state.room_service();
    let (livekit, deleted) = tokio::join!(
        async {
            if state.livekit.configured {
                room_service.delete_room(&room_name).await
            } else {
                Ok(())
            }
        },
        state.voice_db.delete_community_voice_chat(community_id),
    );
    if let Err(e) = livekit {
        tracing::warn!(error = %e, room = %room_name, "failed to delete {what}");
    }
    let participant_count = deleted?;
    notify_community_streaming_ended(state, &[(community_id, participant_count)]).await;
    Ok(())
}

pub async fn handle_community_participant_joined(
    state: &CommsState,
    user_address: &str,
    community_id: CommunityId,
    sid: Option<&str>,
) -> Result<(), ApiError> {
    if sid.is_none() {
        tracing::warn!(user = user_address, %community_id, "community participant joined without a session id");
    }
    Ok(state
        .voice_db
        .update_community_user_status(
            community_id,
            user_address,
            VoiceChatUserStatus::Connected,
            sid,
        )
        .await?)
}

/// A `participant_left` from a previous session (a different sid, or one that
/// predates the row's last update) is stale: the user reconnected.
fn is_stale_leave(
    current_sid: Option<&str>,
    leaving_sid: Option<&str>,
    status_updated_at_ms: i64,
    leave_event_time_ms: Option<i64>,
) -> bool {
    let is_from_previous_session =
        matches!((leaving_sid, current_sid), (Some(leaving), Some(current)) if current != leaving);
    let user_state_is_newer_than_leave = current_sid.is_none()
        && leave_event_time_ms
            .map(|t| status_updated_at_ms > t + STALE_LEAVE_SKEW_MS)
            .unwrap_or(false);
    is_from_previous_session || user_state_is_newer_than_leave
}

pub async fn handle_community_participant_left(
    state: &CommsState,
    user_address: &str,
    community_id: CommunityId,
    reason: DisconnectReason,
    sid: Option<&str>,
    leave_event_time_ms: Option<i64>,
    event_id: &str,
) -> Result<(), ApiError> {
    match reason {
        DisconnectReason::DuplicateIdentity => {
            tracing::debug!(user = user_address, %community_id, "ignoring community disconnect due to duplicate identity");
            Ok(())
        }
        DisconnectReason::RoomDeleted => {
            finish_webhook_community(state, community_id, event_id, false).await
        }
        DisconnectReason::ClientInitiated => {
            let users_in_room = state.voice_db.community_users_in_room(community_id).await?;
            let leaving_user = users_in_room.iter().find(|u| u.address == user_address);
            if let Some(leaving_user) = leaving_user
                && is_stale_leave(
                    leaving_user.sid.as_deref(),
                    sid,
                    leaving_user.status_updated_at,
                    leave_event_time_ms,
                )
            {
                tracing::info!(user = user_address, %community_id, "ignoring stale participant_left: a newer session is active");
                return Ok(());
            }
            if leaving_user.is_some_and(|u| u.is_moderator) {
                let now = now_ms();
                let remaining = users_in_room
                    .iter()
                    .filter(|u| {
                        u.is_moderator
                            && u.address != user_address
                            && state.voice_db.is_active_community_user(u, now)
                    })
                    .count();
                if remaining == 0 {
                    tracing::debug!(%community_id, "no active moderators left in community room, destroying it");
                    return finish_webhook_community(state, community_id, event_id, true).await;
                }
            }
            state
                .voice_db
                .update_community_user_status(
                    community_id,
                    user_address,
                    VoiceChatUserStatus::Disconnected,
                    None,
                )
                .await?;
            Ok(())
        }
        _ => Ok(state
            .voice_db
            .update_community_user_status(
                community_id,
                user_address,
                VoiceChatUserStatus::ConnectionInterrupted,
                None,
            )
            .await?),
    }
}

pub async fn expire_community_voice_chats(state: &CommsState) -> Result<(), ApiError> {
    let room_counts = state.voice_db.active_community_participant_counts().await?;
    let expired = state
        .voice_db
        .delete_expired_community_voice_chats()
        .await?;
    let ended: Vec<(CommunityId, i64)> = expired
        .iter()
        .map(|id| {
            tracing::info!(community = %id, "expiring community voice chat room");
            (*id, room_counts.get(id).copied().unwrap_or(0))
        })
        .collect();
    let rooms: Vec<String> = expired
        .iter()
        .map(crate::rooms::community_voice_chat_room_name)
        .collect();
    let ((), ()) = tokio::join!(
        delete_livekit_rooms(state, &rooms, "expired community voice room"),
        notify_community_streaming_ended(state, &ended)
    );
    Ok(())
}

/// One sweep of both presence tables (the `comms-voice-expiration` job).
pub async fn expire_voice_chats(state: &CommsState) -> anyhow::Result<()> {
    expire_private_voice_chats(state)
        .await
        .map_err(|e| anyhow::anyhow!("private voice chat expiration: {e}"))?;
    expire_community_voice_chats(state)
        .await
        .map_err(|e| anyhow::anyhow!("community voice chat expiration: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_deserialize_as_upstream() {
        let body: PrivateVoiceChatBody = serde_json::from_str(
            r#"{ "room_id": "call-1", "user_addresses": ["0xAAA", "0xBBB"] }"#,
        )
        .unwrap();
        assert_eq!(body.room_id, "call-1");
        assert_eq!(body.user_addresses, vec!["0xAAA", "0xBBB"]);
        let with: EndPrivateVoiceChatBody =
            serde_json::from_str(r#"{ "address": "0xabc" }"#).unwrap();
        assert_eq!(with.address.as_deref(), Some("0xabc"));
        let without: EndPrivateVoiceChatBody = serde_json::from_str("{}").unwrap();
        assert!(without.address.is_none());
        let full: CommunityVoiceChatBody = serde_json::from_str(
            r#"{ "community_id": "c1", "user_address": "0xabc", "user_role": "owner", "action": "create",
                 "profile_data": { "name": "Foo", "has_claimed_name": true, "profile_picture_url": "http://x/y.png" } }"#,
        )
        .unwrap();
        assert_eq!(full.user_role.as_deref(), Some("owner"));
        assert_eq!(full.action.as_deref(), Some("create"));
        let minimal: CommunityVoiceChatBody =
            serde_json::from_str(r#"{ "community_id": "c1", "user_address": "0xabc" }"#).unwrap();
        assert!(
            minimal.user_role.is_none()
                && minimal.action.is_none()
                && minimal.profile_data.is_none()
        );
        let mute: MuteSpeakerBody = serde_json::from_str(r#"{ "muted": true }"#).unwrap();
        assert!(mute.muted);
        let bulk: BulkCommunityStatusBody =
            serde_json::from_str(r#"{ "community_ids": ["a", "b"] }"#).unwrap();
        assert_eq!(bulk.community_ids, vec!["a", "b"]);
    }

    #[test]
    fn bulk_community_ids_bounded_at_100() {
        let at_limit: Vec<String> = (0..100).map(|i| format!("c{i}")).collect();
        assert!(validate_bulk_community_ids(&at_limit).is_ok());
        let over: Vec<String> = (0..101).map(|i| format!("c{i}")).collect();
        assert_eq!(validate_bulk_community_ids(&over).unwrap_err().code, 400);
    }

    #[test]
    fn the_claimed_role_that_grants_speaker_rights_is_exactly_owner_or_moderator() {
        assert!(claimed_role_grants_speaker_rights("owner"));
        assert!(claimed_role_grants_speaker_rights("moderator"));
        for rejected in [
            "member", "none", "mod", "admin", "Owner", "OWNER", "banned", "",
        ] {
            assert!(
                !claimed_role_grants_speaker_rights(rejected),
                "{rejected:?}"
            );
        }
    }

    #[test]
    fn community_join_metadata_shapes() {
        let md = community_join_metadata("owner", true, None);
        assert_eq!(md["role"], "owner");
        assert_eq!(md["isSpeaker"], true);
        assert_eq!(md["muted"], false);
        assert_eq!(md.len(), 3);
        let profile = serde_json::json!({ "name": "Foo", "has_claimed_name": true, "profile_picture_url": "http://x/y.png" });
        let md = community_join_metadata("member", false, Some(&profile));
        assert_eq!(md["isSpeaker"], false);
        assert_eq!(md["name"], "Foo");
        assert_eq!(md["hasClaimedName"], true);
        assert_eq!(md["profilePictureUrl"], "http://x/y.png");
        let md =
            community_join_metadata("member", false, Some(&serde_json::json!({ "name": "Foo" })));
        assert!(!md.contains_key("hasClaimedName") && !md.contains_key("profilePictureUrl"));
    }

    #[test]
    fn the_path_parameter_shape_check_keeps_its_messages_order_and_lowercasing() {
        let id = CommunityId::new().to_string();
        assert_eq!(
            community_and_claimed_target("  ", "0xAbC")
                .unwrap_err()
                .code,
            400
        );
        assert_eq!(
            community_and_claimed_target("not-a-uuid", "0xAbC")
                .unwrap_err()
                .code,
            400
        );
        assert_eq!(
            community_and_claimed_target(&id, "   ").unwrap_err().code,
            400
        );
        assert_eq!(
            community_and_claimed_target(&id, "0xAbC").unwrap().1,
            "0xabc"
        );
    }

    #[test]
    fn disconnect_reason_parses_numeric_and_proto3_json_names() {
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!(1))),
            DisconnectReason::ClientInitiated
        );
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!(2))),
            DisconnectReason::DuplicateIdentity
        );
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!(5))),
            DisconnectReason::RoomDeleted
        );
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!(99))),
            DisconnectReason::Other(99)
        );
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!("CLIENT_INITIATED"))),
            DisconnectReason::ClientInitiated
        );
        assert_eq!(
            DisconnectReason::parse(Some(&serde_json::json!("ROOM_DELETED"))),
            DisconnectReason::RoomDeleted
        );
        assert_eq!(
            DisconnectReason::parse(None),
            DisconnectReason::UnknownReason
        );
        assert_eq!(
            DisconnectReason::parse(Some(&Value::Null)),
            DisconnectReason::UnknownReason
        );
    }

    #[test]
    fn community_streaming_ended_event_matches_dcl_schemas_shape() {
        let event = community_streaming_ended_event(
            "my-community",
            7,
            "community-streaming-ended-my-community-1718900000000",
            1_718_900_000_000,
        );
        assert_eq!(event["type"], "streaming");
        assert_eq!(event["subType"], "community-streaming-ended");
        assert_eq!(
            event["key"],
            "community-streaming-ended-my-community-1718900000000"
        );
        assert_eq!(event["timestamp"], 1_718_900_000_000i64);
        assert_eq!(event["metadata"]["communityId"], "my-community");
        assert_eq!(event["metadata"]["totalParticipants"], 7);
        assert_eq!(event["metadata"].as_object().unwrap().len(), 2);
    }

    #[test]
    fn stale_leave_rules() {
        assert!(is_stale_leave(Some("new"), Some("old"), 0, None));
        assert!(is_stale_leave(Some("new"), Some("old"), 0, Some(1_000_000)));
        assert!(!is_stale_leave(Some("s1"), Some("s1"), 0, None));
        assert!(!is_stale_leave(Some("s1"), Some("s1"), 0, Some(0)));
        assert!(is_stale_leave(None, Some("s"), 10_000, Some(5_000)));
        assert!(is_stale_leave(None, None, 10_000, Some(5_000)));
        assert!(!is_stale_leave(None, Some("s"), 5_500, Some(5_000)));
        assert!(!is_stale_leave(
            None,
            Some("s"),
            5_000 + STALE_LEAVE_SKEW_MS,
            Some(5_000)
        ));
        assert!(is_stale_leave(
            None,
            Some("s"),
            5_000 + STALE_LEAVE_SKEW_MS + 1,
            Some(5_000)
        ));
        assert!(!is_stale_leave(None, None, 999_999, None));
        assert!(!is_stale_leave(Some("s"), Some("s"), 10_000_000, Some(0)));
    }

    #[test]
    fn response_key_casing_matches_upstream() {
        let single = serde_json::to_value(CommunityVoiceChatStatusResponse {
            active: true,
            participant_count: 3,
            moderator_count: 1,
        })
        .unwrap();
        let obj = single.as_object().unwrap();
        assert_eq!(obj.len(), 3);
        assert!(obj.contains_key("participant_count") && obj.contains_key("moderator_count"));
        let status = serde_json::to_value(VoiceChatStatusResponse {
            is_user_in_voice_chat: false,
        })
        .unwrap();
        assert_eq!(status["is_user_in_voice_chat"], false);
    }
}
