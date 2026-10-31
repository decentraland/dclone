//! The wire rows of the community surface, serialised with the camelCase
//! keys unity-explorer reads. `NaiveDateTime` renders as
//! `2026-01-01T00:00:00` (the old shape, no zone suffix), except where a
//! field says [`iso_ms`].

use chrono::NaiveDateTime;
use serde::Serialize;
use uuid::Uuid;

use super::profiles::NameColor;

/// A UTC time as production writes it (`Date.toJSON`: milliseconds, then
/// `Z`; `2025-08-20T19:31:24.575Z`, `.000` on a whole second), the
/// microseconds cut as `pg` does when it reads the column into a `Date`:
/// [`as_of::wire`](crate::shared::as_of::wire)'s form, as a `with` adapter.
pub mod iso_ms {
    use chrono::NaiveDateTime;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(t: &NaiveDateTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&crate::shared::as_of::wire(t.and_utc()))
    }
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct VoiceChatStatus {
    #[serde(rename = "isActive")]
    pub is_active: bool,
    #[serde(rename = "participantCount")]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub participant_count: i64,
    #[serde(rename = "moderatorCount")]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub moderator_count: i64,
}

impl VoiceChatStatus {
    pub fn idle() -> Self {
        Self {
            is_active: false,
            participant_count: 0,
            moderator_count: 0,
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct CommunityListItem {
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub id: Uuid,
    pub name: String,
    pub description: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub privacy: String,
    pub active: bool,
    #[serde(rename = "membersCount")]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub members_count: i64,
    #[serde(rename = "isLive")]
    pub is_live: bool,
    #[serde(rename = "voiceChatStatus")]
    pub voice_chat_status: VoiceChatStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub role: Option<String>,
    #[serde(rename = "isBanned", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub is_banned: Option<bool>,
    #[serde(rename = "thumbnailUrl", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub thumbnail_url: Option<String>,
    #[serde(rename = "ownerName", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub owner_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional, type = "Array<unknown>"))]
    pub friends: Option<Vec<serde_json::Value>>,
    #[serde(skip)]
    #[cfg_attr(feature = "ts", ts(skip))]
    pub has_thumbnail: bool,
}

#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct CommunityDetail {
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub id: Uuid,
    pub name: String,
    pub description: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub privacy: String,
    pub active: bool,
    #[serde(rename = "membersCount")]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub members_count: i64,
    #[serde(rename = "isLive")]
    pub is_live: bool,
    #[serde(rename = "voiceChatStatus")]
    pub voice_chat_status: VoiceChatStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub role: Option<String>,
    #[serde(rename = "isBanned", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub is_banned: Option<bool>,
    #[serde(rename = "thumbnailUrl", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub thumbnail_url: Option<String>,
    #[serde(rename = "ownerName", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub owner_name: Option<String>,
    #[serde(skip)]
    #[cfg_attr(feature = "ts", ts(skip))]
    pub has_thumbnail: bool,
}

#[derive(Debug, Serialize)]
pub struct MemberCommunity {
    pub id: Uuid,
    pub name: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub role: String,
}

#[derive(Debug, Serialize)]
pub struct CommunitySearchResult {
    pub id: Uuid,
    pub name: String,
    #[serde(rename = "membersCount")]
    pub members_count: i64,
    pub privacy: &'static str,
}

#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct CommunityMember {
    #[serde(rename = "communityId")]
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub community_id: Uuid,
    #[serde(rename = "memberAddress")]
    pub member_address: String,
    pub role: String,
    #[serde(rename = "joinedAt", with = "iso_ms")]
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub joined_at: NaiveDateTime,
}

/// The v1 members row: profile fields and the friendship status flattened
/// beside the base row (byte for byte the old value-patched shape).
#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct CommunityMemberWire {
    #[serde(flatten)]
    pub base: CommunityMember,
    pub name: String,
    #[serde(rename = "profilePictureUrl")]
    pub profile_picture_url: String,
    #[serde(rename = "hasClaimedName")]
    pub has_claimed_name: bool,
    #[serde(rename = "nameColor", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(optional))]
    pub name_color: Option<NameColor>,
    #[serde(rename = "friendshipStatus")]
    pub friendship_status: i32,
}

/// The v2 row: address only, the client resolves profiles itself.
#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct CommunityMemberV2Wire {
    #[serde(flatten)]
    pub base: CommunityMember,
    #[serde(rename = "friendshipStatus")]
    pub friendship_status: i32,
}

#[derive(Debug, Serialize)]
pub struct CommunityRequest {
    pub id: Uuid,
    #[serde(rename = "communityId")]
    pub community_id: Uuid,
    #[serde(rename = "memberAddress")]
    pub member_address: String,
    pub status: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "createdAt")]
    pub created_at: NaiveDateTime,
    #[serde(rename = "updatedAt")]
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize)]
pub struct CommunityPost {
    pub id: Uuid,
    #[serde(rename = "communityId")]
    pub community_id: Uuid,
    #[serde(rename = "authorAddress")]
    pub author_address: String,
    pub content: String,
    #[serde(rename = "createdAt", with = "iso_ms")]
    pub created_at: NaiveDateTime,
    #[serde(rename = "likesCount")]
    pub likes_count: i64,
    #[serde(rename = "isLikedByUser")]
    pub liked_by_me: bool,
}

#[derive(Debug, Serialize)]
pub struct CommunityPlace {
    pub id: String,
    #[serde(rename = "addedBy")]
    pub added_by: String,
    #[serde(rename = "addedAt")]
    pub added_at: NaiveDateTime,
}

#[derive(Debug, Serialize)]
pub struct CommunityBan {
    #[serde(rename = "communityId")]
    pub community_id: Uuid,
    #[serde(rename = "memberAddress")]
    pub banned_address: String,
    #[serde(rename = "bannedBy")]
    pub banned_by: String,
    #[serde(rename = "bannedAt")]
    pub banned_at: NaiveDateTime,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CommunityInvite {
    pub id: Uuid,
    pub name: String,
}

/// A row of `GET /v1/moderation/communities` as upstream's
/// `getAllCommunitiesForModeration` selects it (`communities-db.ts` at
/// `fb3dcb8`), its `createdAt` as `Date.toJSON` writes it.
#[derive(Debug, Serialize)]
pub struct CommunityForModeration {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub privacy: String,
    pub active: bool,
    #[serde(rename = "membersCount")]
    pub members_count: i32,
    #[serde(rename = "createdAt", with = "iso_ms")]
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct ActiveCommunityVoiceChat {
    #[serde(rename = "communityId")]
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    pub community_id: Uuid,
    #[serde(rename = "communityName")]
    pub community_name: String,
    #[serde(rename = "communityImage")]
    pub community_image: Option<String>,
    #[serde(rename = "isMember")]
    pub is_member: bool,
    pub positions: Vec<String>,
    pub worlds: Vec<String>,
    #[serde(rename = "participantCount")]
    pub participant_count: i32,
    #[serde(rename = "moderatorCount")]
    pub moderator_count: i32,
}

#[derive(Debug, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct ActiveVoiceChatsData {
    #[serde(rename = "activeChats")]
    pub active_chats: Vec<ActiveCommunityVoiceChat>,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub total: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn day() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    #[test]
    fn member_serializes_with_unity_wire_keys() {
        let m = CommunityMember {
            community_id: Uuid::nil(),
            member_address: "0xabc".into(),
            role: "member".into(),
            joined_at: day(),
        };
        let v = serde_json::to_value(m).unwrap();
        let obj = v.as_object().unwrap();
        for key in ["communityId", "memberAddress", "role", "joinedAt"] {
            assert!(obj.contains_key(key), "member missing {key}");
        }
        assert_eq!(obj["joinedAt"], "2024-01-01T00:00:00.000Z");
    }

    #[test]
    fn post_serializes_with_unity_wire_keys() {
        let p = CommunityPost {
            id: Uuid::nil(),
            community_id: Uuid::nil(),
            author_address: "0xabc".into(),
            content: "hi".into(),
            created_at: day(),
            likes_count: 3,
            liked_by_me: true,
        };
        let v = serde_json::to_value(p).unwrap();
        let m = v.as_object().unwrap();
        for key in ["id", "communityId", "authorAddress", "content", "createdAt"] {
            assert!(m.contains_key(key), "post missing {key}");
        }
        assert_eq!(m["likesCount"], 3);
        assert!(m.get("type").is_none());
        assert_eq!(m["isLikedByUser"], true);
    }

    fn sample_member() -> CommunityMember {
        CommunityMember {
            community_id: Uuid::nil(),
            member_address: "0xabc0000000000000000000000000000000000001".to_string(),
            role: "member".to_string(),
            joined_at: day(),
        }
    }

    fn legacy_patched_row(
        profile: Option<(&str, &str, bool, Option<NameColor>)>,
        friendship_status: i32,
    ) -> serde_json::Value {
        let mut v = serde_json::to_value(sample_member()).unwrap();
        let obj = v.as_object_mut().unwrap();
        if let Some((name, pic, claimed, name_color)) = profile {
            obj.insert("name".into(), serde_json::Value::String(name.into()));
            obj.insert(
                "profilePictureUrl".into(),
                serde_json::Value::String(pic.into()),
            );
            obj.insert("hasClaimedName".into(), serde_json::Value::Bool(claimed));
            if let Some(nc) = &name_color {
                obj.insert("nameColor".into(), serde_json::to_value(nc).unwrap());
            }
        }
        obj.insert(
            "friendshipStatus".into(),
            serde_json::Value::Number(friendship_status.into()),
        );
        v
    }

    #[test]
    fn wire_rows_match_the_legacy_value_patching() {
        let nc =
            NameColor::from_profile(&serde_json::json!({ "r": 0.5, "g": 0.25, "b": 1 })).unwrap();
        let legacy = legacy_patched_row(
            Some((
                "Alice",
                "https://content/contents/xyz",
                true,
                Some(nc.clone()),
            )),
            3,
        );
        let wire = serde_json::to_value(CommunityMemberWire {
            base: sample_member(),
            name: "Alice".to_string(),
            profile_picture_url: "https://content/contents/xyz".to_string(),
            has_claimed_name: true,
            name_color: Some(nc),
            friendship_status: 3,
        })
        .unwrap();
        assert_eq!(legacy, wire);

        let legacy = legacy_patched_row(Some(("", "", false, None)), 7);
        let wire = serde_json::to_value(CommunityMemberWire {
            base: sample_member(),
            name: String::new(),
            profile_picture_url: String::new(),
            has_claimed_name: false,
            name_color: None,
            friendship_status: 7,
        })
        .unwrap();
        assert_eq!(legacy, wire);
        assert!(
            wire.get("nameColor").is_none(),
            "absent nameColor must not serialize (never null)"
        );

        let legacy = legacy_patched_row(None, 0);
        let wire = serde_json::to_value(CommunityMemberV2Wire {
            base: sample_member(),
            friendship_status: 0,
        })
        .unwrap();
        assert_eq!(legacy, wire);
    }
}
