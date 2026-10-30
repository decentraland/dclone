//! The name, claimed-name flag, face picture and name colour that decorate
//! every friend on the wire, read from the active profile entity in
//! `content.deployments` (the same database now, so the old side pool to the
//! content server is gone) and cached for five minutes. The picture is the
//! face the lambdas' snapshots name (`communities::profiles::FaceUrls`). A
//! wallet with no profile, or one whose profile has no name, or no face that
//! can be served, is the placeholder the old service sent (empty name and
//! picture, no colour); the RPC never fails on enrichment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::communities::profiles::FaceUrls;
use crate::rpc::proto::common::Color3;
use crate::rpc::proto::v2::{BlockedUserProfile, FriendProfile};
use crate::shared::cache::TtlMap;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NameColor {
    pub r: f32,
    pub g: f32,
    pub b: f32,
}

impl From<NameColor> for Color3 {
    fn from(c: NameColor) -> Self {
        Color3 {
            r: c.r,
            g: c.g,
            b: c.b,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProfileInfo {
    pub name: String,
    pub profile_picture_url: String,
    pub has_claimed_name: bool,
    pub name_color: Option<NameColor>,
}

const CACHE_TTL: Duration = Duration::from_secs(300);
const CACHE_MAX_ENTRIES: usize = 100_000;

/// Preserves first-seen order without a quadratic scan, so a 10^3-friend
/// page stays linear.
fn dedup_lowercased(addresses: &[String]) -> Vec<String> {
    crate::communities::profiles::first_seen(addresses.iter().map(|a| a.to_lowercase()))
}

/// `active_pointers` names the profile entity of each wallet (the pointer is
/// the lowercase address); the metadata read is one `= ANY` over the
/// `deployments_entity_id_key` probe of those ids. The column wraps the
/// deployed metadata as `{"v": ...}` (content::queries).
const PROFILE_SQL: &str = "\
    SELECT ap.pointer AS addr, ap.entity_id, \
           COALESCE(d.entity_metadata::jsonb #>> '{v,avatars,0,name}', \
                    d.entity_metadata::jsonb #>> '{v,avatars,0,unclaimedName}') AS name, \
           d.entity_metadata::jsonb #>> '{v,avatars,0,avatar,snapshots,face256}' AS face256, \
           (d.entity_metadata::jsonb #>> '{v,avatars,0,hasClaimedName}')::bool AS has_claimed, \
           (d.entity_metadata::jsonb #>> '{v,avatars,0,nameColor,r}')::float8 AS color_r, \
           (d.entity_metadata::jsonb #>> '{v,avatars,0,nameColor,g}')::float8 AS color_g, \
           (d.entity_metadata::jsonb #>> '{v,avatars,0,nameColor,b}')::float8 AS color_b \
    FROM content.active_pointers ap \
    JOIN content.deployments d ON d.entity_id = ap.entity_id \
    WHERE ap.pointer = ANY($1::text[]) AND d.entity_type = 'profile'";

type ProfileRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

#[derive(Clone)]
pub struct Profiles {
    pool: Option<PgPool>,
    faces: FaceUrls,
    cache: Arc<TtlMap<String, Option<ProfileInfo>>>,
}

impl Profiles {
    /// A `None` pool serves placeholders only (the old service without
    /// `CONTENT_PG_CONNECTION_STRING`).
    pub fn new(pool: Option<PgPool>, faces: FaceUrls) -> Self {
        Self {
            pool,
            faces,
            cache: Arc::new(TtlMap::bounded(CACHE_TTL, CACHE_MAX_ENTRIES)),
        }
    }

    pub async fn get_profiles(&self, addresses: &[String]) -> HashMap<String, ProfileInfo> {
        self.get_profiles_result(addresses)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "profile enrichment query failed; serving placeholders");
                addresses
                    .iter()
                    .filter_map(|address| {
                        self.cache
                            .get_fresh(&address.to_lowercase())
                            .flatten()
                            .map(|profile| (address.to_lowercase(), profile))
                    })
                    .collect()
            })
    }

    async fn get_profiles_result(
        &self,
        addresses: &[String],
    ) -> Result<HashMap<String, ProfileInfo>, sqlx::Error> {
        let mut out: HashMap<String, ProfileInfo> = HashMap::new();
        if addresses.is_empty() {
            return Ok(out);
        }
        let wanted = dedup_lowercased(addresses);
        let mut misses: Vec<String> = Vec::new();
        for addr in &wanted {
            match self.cache.get_fresh(addr) {
                Some(Some(info)) => {
                    out.insert(addr.clone(), info);
                }
                Some(None) => {}
                None => misses.push(addr.clone()),
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }
        let Some(pool) = &self.pool else {
            for addr in misses {
                self.cache.insert(addr, None);
            }
            return Ok(out);
        };

        let mut resolved: HashMap<String, ProfileInfo> = HashMap::new();
        match sqlx::query_as::<_, ProfileRow>(PROFILE_SQL)
            .bind(&misses)
            .fetch_all(pool)
            .await
        {
            Ok(rows) => {
                for (addr, entity_id, name, face256, has_claimed, cr, cg, cb) in rows {
                    let name = match name {
                        Some(n) if !n.is_empty() => n,
                        _ => continue,
                    };
                    let Some(picture) = self.faces.picture(&entity_id, face256.as_deref()) else {
                        continue;
                    };
                    let name_color = match (cr, cg, cb) {
                        (Some(r), Some(g), Some(b)) => Some(NameColor {
                            r: r as f32,
                            g: g as f32,
                            b: b as f32,
                        }),
                        _ => None,
                    };
                    resolved.insert(
                        addr.to_lowercase(),
                        ProfileInfo {
                            name,
                            profile_picture_url: picture,
                            has_claimed_name: has_claimed.unwrap_or(false),
                            name_color,
                        },
                    );
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
        for addr in misses {
            let info = resolved.get(&addr).cloned();
            if let Some(info) = &info {
                out.insert(addr.clone(), info.clone());
            }
            self.cache.insert(addr, info);
        }
        Ok(out)
    }

    pub async fn get_profile(&self, address: &str) -> Option<ProfileInfo> {
        self.get_profiles(std::slice::from_ref(&address.to_string()))
            .await
            .remove(&address.to_lowercase())
    }

    pub async fn friend_profile(&self, address: &str) -> FriendProfile {
        friend_profile(address, self.get_profile(address).await.as_ref())
    }

    /// One profile per input address, in input order.
    pub async fn friend_profiles(&self, addresses: &[String]) -> Vec<FriendProfile> {
        let map = self.get_profiles(addresses).await;
        addresses
            .iter()
            .map(|a| friend_profile(a, map.get(&a.to_lowercase())))
            .collect()
    }

    /// A strict bounded initial replay batch; read failure retires this stream rather
    /// than certifying initialization. Missing profiles retain the established representation.
    pub async fn connectivity_profiles(
        &self,
        addresses: &[String],
    ) -> Result<Vec<FriendProfile>, sqlx::Error> {
        let map = self.get_profiles_result(addresses).await?;
        Ok(addresses
            .iter()
            .map(|address| {
                connectivity_profile(friend_profile(address, map.get(&address.to_lowercase())))
            })
            .collect())
    }

    pub async fn blocked_profile(
        &self,
        address: &str,
        blocked_at_ms: Option<i64>,
    ) -> BlockedUserProfile {
        blocked_profile(
            address,
            self.get_profile(address).await.as_ref(),
            blocked_at_ms,
        )
    }
}

/// Reuse the existing enrichment-unavailable profile when optional display metadata
/// cannot fit connectivity's item budget. Identity/status/authorization are never trimmed.
pub(crate) fn connectivity_profile(profile: FriendProfile) -> FriendProfile {
    use prost::Message as _;
    if profile.encoded_len() > crate::rpc::drpc::transport::MAX_GUARDED_MESSAGE_BYTES - 512 {
        tracing::warn!(address = %profile.address, "connectivity enrichment exceeds item budget; using existing unavailable-profile representation");
        friend_profile(&profile.address, None)
    } else {
        profile
    }
}

pub fn friend_profile(address: &str, info: Option<&ProfileInfo>) -> FriendProfile {
    match info {
        Some(info) => FriendProfile {
            address: address.to_string(),
            name: info.name.clone(),
            has_claimed_name: info.has_claimed_name,
            profile_picture_url: info.profile_picture_url.clone(),
            name_color: info.name_color.map(Into::into),
        },
        None => FriendProfile {
            address: address.to_string(),
            name: String::new(),
            has_claimed_name: false,
            profile_picture_url: String::new(),
            name_color: None,
        },
    }
}

pub fn blocked_profile(
    address: &str,
    info: Option<&ProfileInfo>,
    blocked_at_ms: Option<i64>,
) -> BlockedUserProfile {
    match info {
        Some(info) => BlockedUserProfile {
            address: address.to_string(),
            name: info.name.clone(),
            has_claimed_name: info.has_claimed_name,
            profile_picture_url: info.profile_picture_url.clone(),
            blocked_at: blocked_at_ms,
            name_color: info.name_color.map(Into::into),
        },
        None => BlockedUserProfile {
            address: address.to_string(),
            name: String::new(),
            has_claimed_name: false,
            profile_picture_url: String::new(),
            blocked_at: blocked_at_ms,
            name_color: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(addrs: &[String]) -> Vec<String> {
        let mut w = Vec::new();
        for a in addrs {
            let lc = a.to_lowercase();
            if !w.contains(&lc) {
                w.push(lc);
            }
        }
        w
    }

    #[test]
    fn dedup_lowercased_preserves_first_seen_order_and_matches_reference() {
        let mixed: Vec<String> = ["0xAbC", "0xabc", "0xDEF", "0xdef", "0xABC", "0xghi"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(dedup_lowercased(&mixed), vec!["0xabc", "0xdef", "0xghi"]);
        assert_eq!(dedup_lowercased(&mixed), reference(&mixed));
        assert_eq!(dedup_lowercased(&[]), reference(&[]));
        let dups: Vec<String> = vec!["0xZ".to_string(); 8];
        assert_eq!(dedup_lowercased(&dups), reference(&dups));
    }

    #[test]
    fn dedup_of_twenty_thousand_addresses_is_not_quadratic() {
        let addrs: Vec<String> = (0..20_000)
            .map(|i| {
                let v = i % 5_000;
                if i % 2 == 0 {
                    format!("0xADDR{v}")
                } else {
                    format!("0xaddr{v}")
                }
            })
            .collect();
        let kept = dedup_lowercased(&addrs);
        assert_eq!(kept.len(), 5_000);
        let shared =
            crate::communities::profiles::first_seen(addrs.iter().map(|a| a.to_lowercase()));
        assert_eq!(kept, shared, "the dedup is the shared first-seen pass");
        let compared = crate::communities::profiles::first_seen_comparisons(&addrs);
        assert!(
            compared < 2 * addrs.len(),
            "{compared} key comparisons for {} addresses: the kept keys are scanned, not probed",
            addrs.len()
        );
    }

    #[tokio::test]
    async fn without_a_pool_every_address_is_a_placeholder_in_input_order() {
        let profiles = Profiles::new(None, FaceUrls::default());
        let list = profiles
            .friend_profiles(&["0xB".to_string(), "0xa".to_string(), "0xb".to_string()])
            .await;
        let addrs: Vec<&str> = list.iter().map(|p| p.address.as_str()).collect();
        assert_eq!(addrs, ["0xB", "0xa", "0xb"]);
        assert!(list.iter().all(|p| p.name.is_empty()
            && p.profile_picture_url.is_empty()
            && p.name_color.is_none()));
        let blocked = profiles.blocked_profile("0xa", Some(7)).await;
        assert_eq!(
            (blocked.address.as_str(), blocked.blocked_at),
            ("0xa", Some(7))
        );
    }

    async fn deploy_profile(
        pool: &PgPool,
        addr: &str,
        entity_id: &str,
        metadata: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO content.deployments (deployer_address, version, entity_type, entity_id, entity_metadata, \
             entity_timestamp, entity_pointers, local_timestamp, auth_chain) \
             VALUES ($1, 'v3', 'profile', $2, $3, now(), ARRAY[$1], now(), '[]'::json)",
        )
        .bind(addr)
        .bind(entity_id)
        .bind(&metadata)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO content.active_pointers (pointer, entity_id, entity_type) VALUES ($1, $2, 'profile')")
            .bind(addr)
            .bind(entity_id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// The active profile entity of a wallet decorates it, its picture the
    /// `/profile-images` face; a wallet without one is a placeholder; the
    /// second read of both comes from the cache.
    #[tokio::test]
    async fn profiles_come_from_the_active_profile_entity() {
        let Some(pool) = crate::db::scratch::pool(2).await else {
            return;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let addr = format!("0x{}", &format!("{tag}{tag}")[..40]);
        let entity_id = format!("bafkreiprofile{tag}");
        let metadata = serde_json::json!({"v": {"avatars": [{"name": "Alice", "hasClaimedName": true,
            "nameColor": {"r": 0.5, "g": 0.25, "b": 1.0},
            "avatar": {"snapshots": {"face256": "bafkreiface"}}}]}});
        deploy_profile(&pool, &addr, &entity_id, metadata).await;
        let faceless = format!("0x{}", &format!("{tag}{tag}")[2..42]);
        let faceless_entity = format!("bafkreifaceless{tag}");
        let metadata = serde_json::json!({"v": {"avatars": [{"name": "Bob", "avatar": {}}]}});
        deploy_profile(&pool, &faceless, &faceless_entity, metadata).await;

        let profiles = Profiles::new(
            Some(pool.clone()),
            FaceUrls::new(None, "https://peer.test/profile-images/", false),
        );
        let other = format!("0x{}", &format!("{tag}{tag}")[1..41]);
        let list = profiles
            .friend_profiles(&[addr.to_uppercase(), other.clone(), faceless.clone()])
            .await;
        assert_eq!(list[0].address, addr.to_uppercase());
        assert_eq!(list[0].name, "Alice");
        assert!(list[0].has_claimed_name);
        assert_eq!(
            list[0].profile_picture_url,
            format!("https://peer.test/profile-images/entities/{entity_id}/face.png")
        );
        assert_eq!(
            list[0].name_color,
            Some(Color3 {
                r: 0.5,
                g: 0.25,
                b: 1.0
            })
        );
        assert_eq!(list[1].address, other);
        assert!(list[1].name.is_empty());
        assert!(
            list[2].name.is_empty() && list[2].profile_picture_url.is_empty(),
            "no face uploaded and none derived: a placeholder"
        );

        let deriving = Profiles::new(
            Some(pool.clone()),
            FaceUrls::new(None, "https://peer.test/profile-images", true),
        );
        let bob = deriving.friend_profile(&faceless).await;
        assert_eq!(bob.name, "Bob");
        assert!(
            !bob.has_claimed_name,
            "an absent hasClaimedName reads as false"
        );
        assert_eq!(
            bob.profile_picture_url,
            format!("https://peer.test/profile-images/entities/{faceless_entity}/face.png"),
            "the route renders or fetches a face the profile never uploaded"
        );
        let cdn = Profiles::new(
            Some(pool.clone()),
            FaceUrls::new(
                Some("https://cdn.test"),
                "https://peer.test/profile-images/",
                false,
            ),
        );
        assert_eq!(
            cdn.friend_profile(&faceless).await.profile_picture_url,
            format!("https://cdn.test/entities/{faceless_entity}/face.png")
        );

        sqlx::query("DELETE FROM content.active_pointers WHERE pointer = $1")
            .bind(&faceless)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM content.deployments WHERE entity_id = $1")
            .bind(&faceless_entity)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM content.active_pointers WHERE pointer = $1")
            .bind(&addr)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM content.deployments WHERE entity_id = $1")
            .bind(&entity_id)
            .execute(&pool)
            .await
            .unwrap();
        let cached = profiles.friend_profile(&addr).await;
        assert_eq!(
            cached.name, "Alice",
            "served from the cache after the row is gone"
        );
    }

    /// Upstream's `profiles.spec.ts` (`decentraland/social-service-ea` at
    /// `fb3dcb8`, `src/logic/profiles.ts:9`, `:19-21`): the first avatar is the
    /// profile, and a name falls back to `unclaimedName`. Recorded difference
    /// (RPC.md, profiles.spec.ts, "The name falls back to unclaimedName"): an
    /// empty `name` beside an `unclaimedName` is a placeholder here, where
    /// upstream's `name || unclaimedName` takes the unclaimed one.
    #[tokio::test]
    async fn the_first_avatar_names_the_profile_and_falls_back_to_unclaimed_name() {
        let Some(pool) = crate::db::scratch::pool(2).await else {
            return;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let wallet = |i: usize| format!("0x{}", &format!("{tag}{tag}")[i..i + 40]);
        let face = serde_json::json!({"snapshots": {"face256": "bafkreiface"}});
        let cases = [
            (
                wallet(3),
                serde_json::json!({"v": {"avatars": [
                    {"name": "First", "avatar": face},
                    {"name": "Second", "avatar": face}]}}),
            ),
            (
                wallet(4),
                serde_json::json!({"v": {"avatars": [{"unclaimedName": "Unclaimed", "avatar": face}]}}),
            ),
            (
                wallet(5),
                serde_json::json!({"v": {"avatars": [{"name": "", "unclaimedName": "Unclaimed", "avatar": face}]}}),
            ),
        ];
        let mut entities = Vec::new();
        for (i, (addr, metadata)) in cases.iter().enumerate() {
            let entity_id = format!("bafkreiavatars{i}{tag}");
            deploy_profile(&pool, addr, &entity_id, metadata.clone()).await;
            entities.push(entity_id);
        }
        let profiles = Profiles::new(
            Some(pool.clone()),
            FaceUrls::new(None, "https://peer.test/profile-images/", false),
        );
        let addrs: Vec<String> = cases.iter().map(|(a, _)| a.clone()).collect();
        let list = profiles.friend_profiles(&addrs).await;
        assert_eq!(list[0].name, "First");
        assert_eq!(list[1].name, "Unclaimed");
        assert!(!list[1].has_claimed_name);
        assert!(
            list[2].name.is_empty() && list[2].profile_picture_url.is_empty(),
            "an empty name is a placeholder: {:?}",
            list[2]
        );
        for (addr, entity_id) in addrs.iter().zip(&entities) {
            sqlx::query("DELETE FROM content.active_pointers WHERE pointer = $1")
                .bind(addr)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM content.deployments WHERE entity_id = $1")
                .bind(entity_id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
}
