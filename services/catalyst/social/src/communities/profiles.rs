//! Profile enrichment for member, request, ban and friend rows: name,
//! picture, claimed-name flag and name colour read from the active profile
//! entity in `content.deployments`. The picture is the face the lambdas'
//! snapshots name ([`FaceUrls`]), not `contents/<face256>`: sync and the
//! read-through never copy a profile's snapshot files. The old service reached the content
//! database through a second pool (`CONTENT_PG_CONNECTION_STRING`); it is
//! the same database here, so this is a read over the ONE pool with a
//! five-minute memo per address. A failed read serves placeholders, never an
//! error: enrichment is decoration, not authorization.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;

use crate::shared::cache::{TtlCache, cache_get, cache_put};

/// A profile's `nameColor` as production writes it: upstream passes any
/// truthy value through as stored (`src/logic/profiles.ts` at `fb3dcb8`,
/// `...(nameColor && { nameColor })`), and `JSON.stringify` writes each number
/// in JavaScript's form (`1` for a stored `1.0`, `0.6801473` as stored, see
/// [`js_number`]); the usual shape is `{r, g, b}` with `a` when the profile has
/// one (capture `social/community-members`). The type names that usual shape;
/// a stored value of any other truthy shape is passed through unchanged.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct NameColor(
    #[cfg_attr(
        feature = "ts",
        ts(type = "{ r: number, g: number, b: number, a?: number, }")
    )]
    serde_json::Value,
);

impl NameColor {
    /// The stored value when JavaScript reads it as truthy (anything but
    /// `null`, `false`, `0` and `""`), its numbers in `JSON.stringify`'s form.
    pub fn from_profile(value: &serde_json::Value) -> Option<Self> {
        use serde_json::Value;
        let truthy = match value {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64() != Some(0.0),
            Value::String(s) => !s.is_empty(),
            Value::Array(_) | Value::Object(_) => true,
        };
        truthy.then(|| Self(js_numbers(value)))
    }
}

/// An object writes `r`, `g`, `b` and `a` first, in the order a profile
/// stores them, then any other key; any other value as it is.
impl Serialize for NameColor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let serde_json::Value::Object(map) = &self.0 else {
            return self.0.serialize(serializer);
        };
        const FIRST: [&str; 4] = ["r", "g", "b", "a"];
        let mut out = serializer.serialize_map(Some(map.len()))?;
        for key in FIRST {
            if let Some(v) = map.get(key) {
                out.serialize_entry(key, v)?;
            }
        }
        for (k, v) in map.iter().filter(|(k, _)| !FIRST.contains(&k.as_str())) {
            out.serialize_entry(k, v)?;
        }
        out.end()
    }
}

/// `value` with every number in [`js_number`]'s form.
fn js_numbers(value: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => Value::Number(js_number(n)),
        Value::Array(items) => Value::Array(items.iter().map(js_numbers).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), js_numbers(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A JSON number as `JSON.stringify` writes it: a whole value within the
/// safe-integer range as an integer (`1.0` and `1.00` read `1`); any other
/// keeps its `f64`, whose shortest form is JavaScript's in that range.
fn js_number(n: &serde_json::Number) -> serde_json::Number {
    const SAFE: f64 = 9_007_199_254_740_992.0; // 2^53
    match n.as_f64() {
        Some(f) if n.is_f64() && f.fract() == 0.0 && f.abs() < SAFE => {
            serde_json::Number::from(f as i64)
        }
        _ => n.clone(),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileInfo {
    pub name: String,
    #[serde(rename = "profilePictureUrl")]
    pub profile_picture_url: String,
    #[serde(rename = "hasClaimedName")]
    pub has_claimed_name: bool,
    #[serde(rename = "nameColor", skip_serializing_if = "Option::is_none")]
    pub name_color: Option<NameColor>,
}

const CACHE_TTL: Duration = Duration::from_secs(300);

/// Each key once, in first-seen order: one set probe per key rather than a
/// `Vec::contains` scan of the kept ones, which made this shared primitive
/// quadratic in the key count.
pub(crate) fn first_seen<K: Eq + std::hash::Hash + Clone>(
    keys: impl ExactSizeIterator<Item = K>,
) -> Vec<K> {
    let mut seen = HashSet::with_capacity(keys.len());
    let mut wanted = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key.clone()) {
            wanted.push(key);
        }
    }
    wanted
}

fn dedup_lowercased(addresses: &[String]) -> Vec<String> {
    first_seen(addresses.iter().map(|a| a.to_lowercase()))
}

/// The key comparisons [`first_seen`] makes deduplicating `addresses`, lowercased:
/// one per repeat (and the odd hash collision) for a set probe, the kept count per
/// key for a scan.
#[cfg(test)]
pub(crate) fn first_seen_comparisons(addresses: &[String]) -> usize {
    thread_local! {
        static COMPARED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
    #[derive(Clone)]
    struct Counted(String);
    impl PartialEq for Counted {
        fn eq(&self, other: &Self) -> bool {
            COMPARED.set(COMPARED.get() + 1);
            self.0 == other.0
        }
    }
    impl Eq for Counted {}
    impl std::hash::Hash for Counted {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.0.hash(state);
        }
    }
    COMPARED.set(0);
    let kept = first_seen(addresses.iter().map(|a| Counted(a.to_lowercase())));
    assert_eq!(kept.len(), dedup_lowercased(addresses).len());
    COMPARED.get()
}

/// Where a profile's face is served, as the lambdas' snapshots name it
/// (`market::profiles::rewrite_snapshot_urls`): `<base>entities/<profile
/// entity id>/face.png` under `PROFILE_CDN_BASE_URL`, which serves every
/// profile, or under this node's `/profile-images/`, which serves an uploaded
/// face it holds and, when it `derives` (an origin or a renderer is
/// configured), the face of any profile.
#[derive(Clone, Debug, Default)]
pub struct FaceUrls {
    base: String,
    derives: bool,
}

impl FaceUrls {
    pub fn new(cdn: Option<&str>, profile_images: &str, derives: bool) -> Self {
        let base = cdn.unwrap_or(profile_images).trim_end_matches('/');
        Self {
            base: format!("{base}/"),
            derives: derives || cdn.is_some(),
        }
    }

    /// The picture of the profile entity `entity_id`; `None` when it uploaded
    /// no face and nothing here derives one.
    pub fn picture(&self, entity_id: &str, face256: Option<&str>) -> Option<String> {
        let uploaded = face256.is_some_and(|f| !f.is_empty());
        (uploaded || self.derives).then(|| format!("{}entities/{entity_id}/face.png", self.base))
    }
}

#[derive(Clone)]
pub struct ProfilesCache {
    pool: PgPool,
    faces: FaceUrls,
    cache: Arc<TtlCache<Option<ProfileInfo>>>,
}

impl ProfilesCache {
    pub fn new(pool: PgPool, faces: FaceUrls) -> Self {
        Self {
            pool,
            faces,
            cache: Arc::new(TtlCache::default()),
        }
    }

    /// Forgets every memoised profile, misses included, so a test's next
    /// read sees only the rows it seeded since.
    #[cfg(test)]
    pub(crate) fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub async fn get_profiles(&self, addresses: &[String]) -> HashMap<String, ProfileInfo> {
        let mut out: HashMap<String, ProfileInfo> = HashMap::new();
        if addresses.is_empty() {
            return out;
        }
        let wanted = dedup_lowercased(addresses);
        let mut misses: Vec<String> = Vec::new();
        for addr in &wanted {
            match cache_get(&self.cache, addr) {
                Some(Some(info)) => {
                    out.insert(addr.clone(), info);
                }
                Some(None) => {}
                None => misses.push(addr.clone()),
            }
        }
        if misses.is_empty() {
            return out;
        }

        let rows = sqlx::query_as::<
            _,
            (String, String, Option<String>, Option<String>, Option<bool>, Option<serde_json::Value>),
        >(
            "SELECT lower(d.entity_pointers[1]) AS addr, d.entity_id, \
                    COALESCE(d.entity_metadata::jsonb #>> '{v,avatars,0,name}', \
                             d.entity_metadata::jsonb #>> '{v,avatars,0,unclaimedName}') AS name, \
                    d.entity_metadata::jsonb #>> '{v,avatars,0,avatar,snapshots,face256}' AS face256, \
                    (d.entity_metadata::jsonb #>> '{v,avatars,0,hasClaimedName}')::bool AS has_claimed, \
                    d.entity_metadata::jsonb #> '{v,avatars,0,nameColor}' AS name_color \
             FROM content.deployments d \
             WHERE d.entity_type = 'profile' \
               AND d.deleter_deployment IS NULL \
               AND d.entity_pointers && $1::text[]",
        )
        .bind(&misses)
        .fetch_all(&self.pool)
        .await;

        let mut resolved: HashMap<String, ProfileInfo> = HashMap::new();
        match rows {
            Ok(rows) => {
                for (addr, entity_id, name, face256, has_claimed, color) in rows {
                    let name = match name {
                        Some(n) if !n.is_empty() => n,
                        _ => continue,
                    };
                    let Some(picture) = self.faces.picture(&entity_id, face256.as_deref()) else {
                        continue;
                    };
                    let name_color = color.as_ref().and_then(NameColor::from_profile);
                    resolved.insert(
                        addr,
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
                tracing::warn!(error = %e, "profile enrichment query failed; serving placeholders");
            }
        }
        for addr in misses {
            let info = resolved.get(&addr).cloned();
            if let Some(info) = &info {
                out.insert(addr.clone(), info.clone());
            }
            cache_put(&self.cache, addr, info, CACHE_TTL);
        }
        out
    }

    pub async fn get_owner_names(&self, addresses: &[String]) -> HashMap<String, String> {
        self.get_profiles(addresses)
            .await
            .into_iter()
            .map(|(addr, info)| (addr, info.name))
            .collect()
    }

    /// Whether the wallet's active profile claims a name. `None` when the
    /// answer could not be read (the caller then does not refuse on it, as
    /// the old service did not).
    pub async fn has_owned_name(&self, address: &str) -> Option<bool> {
        let addr = address.to_lowercase();
        let row: Option<(bool,)> = sqlx::query_as(
            "SELECT COALESCE((d.entity_metadata::jsonb #>> '{v,avatars,0,hasClaimedName}')::bool, false) \
             FROM content.deployments d \
             WHERE d.entity_type = 'profile' \
               AND d.deleter_deployment IS NULL \
               AND d.entity_pointers && ARRAY[$1]::text[] \
             LIMIT 1",
        )
        .bind(&addr)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        Some(row.map(|(c,)| c).unwrap_or(false))
    }
}

/// `name`, `profilePictureUrl`, `hasClaimedName` (and `nameColor` when known)
/// on every row whose `addr_field` resolved; placeholders otherwise.
pub async fn enrich_with_profiles(
    profiles: &ProfilesCache,
    rows: &mut [serde_json::Value],
    addr_field: &str,
) {
    if rows.is_empty() {
        return;
    }
    let addresses: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            r.get(addr_field)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    let map = profiles.get_profiles(&addresses).await;
    for row in rows.iter_mut() {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };
        let addr = obj
            .get(addr_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        let info = map.get(&addr);
        let (name, pic, claimed) = match info {
            Some(info) => (
                info.name.clone(),
                info.profile_picture_url.clone(),
                info.has_claimed_name,
            ),
            None => (String::new(), String::new(), false),
        };
        obj.insert("name".to_string(), serde_json::Value::String(name));
        obj.insert(
            "profilePictureUrl".to_string(),
            serde_json::Value::String(pic),
        );
        obj.insert(
            "hasClaimedName".to_string(),
            serde_json::Value::Bool(claimed),
        );
        if let Some(nc) = info.and_then(|i| i.name_color.as_ref())
            && let Ok(v) = serde_json::to_value(nc)
        {
            obj.insert("nameColor".to_string(), v);
        }
    }
}

/// The post author fields; an unresolved author falls back to the address
/// as its name (the old shape).
pub async fn enrich_posts_with_authors(
    profiles: &ProfilesCache,
    rows: &mut [serde_json::Value],
    addr_field: &str,
) {
    if rows.is_empty() {
        return;
    }
    let addresses: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            r.get(addr_field)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    let map = profiles.get_profiles(&addresses).await;
    for row in rows.iter_mut() {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };
        let addr = obj
            .get(addr_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        let (name, pic, claimed) = match map.get(&addr) {
            Some(info) => (
                info.name.clone(),
                info.profile_picture_url.clone(),
                info.has_claimed_name,
            ),
            None => (addr.clone(), String::new(), false),
        };
        obj.insert("authorName".to_string(), serde_json::Value::String(name));
        obj.insert(
            "authorProfilePictureUrl".to_string(),
            serde_json::Value::String(pic),
        );
        obj.insert(
            "authorHasClaimedName".to_string(),
            serde_json::Value::Bool(claimed),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What `JSON.stringify` writes for a profile's colour numbers.
    #[test]
    fn a_name_color_is_written_as_javascript_writes_it() {
        let written = |raw: &str| {
            let value: serde_json::Value = serde_json::from_str(raw).unwrap();
            serde_json::to_string(&NameColor::from_profile(&value).unwrap()).unwrap()
        };
        assert_eq!(
            written(r#"{"r":1.0,"g":1.50,"b":0.6801473,"a":1}"#),
            r#"{"r":1,"g":1.5,"b":0.6801473,"a":1}"#
        );
        assert_eq!(
            written(r#"{"r":0.6801472902297974,"g":-0.0,"b":0}"#),
            r#"{"r":0.6801472902297974,"g":0,"b":0}"#
        );
        // any truthy value passes through as stored; a falsy one is no colour
        assert_eq!(
            written(r#"{"r":1,"g":"1","b":1.0,"x":[2.0]}"#),
            r#"{"r":1,"g":"1","b":1,"x":[2]}"#
        );
        assert_eq!(written(r#""red""#), r#""red""#);
        for falsy in ["null", "false", "0", "0.0", r#""""#] {
            let value: serde_json::Value = serde_json::from_str(falsy).unwrap();
            assert!(NameColor::from_profile(&value).is_none(), "{falsy}");
        }
    }

    #[test]
    fn dedup_lowercased_preserves_first_seen_order() {
        let mixed: Vec<String> = ["0xAbC", "0xabc", "0xDEF", "0xdef", "0xABC", "0xghi"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(dedup_lowercased(&mixed), vec!["0xabc", "0xdef", "0xghi"]);
        assert!(dedup_lowercased(&[]).is_empty());
        let dups: Vec<String> = vec!["0xZ".to_string(); 8];
        assert_eq!(dedup_lowercased(&dups), vec!["0xz"]);
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
        assert_eq!(dedup_lowercased(&addrs).len(), 5_000);
        let compared = first_seen_comparisons(&addrs);
        assert!(
            compared < 2 * addrs.len(),
            "{compared} key comparisons for {} addresses: the kept keys are scanned, not probed",
            addrs.len()
        );
    }

    #[test]
    fn faces_are_the_profile_images_route_or_the_cdn_never_contents() {
        let node = FaceUrls::new(None, "https://node/profile-images/", false);
        assert_eq!(
            node.picture("bafyprofile", Some("bafyface")).as_deref(),
            Some("https://node/profile-images/entities/bafyprofile/face.png")
        );
        assert_eq!(node.picture("bafyprofile", None), None);
        assert_eq!(node.picture("bafyprofile", Some("")), None);
        let deriving = FaceUrls::new(None, "https://node/profile-images", true);
        assert_eq!(
            deriving.picture("bafyprofile", None).as_deref(),
            Some("https://node/profile-images/entities/bafyprofile/face.png")
        );
        let cdn = FaceUrls::new(Some("https://cdn/"), "https://node/profile-images/", false);
        assert_eq!(
            cdn.picture("bafyprofile", None).as_deref(),
            Some("https://cdn/entities/bafyprofile/face.png")
        );
    }

    #[tokio::test]
    async fn member_rows_get_all_profile_fields_and_posts_get_author_fields() {
        let Some(pool) = crate::db::scratch::pool(2).await else {
            return;
        };
        let profiles = ProfilesCache::new(
            pool,
            FaceUrls::new(None, "https://node/profile-images/", false),
        );
        let mut rows = vec![serde_json::json!({
            "communityId": "c1",
            "memberAddress": "0x00000000000000000000000000000000000000ab",
            "role": "member",
            "joinedAt": "2024-01-01T00:00:00",
        })];
        enrich_with_profiles(&profiles, &mut rows, "memberAddress").await;
        let m = rows[0].as_object().unwrap();
        for key in [
            "memberAddress",
            "name",
            "profilePictureUrl",
            "hasClaimedName",
        ] {
            assert!(m.contains_key(key), "member missing {key}");
        }
        assert_eq!(m["name"], "");
        assert_eq!(m["profilePictureUrl"], "");
        assert_eq!(m["hasClaimedName"], false);
        assert!(
            !m.contains_key("nameColor"),
            "unresolved member must not carry a nameColor key"
        );

        let mut rows =
            vec![serde_json::json!({ "id": "p1", "authorAddress": "0xABC", "content": "hi" })];
        enrich_posts_with_authors(&profiles, &mut rows, "authorAddress").await;
        let m = rows[0].as_object().unwrap();
        for key in [
            "authorName",
            "authorProfilePictureUrl",
            "authorHasClaimedName",
        ] {
            assert!(m.contains_key(key), "post missing {key}");
        }
        assert_eq!(m["authorName"], "0xabc");
    }
}
