//! A fixture's recorded response against a candidate's: the request's query
//! string, the status and recorded headers, and the body. The live replay
//! (`tests/support/conformance/replay.rs`) and the in-process replays share
//! these rules; each words its own notes.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::Value;

use super::diff::{Difference, compare_json};
use super::fixture::{Fixture, VOLATILE_HEADERS, apply_volatile_paths, media_type};
use super::volatility::Volatility;

/// The query string of a fixture (`k=v` pairs, URL-encoded, a valueless key
/// as `k`), with an optional rewrite of every value; `None` when it has none.
pub fn query_string(
    query: &BTreeMap<String, Vec<String>>,
    rewrite: Option<&dyn Fn(&str) -> String>,
) -> Option<String> {
    if query.is_empty() {
        return None;
    }
    let pairs: Vec<String> = query
        .iter()
        .flat_map(|(k, vs)| vs.iter().map(move |v| (k.as_str(), v.as_str())))
        .map(|(k, v)| {
            let v = rewrite.map(|f| f(v)).unwrap_or_else(|| v.to_string());
            if v.is_empty() {
                urlencoding::encode(k).into_owned()
            } else {
                format!("{}={}", urlencoding::encode(k), urlencoding::encode(&v))
            }
        })
        .collect();
    Some(pairs.join("&"))
}

/// The status and every non-volatile header the fixture records against the
/// candidate's (`header` looks one up by name): `content-type` compares by
/// media type, any other header trimmed.
pub fn status_and_header_notes(
    fx: &Fixture,
    status: u16,
    header: &dyn Fn(&str) -> Option<String>,
) -> Vec<String> {
    let mut notes = Vec::new();
    if status != fx.response.status {
        notes.push(format!(
            "status: fixture={} candidate={status}",
            fx.response.status
        ));
    }
    for (key, expected) in &fx.response.headers {
        let key_lc = key.to_ascii_lowercase();
        if VOLATILE_HEADERS.contains(&key_lc.as_str()) {
            continue;
        }
        let Some(actual) = header(key) else {
            notes.push(format!(
                "header {key}: fixture={expected:?} candidate=<missing>"
            ));
            continue;
        };
        let same = if key_lc == "content-type" {
            media_type(expected) == media_type(&actual)
        } else {
            expected.trim() == actual.trim()
        };
        if !same {
            notes.push(format!(
                "header {key}: fixture={expected:?} candidate={actual:?}"
            ));
        }
    }
    notes
}

/// How a candidate's body compares with the fixture's.
pub enum Body {
    /// A byte fixture: the recorded bytes, and whether the candidate's match.
    Bytes { expected: Vec<u8>, same: bool },
    /// A JSON fixture: the differences after both sides' volatile paths.
    Json(Vec<Difference>),
    /// A JSON fixture whose candidate body does not parse.
    NotJson(String),
    /// The fixture's own body is unusable (bad base64, or no body kind).
    BadFixture(String),
}

/// `bytes` against the fixture's body; `section` names the diff's paths.
pub fn compare_body(fx: &Fixture, section: &str, bytes: &[u8], vol: &Volatility) -> Body {
    if let Some(b64) = &fx.response.body_bytes_b64 {
        return match B64.decode(b64.as_bytes()) {
            Ok(expected) => {
                let same = expected.as_slice() == bytes;
                Body::Bytes { expected, same }
            }
            Err(e) => Body::BadFixture(format!("fixture body_bytes_b64 not valid base64: {e}")),
        };
    }
    let Some(expected) = &fx.response.body_json else {
        return Body::BadFixture("fixture has neither body_json nor body_bytes_b64".to_string());
    };
    let mut candidate = match serde_json::from_slice::<Value>(bytes) {
        Ok(v) => v,
        Err(e) => return Body::NotJson(e.to_string()),
    };
    let mut baseline = expected.clone();
    apply_volatile_paths(&mut baseline, &fx.volatile_paths);
    apply_volatile_paths(&mut candidate, &fx.volatile_paths);
    Body::Json(compare_json(section, "fixture", &baseline, &candidate, vol))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_is_encoded_and_a_valueless_key_goes_bare() {
        let query = BTreeMap::from([
            (
                "search".to_string(),
                vec!["Decentraland Foundation".to_string()],
            ),
            ("roles[]".to_string(), vec!["owner".to_string()]),
            ("onlyUnread".to_string(), vec![String::new()]),
        ]);
        assert_eq!(
            query_string(&query, None).unwrap(),
            "onlyUnread&roles%5B%5D=owner&search=Decentraland%20Foundation"
        );
        assert_eq!(query_string(&BTreeMap::new(), None), None);
    }
}
