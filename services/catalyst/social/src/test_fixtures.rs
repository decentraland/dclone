//! The social family's captures (`tests/fixtures/conformance/social/`)
//! replayed against an in-process router with the live replay's rules, the
//! ones `tests/support/conformance/replay.rs` calls too
//! (`catalyst_testing::conformance::compare`): the query string, the status,
//! the non-volatile headers the fixture records (`content-type` by media
//! type), then the body, exact bytes or JSON after both sides' volatile paths.

use std::path::PathBuf;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, header};
use catalyst_testing::conformance::compare::{
    self, compare_body, query_string, status_and_header_notes,
};
use catalyst_testing::conformance::fixture::Fixture;
use catalyst_testing::conformance::volatility::Volatility;
use tower::ServiceExt;

/// `tests/fixtures/conformance/social/<service>/<name>.json`.
pub fn fixture(service: &str, name: &str) -> Fixture {
    let path = PathBuf::from(env!("DCL_ONE_ROOT"))
        .join("services/catalyst/tests/fixtures/conformance/social")
        .join(service)
        .join(format!("{name}.json"));
    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
}

/// What `fx` differs in when its request, with its recorded headers but any
/// auth chain (nothing here re-signs), is sent to `app` under `prefix` (the
/// Catalyst mount of the captured host): `status: ...` and `header ...` notes,
/// then one entry per differing body field, `<path> missing|extra|value`
/// with `[n]` folded to `[]` (what differs, not the seeded rows' own values),
/// or `body bytes differ` for a byte body.
pub async fn replay(app: &Router, prefix: &str, name: &str, fx: &Fixture) -> Vec<String> {
    let mut uri = format!("{prefix}{}", fx.request.path);
    if let Some(query) = query_string(&fx.request.query, None) {
        uri = format!("{uri}?{query}");
    }
    let mut request = Request::builder()
        .method(fx.request.method.as_str())
        .uri(uri);
    for (k, v) in &fx.request.headers {
        if !k.to_ascii_lowercase().starts_with("x-identity-") {
            request = request.header(k.as_str(), v.as_str());
        }
    }
    let request = request.body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let mut notes = status_and_header_notes(fx, status, &|key| {
        headers
            .get(key)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            // hyper writes the length of a sized body on the wire, not here
            .or_else(|| {
                key.eq_ignore_ascii_case(header::CONTENT_LENGTH.as_str())
                    .then(|| bytes.len().to_string())
            })
    });
    let diffs = match compare_body(fx, name, &bytes, &Volatility::default()) {
        compare::Body::Json(diffs) => diffs,
        compare::Body::Bytes { expected, same } => {
            if !same {
                notes.push(format!(
                    "body bytes differ: fixture={:?} candidate={:?}",
                    String::from_utf8_lossy(&expected),
                    String::from_utf8_lossy(&bytes)
                ));
            }
            return notes;
        }
        compare::Body::NotJson(_) => {
            notes.push(format!(
                "candidate body is not JSON: {:?}",
                String::from_utf8_lossy(&bytes)
            ));
            return notes;
        }
        compare::Body::BadFixture(note) => {
            notes.push(note);
            return notes;
        }
    };
    let mut fields: Vec<String> = diffs
        .into_iter()
        .map(|d| {
            let mut path = String::new();
            let mut index = false;
            for c in d.path.trim_start_matches("fixture.").chars() {
                match c {
                    '[' => {
                        index = true;
                        path.push_str("[]");
                    }
                    ']' => index = false,
                    _ if index => {}
                    _ => path.push(c),
                }
            }
            let kind = if d.candidate_value == "missing" {
                "missing"
            } else if d.baseline_value == "missing" {
                "extra"
            } else {
                "value"
            };
            format!("{path} {kind}")
        })
        .collect();
    fields.sort();
    fields.dedup();
    notes.extend(fields);
    notes
}
