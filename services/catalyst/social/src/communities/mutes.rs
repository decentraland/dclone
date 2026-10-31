//! `/v1/mutes`: the caller's client-side mute list (`social.user_mutes`),
//! offset-paged newest first, optionally filtered by `address`/`addresses`.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::Address;

use super::http::{CommError, EnvelopeData, Paginated, get_all, get_first, get_pagination_params};
use super::{SharedState, signed_path};

#[derive(Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct Mute {
    pub address: String,
    pub muted_at: String,
}

/// The body's wire shape for the TS export; the handlers read the bytes
/// with [`muted_address`].
#[derive(Deserialize)]
#[cfg_attr(
    not(feature = "ts"),
    allow(dead_code, reason = "only the TS export names it")
)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "communities/")
)]
pub struct MuteBody {
    pub muted_address: String,
}

/// The `muted_address` of a POST/DELETE body, read after the signed-fetch
/// gate and in upstream's order (`add-user-mute-handler.ts`): a body that is
/// not JSON, or is `null`, fails `request.json()` / the property read and
/// answers 500; any other value without a non-empty string `muted_address`
/// is "muted_address is required"; then the address must be `0x` + 40 hex
/// after lowercasing only (upstream `normalizeAddress` does not trim).
fn muted_address(body: &[u8]) -> Result<Address, CommError> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| CommError::Internal)?;
    if value.is_null() {
        return Err(CommError::Internal);
    }
    let raw = value
        .get("muted_address")
        .and_then(serde_json::Value::as_str)
        .filter(|raw| !raw.is_empty())
        .ok_or_else(|| CommError::bad_request("muted_address is required"))?;
    Address::parse_untrimmed(raw).map_err(|_| CommError::bad_request("Invalid muted_address"))
}

pub async fn get_mutes(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<impl IntoResponse, CommError> {
    let muter = state
        .require_signer(&headers, "get", &signed_path(&original, "/v1/mutes"))
        .await?;
    let pagination = get_pagination_params(&pairs);
    let mut filter: Vec<String> = get_all(&pairs, "addresses")
        .into_iter()
        .filter(|a| !a.is_empty())
        .map(|a| a.to_lowercase())
        .collect();
    if let Some(a) = get_first(&pairs, "address").filter(|a| !a.is_empty()) {
        filter.push(a.to_lowercase());
    }
    let filter: Option<Vec<String>> = if filter.is_empty() {
        None
    } else {
        Some(filter)
    };
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM social.user_mutes \
         WHERE muter_address = $1 AND ($2::text[] IS NULL OR muted_address = ANY($2))",
    )
    .bind(&muter)
    .bind(&filter)
    .fetch_one(&state.pool)
    .await?;
    let rows: Vec<(String, chrono::NaiveDateTime)> = sqlx::query_as(
        "SELECT muted_address, muted_at FROM social.user_mutes \
         WHERE muter_address = $1 AND ($2::text[] IS NULL OR muted_address = ANY($2)) \
         ORDER BY muted_at DESC LIMIT $3 OFFSET $4",
    )
    .bind(&muter)
    .bind(&filter)
    .bind(pagination.limit)
    .bind(pagination.offset)
    .fetch_all(&state.pool)
    .await?;
    let results: Vec<Mute> = rows
        .into_iter()
        .map(|(address, muted_at)| Mute {
            address,
            muted_at: format!("{}Z", muted_at.format("%Y-%m-%dT%H:%M:%S%.3f")),
        })
        .collect();
    Ok(Json(EnvelopeData {
        data: Paginated::new(results, total, &pagination),
    }))
}

pub async fn add_mute(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    body: Bytes,
) -> Result<impl IntoResponse, CommError> {
    let muter = state
        .require_signer(&headers, "post", &signed_path(&original, "/v1/mutes"))
        .await?;
    let muted = muted_address(&body)?;
    if muted == muter {
        return Err(CommError::bad_request("Cannot mute yourself"));
    }
    sqlx::query(
        "INSERT INTO social.user_mutes (muter_address, muted_address) VALUES ($1, $2) \
         ON CONFLICT (muter_address, muted_address) DO NOTHING",
    )
    .bind(&muter)
    .bind(&muted)
    .execute(&state.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_mute(
    State(state): State<SharedState>,
    headers: HeaderMap,
    original: OriginalUri,
    body: Bytes,
) -> Result<impl IntoResponse, CommError> {
    let muter = state
        .require_signer(&headers, "delete", &signed_path(&original, "/v1/mutes"))
        .await?;
    let muted = muted_address(&body)?;
    sqlx::query("DELETE FROM social.user_mutes WHERE muter_address = $1 AND muted_address = $2")
        .bind(&muter)
        .bind(&muted)
        .execute(&state.pool)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
