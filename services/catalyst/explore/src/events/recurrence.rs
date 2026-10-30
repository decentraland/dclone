//! Materialize bounded, finite event recurrence rules. Keeping all accepted
//! dates makes read-time occurrence selection independent of a rollover job.
//! The RFC 5545 engine applies weekday/month masks and calendar positions;
//! both its internal iteration guard and our result cap fail closed.

use chrono::{DateTime, Duration, Timelike, Utc};
use serde_json::{Map, Value, json};

use super::moderation::{finish_at_of, parse_rfc3339};
use crate::shared::errors::ApiError;

const MAX_OCCURRENCES: u16 = 1_000;

fn number(raw: &Map<String, Value>, key: &str) -> Option<i64> {
    raw.get(key).and_then(Value::as_i64)
}

pub(super) fn recompute(raw: &mut Map<String, Value>, now: DateTime<Utc>) -> Result<(), ApiError> {
    let start = raw
        .get("start_at")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339)
        .ok_or_else(|| ApiError::bad_request("invalid start_at"))?;
    let duration = number(raw, "duration").unwrap_or(0);
    if duration < 0 {
        return Err(ApiError::bad_request("duration must be positive"));
    }
    let frequency = raw.get("recurrent_frequency").and_then(Value::as_str);
    if frequency.is_some_and(|f| !matches!(f, "YEARLY" | "MONTHLY" | "WEEKLY" | "DAILY" | "HOURLY"))
    {
        return Err(ApiError::bad_request("unsupported recurrent_frequency"));
    }
    let interval = number(raw, "recurrent_interval").unwrap_or(1);
    let count = number(raw, "recurrent_count").filter(|c| *c != 0);
    let until = match raw.get("recurrent_until").and_then(Value::as_str) {
        Some(until) => Some(
            parse_rfc3339(until).ok_or_else(|| ApiError::bad_request("invalid recurrent_until"))?,
        ),
        None => None,
    };
    if !(1..=1000).contains(&interval) || count.is_some_and(|c| !(1..=1000).contains(&c)) {
        return Err(ApiError::bad_request(
            "recurrence interval/count out of range",
        ));
    }
    let weekdays = number(raw, "recurrent_weekday_mask").unwrap_or(0);
    let months = number(raw, "recurrent_month_mask").unwrap_or(0);
    if !(0..=127).contains(&weekdays) || !(0..=4095).contains(&months) {
        return Err(ApiError::bad_request("recurrence mask out of range"));
    }
    let recurrent = raw
        .get("recurrent")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && frequency.is_some()
        && (count.is_some() || until.is_some());
    let mut dates = vec![start];
    if recurrent {
        let mut rule = format!(
            "DTSTART:{}\nRRULE:FREQ={};INTERVAL={interval}",
            start.format("%Y%m%dT%H%M%SZ"),
            frequency.unwrap_or("DAILY")
        );
        if let Some(count) = count {
            rule.push_str(&format!(";COUNT={count}"));
        }
        if let Some(until) = until {
            rule.push_str(&format!(";UNTIL={}", until.format("%Y%m%dT%H%M%SZ")));
        }
        if weekdays != 0 {
            let days = ["SU", "MO", "TU", "WE", "TH", "FR", "SA"]
                .iter()
                .enumerate()
                .filter_map(|(i, day)| (weekdays & (1 << i) != 0).then_some(*day))
                .collect::<Vec<_>>();
            rule.push_str(&format!(";BYDAY={}", days.join(",")));
        }
        if months != 0 {
            let selected = (0..12)
                .filter(|i| months & (1 << i) != 0)
                .map(|i| (i + 1).to_string())
                .collect::<Vec<_>>();
            rule.push_str(&format!(";BYMONTH={}", selected.join(",")));
        }
        for (field, parameter, max) in [
            ("recurrent_setpos", "BYSETPOS", 366),
            ("recurrent_monthday", "BYMONTHDAY", 31),
        ] {
            if let Some(value) = number(raw, field).filter(|v| *v != 0) {
                if !(-max..=max).contains(&value) {
                    return Err(ApiError::bad_request("recurrence position out of range"));
                }
                rule.push_str(&format!(";{parameter}={value}"));
            }
        }
        let set = rule
            .parse::<rrule::RRuleSet>()
            .map_err(|_| ApiError::bad_request("invalid recurrence rule"))?;
        let result = set.all(MAX_OCCURRENCES + 1);
        if result.limited || result.dates.len() > usize::from(MAX_OCCURRENCES) {
            return Err(ApiError::bad_request(
                "recurrence exceeds the maximum expansion budget",
            ));
        }
        if !result.dates.is_empty() {
            dates = result
                .dates
                .into_iter()
                .map(|d| {
                    d.with_timezone(&Utc) + Duration::nanoseconds(i64::from(start.nanosecond()))
                })
                .collect();
        }
    }
    let finish = finish_at_of(*dates.last().unwrap_or(&start), duration)
        .ok_or_else(|| ApiError::bad_request("event finish out of range"))?;
    let next = dates
        .iter()
        .find(|d| finish_at_of(**d, duration).is_some_and(|f| f > now))
        .copied()
        .unwrap_or_else(|| *dates.last().unwrap_or(&start));
    raw.insert("recurrent".into(), json!(recurrent));
    raw.insert(
        "recurrent_dates".into(),
        json!(dates.iter().map(DateTime::to_rfc3339).collect::<Vec<_>>()),
    );
    raw.insert("finish_at".into(), json!(finish.to_rfc3339()));
    raw.insert("next_start_at".into(), json!(next.to_rfc3339()));
    raw.insert(
        "next_finish_at".into(),
        json!(finish_at_of(next, duration).map(|d| d.to_rfc3339())),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Map<String, Value> {
        json!({"start_at":"2099-06-01T18:00:00.123Z", "duration":3600000, "recurrent":true, "recurrent_frequency":"WEEKLY", "recurrent_count":3})
            .as_object().unwrap().clone()
    }

    #[test]
    fn weekly_series_preserves_future_occurrences_and_precision() {
        let mut raw = event();
        recompute(&mut raw, parse_rfc3339("2099-06-02T00:00:00Z").unwrap()).unwrap();
        let dates = raw["recurrent_dates"].as_array().unwrap();
        assert_eq!(dates.len(), 3);
        assert_eq!(dates[1], "2099-06-08T18:00:00.123+00:00");
        assert_eq!(raw["next_start_at"], dates[1]);
        assert_eq!(raw["finish_at"], "2099-06-15T19:00:00.123+00:00");
    }

    #[test]
    fn monthly_last_weekday_and_month_masks_use_calendar_rules() {
        let mut raw = event();
        raw.extend(json!({"recurrent_frequency":"MONTHLY", "recurrent_weekday_mask":62, "recurrent_setpos":-1, "recurrent_month_mask":160}).as_object().unwrap().clone());
        recompute(&mut raw, Utc::now()).unwrap();
        assert_eq!(raw["recurrent_dates"][0], "2099-06-30T18:00:00.123+00:00");
        assert_eq!(raw["recurrent_dates"][1], "2099-08-31T18:00:00.123+00:00");
    }

    #[test]
    fn invalid_and_excessive_rules_are_rejected() {
        let mut raw = event();
        raw.insert("recurrent_frequency".into(), json!("SECONDLY"));
        assert!(recompute(&mut raw, Utc::now()).is_err());
        raw.insert("recurrent_frequency".into(), json!("HOURLY"));
        raw.remove("recurrent_count");
        raw.insert("recurrent_until".into(), json!("2199-06-01T18:00:00Z"));
        assert!(recompute(&mut raw, Utc::now()).is_err());
    }

    #[test]
    fn until_and_count_share_the_same_thousand_occurrence_limit() {
        let mut raw = event();
        raw.insert("recurrent_frequency".into(), json!("HOURLY"));
        raw.remove("recurrent_count");
        let start = parse_rfc3339("2099-06-01T18:00:00Z").unwrap();
        raw.insert(
            "recurrent_until".into(),
            json!((start + Duration::hours(999)).to_rfc3339()),
        );
        recompute(&mut raw, Utc::now()).unwrap();
        assert_eq!(raw["recurrent_dates"].as_array().unwrap().len(), 1000);
        raw.insert(
            "recurrent_until".into(),
            json!((start + Duration::hours(1000)).to_rfc3339()),
        );
        assert!(recompute(&mut raw, Utc::now()).is_err());
    }
}
