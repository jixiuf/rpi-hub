//! Argument suggestions for `/msg` `/cmd` `/ask` `/use`.
//!
//! Called by the host TUI on a blocking thread with a bounded budget per
//! keystroke, so this must stay cheap: for client-mode hubs one keystroke
//! would otherwise cost a relay round-trip (~150 ms), so the instance list is
//! cached for 2 s (instance presence changes on the 3 s heartbeat cadence —
//! one lookup per cache window is plenty).
//!
//! Contract (host side, `register_argument_suggester`):
//! - request: `{"command":"/msg","args":"ap"}` — `args` is the parameter text
//!   typed so far, without the command token
//! - response: `{"items":[{"value","label","description"}]}` — `value` is what
//!   the completion inserts (`name@host`, the token `/msg` and friends accept)

use std::sync::{Mutex, OnceLock};

use rpi_hub_core::now_ms;
use rpi_hub_core::store::InstanceView;
use rpi_plugin_sdk::{StbString, StbStringRef};

use crate::host;

const CACHE_TTL_MS: i64 = 2_000;

/// Host→suggester entry point.
///
/// SAFETY (host contract): `request` is borrowed for this call only — do not
/// free; `out` is written exactly once and reclaimed by the host via the
/// `plugin_free_string` registered alongside this handler.
pub(crate) extern "C" fn suggest_arguments(
    request_json: StbStringRef,
    out: *mut StbString,
    _ud: *mut std::ffi::c_void,
) -> i32 {
    rpi_plugin_sdk::guard_or(1, || {
        let request = unsafe { request_json.as_str() };
        let payload =
            build_suggestions(request).unwrap_or_else(|| serde_json::json!({ "items": [] }));
        unsafe {
            *out = StbString::from_string(payload.to_string());
        }
        0
    })
}

/// Build the suggestion payload; `None` on any internal failure (the host
/// renders an empty popup rather than an error while typing).
fn build_suggestions(request: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(request).ok()?;
    let command = value.get("command")?.as_str()?.trim_start_matches('/');
    let args = value.get("args").and_then(|v| v.as_str()).unwrap_or("");

    let hub = host::current()?;
    let items = cached_rows(&hub, args)?;
    Some(serde_json::json!({
        "items": items,
        "command": command,
    }))
}

/// Cached instance rows (label/value/description triples, pre-filtered by the
/// current argument text). The cache holds the *unfiltered* rows so a
/// keystroke never pays for a relay round-trip; filtering is free.
fn cached_rows(hub: &std::sync::Arc<host::Hub>, query: &str) -> Option<Vec<serde_json::Value>> {
    struct Rows {
        payload: Vec<serde_json::Value>,
        at: i64,
    }
    static ROWS: OnceLock<Mutex<Option<Rows>>> = OnceLock::new();
    let cell = ROWS.get_or_init(|| Mutex::new(None));

    let payload = {
        let guard = cell.lock().unwrap_or_else(|p| p.into_inner());
        match guard.as_ref() {
            Some(rows) if now_ms() - rows.at < CACHE_TTL_MS => Some(rows.payload.clone()),
            _ => None,
        }
    };
    let all = match payload {
        Some(all) => all,
        None => {
            let instances = hub.instances(now_ms()).ok()?;
            let holder = hub.store.lease_holder("wechat").ok()?;
            // Local instance first (current), then others by name — same order
            // as `/instances`, so the popup numbering matches.
            let mut rows: Vec<(bool, &InstanceView)> = instances
                .iter()
                .map(|instance| (instance.node_id == hub.node_id, instance))
                .collect();
            rows.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| a.1.name.cmp(&b.1.name))
                    .then_with(|| a.1.host.cmp(&b.1.host))
            });
            let payload: Vec<serde_json::Value> = rows
                .iter()
                .map(|(is_current, instance)| {
                    let mut label = instance.label();
                    if *is_current {
                        label.push_str("（当前）");
                    }
                    if holder
                        .as_ref()
                        .is_some_and(|h| h.owner_node == instance.node_id)
                    {
                        label.push_str("（微信）");
                    }
                    if instance.busy {
                        label.push_str(" 忙");
                    }
                    serde_json::json!({
                        "value": instance.label(),
                        "label": label,
                        "description": instance.cwd,
                    })
                })
                .collect();
            {
                let mut guard = cell.lock().unwrap_or_else(|p| p.into_inner());
                *guard = Some(Rows {
                    payload: payload.clone(),
                    at: now_ms(),
                });
            }
            payload
        }
    };

    // Prefix-narrow against value/name. Contains-matching would drop the row
    // the user just completed (query equals its own value, which does not
    // contain itself as a substring), breaking Up/Down navigation afterwards.
    let query = query.trim().to_ascii_lowercase();
    let items: Vec<serde_json::Value> = if query.is_empty() {
        all
    } else {
        all.into_iter()
            .filter(|item| {
                let value = item.get("value").and_then(|v| v.as_str()).unwrap_or("");
                let label = item.get("label").and_then(|v| v.as_str()).unwrap_or("");
                value.to_lowercase().starts_with(&query)
                    || label.to_lowercase().contains(&query)
            })
            .collect()
    };
    (!items.is_empty()).then_some(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_requests_build_nothing() {
        assert!(build_suggestions("not json").is_none());
        assert!(build_suggestions("{}").is_none());
        assert!(build_suggestions(r#"{"command":"/msg"}"#).is_none());
    }
}
