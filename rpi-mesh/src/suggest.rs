//! Argument suggestions for `/msg` `/ask` `/use` `/stop`.
//!
//! Contract (pi-rust PR #338, **full-candidate-list mode**): the host caches
//! the response **per command** and narrows it locally against the typed
//! prefix on every keystroke, so rpi-mesh returns the FULL instance list and
//! ignores the request's `args` field (`{"command":"/msg","args":"ap"}`).
//! Filtering here would poison the host's per-command cache: after the user
//! types `ap`, deleting a character could not restore candidates the filtered
//! frame no longer contains.
//!
//! - request: `{"command":"/msg","args":"ap"}` — `args` is the parameter text
//!   typed so far, without the command token; accepted but not filtered on
//!   (see above)
//! - response: `{"items":[{"value","label","description"}]}` — `value` is what
//!   the completion inserts (`name@host`, the token `/msg` and friends accept)
//! - caps: at most [`MAX_ITEMS`] items, each field at most [`MAX_FIELD_CHARS`]
//!   characters, truncated with `…` — size limits are enforced here, before
//!   the payload crosses the ABI (the host re-checks its own byte budget, but
//!   the plugin must not rely on it).
//!
//! The instance list is cached for 2 s (presence changes on the 3 s heartbeat
//! cadence; for client-mode instances a refresh costs a relay round-trip), so the
//! host's background refresh pays the round-trip at most once per cache
//! window. Typing itself never waits on this handler.

use std::sync::{Mutex, OnceLock};

use rpi_mesh_core::now_ms;
use rpi_mesh_core::store::InstanceView;
use rpi_plugin_sdk::{StbString, StbStringRef};

use crate::mesh;

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

    let mesh = mesh::current()?;
    // /cmd and /ask take a SECOND argument (the intent) after the target:
    // offer the intent list as composite rows ("target /new", …) once a
    // target is typed. The host's narrowing is prefix-ranked, so these
    // composite rows win (rank 0) while the bare instance rows still match
    // by contains (rank 2) — both visible, the intent rows first.
    let items = if command == "ask" {
        let mut items = cached_rows(&mesh)?;
        let target_part = args.split_once(char::is_whitespace).map(|(t, _)| t);
        if let Some(target) = target_part.filter(|t| !t.trim().is_empty()) {
            for (name, desc) in INTENTS {
                items.push(serde_json::json!({
                    "value": format!("{target} /{name}"),
                    "label": format!("/{name} — {desc}"),
                }));
            }
        }
        cap_rows(items)
    } else {
        cap_rows(cached_rows(&mesh)?)
    };
    Some(serde_json::json!({
        "items": items,
        "command": command,
    }))
}

/// The intents valid after a target in /cmd and /ask (kept in sync with
/// intent.rs's execute match).
const INTENTS: &[(&str, &str)] = &[
    ("status", "实例与会话状态"),
    ("new", "开启新会话"),
    ("fork", "从当前会话 fork"),
    ("switch", "切换到指定会话"),
    ("compact", "压缩上下文"),
    ("model", "切换模型"),
    ("thinking", "设置 thinking 级别"),
    ("name", "设置会话名"),
    ("stop", "停止当前生成"),
    ("reload", "重载本实例扩展"),
];

/// Cached instance rows (label/value/description triples, **unfiltered** —
/// the full candidate list; the host narrows against the typed prefix
/// locally, per keystroke). The cache holds the full list so the host's
/// background refresh pays the relay round-trip at most once per 2 s window.
fn cached_rows(mesh: &std::sync::Arc<mesh::Mesh>) -> Option<Vec<serde_json::Value>> {
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
            let instances = mesh.instances(now_ms()).ok()?;
            let holder = mesh.lease_observer.holder("wechat").ok()?;
            // Local instance first (current), then others by name — same order
            // as `/instances`, so the popup numbering matches.
            let mut rows: Vec<(bool, &InstanceView)> = instances
                .iter()
                .map(|instance| (instance.node_id == mesh.node_id, instance))
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

    // No prefix filtering here (full-candidate-list mode): the host caches by
    // command and narrows locally. Filtering on `args` plugin-side would
    // poison that per-command cache — after typing `ap`, deleting a character
    // could not restore candidates the filtered frame no longer contains.
    (!all.is_empty()).then_some(all)
}

/// Response caps (review: size limits must be enforced before the payload
/// crosses the ABI; the host re-checks its own byte budget, but the plugin
/// must not rely on it).
const MAX_ITEMS: usize = 32;
/// Per-field character cap (labels embed cwd paths and CJK text, so count
/// chars, not bytes); longer fields are truncated with `…`.
const MAX_FIELD_CHARS: usize = 256;

/// Enforce the response caps: at most [`MAX_ITEMS`] rows, every field at most
/// [`MAX_FIELD_CHARS`] characters. Pure, so the caps are regression-tested
/// without a live mesh.
fn cap_rows(items: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    items
        .into_iter()
        .take(MAX_ITEMS)
        .map(|item| {
            let field = |key: &str| item.get(key).and_then(|v| v.as_str()).map(truncate_field);
            serde_json::json!({
                "value": field("value").unwrap_or_default(),
                "label": field("label"),
                "description": field("description"),
            })
        })
        .collect()
}

fn truncate_field(text: &str) -> String {
    if text.chars().count() <= MAX_FIELD_CHARS {
        return text.to_string();
    }
    // Total length INCLUDING the ellipsis stays at MAX_FIELD_CHARS (the SDK
    // contract: a field is at most 256 characters, not 257).
    let cut: String = text.chars().take(MAX_FIELD_CHARS - 1).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmd_with_target_offers_intent_rows() {
        // build_suggestions 需要 host::current（插件上下文），单测只覆盖
        // INTENTS 表与 value 拼接逻辑——通过 cap_rows 纯函数侧写验证。
        let items: Vec<serde_json::Value> = INTENTS
            .iter()
            .map(|(name, desc)| {
                serde_json::json!({
                    "value": format!("jixiuf@h1 /{name}"),
                    "label": format!("/{name} — {desc}"),
                })
            })
            .collect();
        let capped = cap_rows(items);
        assert_eq!(capped.len(), INTENTS.len());
        assert!(capped[0]["value"]
            .as_str()
            .unwrap()
            .starts_with("jixiuf@h1 /status"));
    }

    #[test]
    fn malformed_requests_build_nothing() {
        assert!(build_suggestions("not json").is_none());
        assert!(build_suggestions("{}").is_none());
        assert!(build_suggestions(r#"{"command":"/msg"}"#).is_none());
    }

    #[test]
    fn caps_enforce_item_and_field_limits() {
        let many: Vec<serde_json::Value> = (0..40)
            .map(|i| serde_json::json!({"value": format!("inst{i}@h"), "label": i}))
            .collect();
        assert_eq!(cap_rows(many).len(), MAX_ITEMS);

        // A >MAX_FIELD_CHARS CJK value is truncated to exactly
        // MAX_FIELD_CHARS characters including the ellipsis marker, never
        // mid-codepoint.
        let rows = vec![serde_json::json!({
            "value": "值".repeat(MAX_FIELD_CHARS + 50),
            "label": "x",
        })];
        let capped = cap_rows(rows);
        let value = capped[0].get("value").and_then(|v| v.as_str()).unwrap();
        assert_eq!(value.chars().count(), MAX_FIELD_CHARS); // incl. `…`
        assert!(value.ends_with('…'));
    }

    #[test]
    fn short_fields_pass_through_cap_rows_unharmed() {
        let rows = vec![serde_json::json!({
            "value": "apmprod@h",
            "label": "1. apmprod（微信）",
            "description": "/export/home/jixiuf",
        })];
        let capped = cap_rows(rows);
        assert_eq!(capped.len(), 1);
        let row = &capped[0];
        assert_eq!(row.get("value").and_then(|v| v.as_str()), Some("apmprod@h"));
        assert_eq!(
            row.get("label").and_then(|v| v.as_str()),
            Some("1. apmprod（微信）")
        );
        assert_eq!(
            row.get("description").and_then(|v| v.as_str()),
            Some("/export/home/jixiuf")
        );
    }
}
