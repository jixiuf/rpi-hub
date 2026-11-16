//! Event handlers.
//!
//! The host runs these synchronously (with a 5–15 s budget each), so they only
//! flip atomics or push into a bounded list. Network and SQLite work belongs to
//! the worker thread.

use rpi_hub_core::protocol::Envelope;
use rpi_hub_core::now_ms;
use rpi_plugin_sdk::{EventHandlerFn, EventTag};

use crate::host::{self, Hub};

pub fn registry() -> Vec<(EventTag, EventHandlerFn)> {
    vec![
        (EventTag::AgentStart, on_agent_start),
        (EventTag::AgentEnd, on_agent_end),
        (EventTag::AgentSettled, on_agent_settled),
        (EventTag::MessageEnd, on_message_end),
        (EventTag::TurnEndPayload, on_turn_end),
        (EventTag::SessionShutdown, on_session_shutdown),
    ]
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_start(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.set_busy(true);
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_end(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.set_busy(false);
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_settled(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.set_busy(false);
        }
        0
    }
}

// Finish the oldest pending `hub_ask` with the assistant text that just closed.
rpi_plugin_sdk::event_handler! {
    fn on_message_end(event, _user_data) {
        let Some(raw) = event.message_text() else {
            return 0;
        };
        let Some(text) = assistant_text(&raw) else {
            return 0;
        };
        if let Some(hub) = host::current() {
            complete_oldest_ask(&hub, true, text);
        }
        0
    }
}

// A turn that ended in `aborted`/`error` must fail the pending asks instead of
// letting them time out.
rpi_plugin_sdk::event_handler! {
    fn on_turn_end(event, _user_data) {
        let Some(raw) = event.data_text() else {
            return 0;
        };
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return 0;
        };
        let outcome = payload
            .get("outcome")
            .and_then(|v| v.as_str())
            .unwrap_or("stop");
        if outcome == "stop" {
            return 0;
        }
        if let Some(hub) = host::current() {
            let reason = payload
                .get("stopReason")
                .and_then(|v| v.as_str())
                .unwrap_or(outcome);
            complete_oldest_ask(&hub, false, format!("目标实例本轮结束: {reason}"));
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_session_shutdown(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.request_shutdown();
        }
        0
    }
}

/// Send the reply for the oldest pending ask and drop it from the queue.
fn complete_oldest_ask(hub: &Hub, ok: bool, text: String) {
    let pending = {
        let mut queue = hub.pending.lock().unwrap_or_else(|p| p.into_inner());
        if queue.is_empty() {
            return;
        }
        queue.remove(0)
    };
    let reply = Envelope::reply(
        &hub.node_id,
        &hub.name(),
        &pending.to_node,
        &pending.correlation,
        text,
        ok,
        now_ms(),
    );
    if let Err(error) = hub.store.send_envelope(&reply) {
        host::log(&format!("ask reply delivery failed: {error}"));
    }
}

/// Extract the assistant's visible text from a `MessageEnd` payload.
///
/// Thinking blocks, tool calls and empty assistant frames produce `None`, so a
/// tool-use message does not close a pending ask prematurely.
fn assistant_text(raw: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    if value.get("role").and_then(|v| v.as_str()) != Some("assistant") {
        return None;
    }
    let content = value.get("content")?;
    let mut out = String::new();
    match content {
        serde_json::Value::String(text) => out.push_str(text),
        serde_json::Value::Array(blocks) => {
            for block in blocks {
                match block.get("type").and_then(|v| v.as_str()) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                            if !out.is_empty() {
                                out.push('\n');
                            }
                            out.push_str(text);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::assistant_text;

    #[test]
    fn extracts_only_visible_assistant_text() {
        let raw = r#"{"role":"assistant","content":[{"type":"thinking","thinking":"hm"},{"type":"text","text":"答案"}]}"#;
        assert_eq!(assistant_text(raw).as_deref(), Some("答案"));
    }

    #[test]
    fn ignores_tool_calls_and_user_messages() {
        let tool_only =
            r#"{"role":"assistant","content":[{"type":"toolCall","name":"read","arguments":{}}]}"#;
        assert!(assistant_text(tool_only).is_none());
        let user = r#"{"role":"user","content":"hi"}"#;
        assert!(assistant_text(user).is_none());
        let empty = r#"{"role":"assistant","content":[{"type":"text","text":"   "}]}"#;
        assert!(assistant_text(empty).is_none());
    }
}
