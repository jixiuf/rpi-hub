//! Event handlers.
//!
//! The host runs these synchronously (with a 5–15 s budget each), so they only
//! flip atomics or push into a bounded list. Network and SQLite work belongs to
//! the worker thread.

use rpi_plugin_sdk::{EventHandlerFn, EventTag};

use crate::mesh;

rpi_plugin_sdk::event_handler! {
    fn on_session_shutdown(_event, _user_data) {
        if let Some(mesh) = mesh::current() {
            mesh.request_shutdown();
        }
        0
    }
}

pub fn registry() -> Vec<(EventTag, EventHandlerFn)> {
    vec![
        (EventTag::AgentStart, on_agent_start),
        (EventTag::AgentEnd, on_agent_end),
        (EventTag::AgentSettled, on_agent_settled),
        (EventTag::SessionShutdown, on_session_shutdown),
    ]
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_start(_event, _user_data) {
        if let Some(mesh) = mesh::current() {
            mesh.set_busy(true);
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_end(_event, _user_data) {
        if let Some(mesh) = mesh::current() {
            mesh.set_busy(false);
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_settled(_event, _user_data) {
        if let Some(mesh) = mesh::current() {
            mesh.set_busy(false);
        }
        0
    }
}



