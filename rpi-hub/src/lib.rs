//! `rpi-hub` — multi-instance coordination for rpi.
//!
//! Registered surface:
//! - tools: `hub_instances`, `hub_send`, `hub_cmd`, `hub_ask`, `hub_switch`
//! - commands: `/instances`, `/use`, `/msg`, `/cmd`, `/ask`, `/hub`
//! - events: presence/busy tracking, and the `MessageEnd` -> `hub_ask` reply
//!   correlation
//!
//! Everything periodic (heartbeat, mailbox dispatch, lease renewal, pruning)
//! runs on one dedicated thread. Event handlers and command handlers only touch
//! memory or a microsecond SQLite statement, because the host runs them
//! synchronously on the TUI/blocking thread.

mod backends;
mod commands;
mod suggest;
mod events;
mod host;
mod intent;
mod remote;
mod tools;
mod worker;

pub use host::{install, Hub};

use rpi_plugin_sdk::{EventTag, StbStringRef};

// Every capability slot is optional: an older host that lacks one is skipped
// with a warning instead of failing the whole plugin.
rpi_plugin_sdk::export_plugin!(|api| {
    let hub = match host::install(&api) {
        Ok(hub) => hub,
        Err(error) => {
            host::log(&format!("rpi-hub disabled: {error}"));
            return 0;
        }
    };
    let mut status = 0;

    if let Err(error) = tools::register(&hub, &api) {
        host::log(&format!("rpi-hub: tools not registered: {error}"));
        status = 1;
    }

    if let Some(register_command) = api.register_command {
        for (name, description, handler) in commands::registry() {
            let rc = register_command(
                StbStringRef::from_str(name),
                StbStringRef::from_str(description),
                handler,
            );
            if rc != 0 {
                host::log(&format!("rpi-hub: command /{name} rejected by host"));
                status = 1;
            }
        }
    } else {
        host::log("rpi-hub: host has no command registrar; slash commands disabled");
    }

    // Argument suggestions for the TUI autocomplete pipeline: typing
    // `/msg ap` narrows the candidate list to `apmprod@…` before Enter.
    // One handler serves every instance-targeting command; skipped silently
    // on hosts without the slot (older rpi).
    if let Some(register_suggester) = api.register_argument_suggester {
        for command in ["msg", "cmd", "ask", "use"] {
            let rc = register_suggester(
                rpi_plugin_sdk::StbStringRef::from_str(command),
                suggest::suggest_arguments,
                rpi_plugin_sdk::callbacks::free_string,
                hub.bridge_ptr(),
            );
            if rc != 0 {
                host::log(&format!("rpi-hub: argument suggester /{command} rejected"));
            }
        }
    }

    if let Some(register_event) = api.register_event_handler {
        for (tag, handler) in events::registry() {
            let rc = register_event(tag, handler, hub.bridge_ptr());
            if rc != 0 {
                host::log(&format!("rpi-hub: event {tag:?} not registered"));
            }
        }
    } else {
        host::log("rpi-hub: host has no event registrar; presence updates disabled");
    }

    // Subscribe the lifecycle hooks last: once they exist the host may call
    // `begin_shutdown` at any time.
    if let Some(register_lifecycle) = api.register_plugin_lifecycle {
        let rc = register_lifecycle(
            worker::begin_shutdown,
            worker::finish_shutdown,
            hub.bridge_ptr(),
        );
        if rc != 0 {
            host::log("rpi-hub: lifecycle hooks rejected by host");
        }
    }

    if let Err(error) = hub.register_presence() {
        // Do not fail the plugin: a busy database at startup recovers on the
        // worker's retry loop.
        host::log(&format!("rpi-hub: presence registration deferred: {error}"));
    }
    hub.start_worker();
    // `EventTag` is needed for `registry()`'s return type; keep the import live.
    let _: Option<EventTag> = None;
    status
});
