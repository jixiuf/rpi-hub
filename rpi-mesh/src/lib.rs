//! `rpi-mesh` — single-plugin multi-instance coordination for rpi.
//!
//! One dylib per rpi process owns ALL of: presence, the mailbox, lease
//! election/ownership and the channel implementations. The old split
//! (rpi-hub + rpi-wechat) made every cross-plugin fact a cross-dylib fact,
//! which is impossible to do correctly because each dylib static-links its
//! own copy of the core (two copies of every global). The unified crate
//! turns those coordination seams into plain module boundaries enforced by
//! the compiler.
//!
//! Registered surface:
//! - tools: `hub_instances`, `hub_send`, `hub_ask`, `hub_switch`,
//!   `send_image_to_wechat`, `send_file_to_wechat`
//! - commands: `/instances`, `/use`, `/msg`, `/ask`, `/hub`, `/wechat`,
//!   `/start`, `/stop`
//! - events: presence/busy, ask-reply correlation
//! - threads: worker (heartbeat/mailbox/prune), owner loop (lease renew),
//!   bridge (channel IO), watcher (election + takeover)

mod backends;
mod commands;
mod events;
mod hub;
mod intent;
mod suggest;
mod tools;
pub mod wechat;
mod worker;

pub use hub::install;

use rpi_plugin_sdk::StbStringRef;

rpi_plugin_sdk::export_plugin!(|api| {
    let hub = match install(&api) {
        Ok(hub) => Ok(hub),
        Err(error) => {
            hub::log(&format!("rpi-mesh disabled: {error}"));
            Err(())
        }
    };
    let Ok(mesh) = hub else { return 0 };
    let mut status = 0;
    let hub = wechat::hub::Hub::new(std::sync::Arc::clone(&mesh));

    if let Err(error) = tools::register(&mesh, &api) {
        hub::log(&format!("rpi-mesh: tools not registered: {error}"));
        status = 1;
    }
    // Media tools from the wechat module.
    if let Err(error) = wechat::outbound::register(&hub, &api) {
        hub::log(&format!("rpi-mesh: media tools not registered: {error}"));
        status = 1;
    }

    if let Some(register_command) = api.register_command {
        for (name, description, handler) in commands::registry()
            .into_iter()
            .chain(wechat::commands::registry())
        {
            let rc = register_command(
                StbStringRef::from_str(name),
                StbStringRef::from_str(description),
                handler,
            );
            if rc != 0 {
                hub::log(&format!("rpi-mesh: command /{name} rejected by host"));
                status = 1;
            }
        }
    }

    if let Some(register_suggester) = api.register_argument_suggester {
        for command in ["msg", "ask", "use", "stop"] {
            let rc = register_suggester(
                StbStringRef::from_str(command),
                suggest::suggest_arguments,
                rpi_plugin_sdk::callbacks::free_string,
                hub.bridge_ptr(),
            );
            if rc != 0 {
                hub::log(&format!("rpi-mesh: argument suggester /{command} rejected"));
            }
        }
    }

    if let Some(register_event) = api.register_event_handler {
        for (tag, handler) in events::registry().into_iter().chain(wechat::commands::event_registry()) {
            let _ = register_event(tag, handler, hub.bridge_ptr());
        }
    }

    if let Some(register_lifecycle) = api.register_plugin_lifecycle {
        let _ = register_lifecycle(worker::begin_shutdown, worker::finish_shutdown, hub.bridge_ptr());
    }

    if let Err(error) = mesh.register_presence() {
        hub::log(&format!("rpi-mesh: presence registration deferred: {error}"));
    }
    mesh.start_worker();

    // Wechat channel bootstrap: credentials + disabled flag + session latch.
    // The bridge claim happens inside spawn; the watcher handles takeover.
    wechat::commands::bootstrap(&hub);
    status
});
