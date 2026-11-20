//! `rpi-mesh` — single-plugin multi-instance coordination for rpi.
//!
//! One dylib per rpi process owns ALL of: presence, the mailbox, lease
//! election/ownership and the channel implementations. The old split
//! (rpi-mesh + rpi-wechat) made every cross-plugin fact a cross-dylib fact,
//! which is impossible to do correctly because each dylib static-links its
//! own copy of the core (two copies of every global). The unified crate
//! turns those coordination seams into plain module boundaries enforced by
//! the compiler.
//!
//! Registered surface:
//! - tools: `mesh_instances`, `mesh_send`, `mesh_ask`, `mesh_switch`,
//!   `send_image_to_wechat`, `send_file_to_wechat`
//! - commands: `/instances`, `/use`, `/msg`, `/ask`, `/mesh`, `/wechat`,
//!   `/start`, `/stop`
//! - events: presence/busy, ask-reply correlation
//! - threads: worker (heartbeat/mailbox/prune), owner loop (lease renew),
//!   bridge (channel IO), watcher (election + takeover)

mod backends;
mod commands;
mod events;
mod mesh;
mod intent;
mod suggest;
mod tools;
pub mod wechat;
mod worker;

pub use mesh::install;

use rpi_plugin_sdk::StbStringRef;

rpi_plugin_sdk::export_plugin!(|api| {
    let mesh = match install(&api) {
        Ok(mesh) => Ok(mesh),
        Err(error) => {
            mesh::log(&format!("rpi-mesh disabled: {error}"));
            Err(())
        }
    };
    let Ok(state) = mesh else { return 0 };
    let mut status = 0;
    let channel = wechat::channel::Channel::new(std::sync::Arc::clone(&state));

    if let Err(error) = tools::register(&state, &api) {
        mesh::log(&format!("rpi-mesh: tools not registered: {error}"));
        status = 1;
    }
    // Media tools from the wechat module.
    if let Err(error) = wechat::outbound::register(&channel, &api) {
        mesh::log(&format!("rpi-mesh: media tools not registered: {error}"));
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
                mesh::log(&format!("rpi-mesh: command /{name} rejected by host"));
                status = 1;
            }
        }
    }

    if let Some(register_suggester) = api.register_argument_suggester {
        for command in ["msg", "ask", "use", "stop", "start"] {
            let rc = register_suggester(
                StbStringRef::from_str(command),
                suggest::suggest_arguments,
                rpi_plugin_sdk::callbacks::free_string,
                state.bridge_ptr(),
            );
            if rc != 0 {
                mesh::log(&format!("rpi-mesh: argument suggester /{command} rejected"));
            }
        }
    }

    if let Some(register_event) = api.register_event_handler {
        for (tag, handler) in events::registry().into_iter().chain(wechat::commands::event_registry()) {
            let _ = register_event(tag, handler, state.bridge_ptr());
        }
    }

    if let Some(register_lifecycle) = api.register_plugin_lifecycle {
        let _ = register_lifecycle(worker::begin_shutdown, worker::finish_shutdown, state.bridge_ptr());
    }

    if let Err(error) = state.register_presence() {
        mesh::log(&format!("rpi-mesh: presence registration deferred: {error}"));
    }
    state.start_worker();

    // Wechat channel bootstrap: credentials + disabled flag + session latch.
    // The bridge claim happens inside spawn; the watcher handles takeover.
    wechat::commands::bootstrap(&channel);
    status
});
