//! `rpi-wechat` — WeChat iLink Bot channel for rpi.
//!
//! One instance owns the channel at a time (the `wechat` lease in
//! rpi-hub-core). While the lease is held this plugin:
//! - long-polls `getupdates` and feeds authorized inbound text into the
//!   agent lane (`SendUserMessage`, steer while busy),
//! - forwards assistant replies to WeChat (`MessageEnd` → chunked
//!   `sendmessage`),
//! - exposes `/wechat login|start|stop|status` commands and
//!   `send_file_to_wechat` / `send_image_to_wechat` tools (media milestone).
//!
//! Everything periodic runs on one dedicated thread; FFI callbacks only flip
//! atomics or push to bounded queues, per the host's synchronous-callback
//! contract.

mod bridge;
mod commands;
mod error;
pub mod host;
mod ilink;
mod media;
mod outbound;
pub mod qr;
mod state;


pub use error::IlinkError;
pub use host::{current, Hub};
pub use state::Credentials;

// Plugin entry point. All slots are optional; an older host that lacks one is
// skipped with a warning.
rpi_plugin_sdk::export_plugin!(|api| {
    let hub = match host::Hub::new() {
        Ok(hub) => hub,
        Err(error) => {
            host::log(&format!("rpi-wechat disabled: {error}"));
            return 0;
        }
    };
    hub.set_bridge(api.user_data, api.runtime_action, api.free_string);

    if let Some(register_command) = api.register_command {
        for (name, description, handler) in commands::registry() {
            let rc = register_command(
                rpi_plugin_sdk::StbStringRef::from_str(name),
                rpi_plugin_sdk::StbStringRef::from_str(description),
                handler,
            );
            if rc != 0 {
                host::log(&format!("rpi-wechat: command /{name} rejected"));
            }
        }
    }
    if let Err(error) = outbound::register(&hub, &api) {
        host::log(&format!(
            "rpi-wechat: outbound tools not registered: {error}"
        ));
    }

    if let Some(register_event) = api.register_event_handler {
        for (tag, handler) in commands::event_registry() {
            let _ = register_event(tag, handler, hub.bridge_ptr());
        }
    }
    if let Some(register_lifecycle) = api.register_plugin_lifecycle {
        let _ = register_lifecycle(lifecycle_begin, lifecycle_finish, hub.bridge_ptr());
    }
    // Auto-start the bridge when credentials exist, the disabled flag is off,
    // and the wechat lease is free (claim is inside spawn_bridge).
    let dir = hub.dir.clone();
    if !state::is_disabled(&dir) {
        if let Some(credentials) = state::Credentials::load(&dir) {
            match commands::spawn_bridge_public(&hub, &credentials) {
                Ok(()) => host::log("bridge auto-started"),
                Err(error) => host::log(&format!(
                    "bridge auto-start deferred: {error}; watcher will take over"
                )),
            }
            // Watcher: takes over the channel whenever it is unattended
            // (remote instance died, lease expired) and this process is not
            // already running a bridge. Skipped while the user has the
            // bridge disabled.
            commands::spawn_watcher(&hub, credentials);
        }
    }
    hub.install();
    0
});

extern "C" fn lifecycle_begin(_user_data: *mut std::ffi::c_void) {
    if let Some(hub) = host::current() {
        hub.request_shutdown();
    }
}

extern "C" fn lifecycle_finish(_user_data: *mut std::ffi::c_void) -> i32 {
    if let Some(hub) = host::current() {
        hub.request_shutdown();
        hub.abort_renew_task();
        hub.stop_thread();
        // Release the wechat lease on exit: otherwise it lingers for the TTL
        // (20s) showing a dead instance as the channel owner in /instances,
        // and a graceful exit should hand the channel over immediately.
        bridge::release_and_stop(&hub);
        host::clear_current();
    }
    0
}
