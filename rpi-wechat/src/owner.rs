//! The lease owner loop: the single writer of the wechat lease and of this
//! instance's wechat footer slot.
//!
//! Why this thread exists: the bridge's loop body is dominated by a 40 s
//! `getupdates` long poll. Renewing the lease only at the top of that loop
//! meant the lease could (and did) expire mid-poll on every idle cycle, and
//! any other instance's watcher would grab it — the takeover-oscillation bug.
//! Renewal therefore lives on its own clock, immune to whatever the bridge
//! is doing.
//!
//! Ownership rules enforced across the crate:
//!
//! - renew / re-claim / release happen HERE only;
//! - the bridge never touches the lease — it polls `lease_alive` and stops
//!   when the owner loop goes away;
//! - the footer (`SetStatus{key:"wechat"}`) is written by the owner loop
//!   while holding, and by the watcher (commands.rs) otherwise — both render
//!   from store facts, never from private bookkeeping.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rpi_hub_core::now_ms;
use rpi_hub_core::status;
use rpi_hub_core::store::Claim;

use crate::bridge::StopReason;
use crate::host::Hub;

/// Spawn the owner loop. Called by `spawn_bridge` after a successful claim;
/// the loop exits when shutdown is requested or the lease is lost.
pub fn spawn(hub: Arc<Hub>) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("rpi-wechat-owner".into())
        .spawn(move || run(hub))
        .expect("spawn rpi-wechat-owner thread")
}

/// Poll cadence for lease-fact checks. Cheap: one SQLite row read (WAL,
/// <0.1ms). Note on cross-dylib signaling: rpi-hub-core is an rlib that
/// each plugin dylib static-links, so process-global Condvars are PER-DYLIB
/// and a publish from the hub plugin can never wake the wechat plugin's
/// threads. Honest polling at this cadence replaces the (dead) signal
/// design; it also detects a hand-off faster than the old 5s renew tick.
pub(crate) const FACT_POLL: Duration = Duration::from_millis(200);

fn run(hub: Arc<Hub>) {
    let renew_period = Duration::from_millis(hub.config.lease.renew_ms() as u64);
    crate::host::log("owner loop started");
    let mut last_renew = Instant::now();
    loop {
        if hub.shutdown.requested() {
            break;
        }
        // The bridge's last word: a session expiry must release the lease
        // (see `handle_bridge_stop`) instead of renewing it forever.
        if let Some(reason) = hub.take_bridge_stop() {
            handle_bridge_stop(&hub, reason);
            break;
        }
        // Fact check every FACT_POLL: is the lease row still ours? One
        // indexed read; catches a `/use` hand-off within 200ms instead of
        // waiting out the renew period.
        match hub.lease_owner.holder("wechat") {
            Ok(Some(holder)) if holder.owner_node != hub.node_id => {
                hub.record_error(format!(
                    "wechat: lease lost to {} (generation {})",
                    holder.full_name(),
                    holder.generation
                ));
                hub.set_status(&status::status_remote(&holder.full_name()));
                *hub.lease.lock().unwrap_or_else(|p| p.into_inner()) = None;
                break;
            }
            Ok(None) => {
                // Row vanished (released or pruned): treat as lost.
                hub.record_error("wechat: lease row gone");
                *hub.lease.lock().unwrap_or_else(|p| p.into_inner()) = None;
                break;
            }
            _ => {}
        }
        // Renewal on its own cadence.
        if last_renew.elapsed() >= renew_period {
            last_renew = Instant::now();
            if !tick(&hub) {
                break;
            }
        }
        hub.shutdown.wait(FACT_POLL);
    }
    hub.set_lease_alive(false);
    crate::host::log("owner loop stopped");
}

/// One renewal attempt. Returns `false` when the loop must stop.
fn tick(hub: &Arc<Hub>) -> bool {
    let ttl = hub.config.lease.ttl_ms();
    let now = now_ms();
    let Some(generation) = hub
        .lease
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .map(|lease| lease.generation)
    else {
        return false;
    };
    if hub
        .lease_owner
        .renew("wechat", generation, now)
        .unwrap_or(false)
    {
        if let Some(lease) = hub.lease.lock().unwrap_or_else(|p| p.into_inner()).as_mut() {
            lease.expires_at = now + ttl;
        }
        write_footer(hub);
        return true;
    }

    // Renewal refused: fenced, expired-and-claimed, or a transient store
    // error. Re-claim once before giving up — this is the "process paused
    // longer than the TTL, nobody took over" resume path; when someone else
    // does hold it, `Held` says so and we exit instead of fighting.
    if hub.shutdown.requested() {
        return false;
    }
    match hub.claim_wechat() {
        Ok(Claim::Granted { .. }) => {
            hub.record_error("wechat: lease resumed after renewal lapse");
            write_footer(hub);
            true
        }
        Ok(Claim::Held { holder }) => {
            hub.record_error(format!(
                "wechat: lease lost to {} (generation {})",
                holder.full_name(),
                holder.generation
            ));
            // Flip the footer immediately from the fact we just read — the
            // watcher re-renders the same line once the bridge thread exits,
            // but the user should not wait for that poll cycle.
            hub.set_status(&status::status_remote(&holder.full_name()));
            *hub.lease.lock().unwrap_or_else(|p| p.into_inner()) = None;
            false
        }
        Err(error) => {
            hub.record_error(format!("wechat: renew/claim failed: {error}"));
            false
        }
    }
}

/// The footer, composed from facts this process can verify: still holding
/// (💬), holding but the outbound path is degraded (💭). The watcher owns
/// the remote (🗨️) and stopped (📴) shapes.
fn write_footer(hub: &Arc<Hub>) {
    let label = hub.label();
    let text = if hub.degraded() {
        status::status_degraded(&label)
    } else {
        status::status_local(&label)
    };
    hub.set_status(&text);
}

fn handle_bridge_stop(hub: &Arc<Hub>, reason: StopReason) {
    match reason {
        StopReason::SessionExpired => {
            // The bridge can no longer poll or send: keeping the lease would
            // block every other instance for a full TTL per cycle. Release
            // and let a fresh `/wechat login` start a clean cycle. The
            // session_expired latch also keeps the watcher from churning a
            // dead session (cleared by /wechat login|start).
            if let Some(lease) = hub.lease.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
                let _ = hub.lease_owner.release("wechat", lease.generation);
            }
            *hub.lease.lock().unwrap_or_else(|p| p.into_inner()) = None;
            hub.set_session_expired(true);
            hub.set_status(&status::status_stopped("登录过期"));
        }
        StopReason::LeaseLost | StopReason::Shutdown => {}
    }
}
