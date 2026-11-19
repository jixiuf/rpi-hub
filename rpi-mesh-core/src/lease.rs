//! Lease access, split by capability (review refactor PR-2).
//!
//! The wechat lease historically had four writers across two plugins, which
//! produced both the stale-status bugs and the display contradictions: the
//! hub worker claimed and renewed beside the wechat plugin's own watcher +
//! bridge (same node_id, different epochs — fencing each other), and every
//! writer wrote the footer from its own momentary view.
//!
//! The fix is a *capability split expressed in types*:
//!
//! - [`LeaseOwner`] — claim / renew / release / takeover-dead-owner. Held by
//!   the **wechat plugin only**; its watcher and bridge are the two legitimate
//!   brains of the channel lifecycle.
//! - [`LeaseObserver`] — read the holder, list leases. Held by the **hub
//!   plugin** (its `/use` assign flows through [`LeaseOwner::assign_to`],
//!   reached via the owner token that the core hands out — see below).
//! - [`LeaseAdmin`] — `assign` an arbitrary capability to a present instance
//!   (`/use`, `/hub switch`). Exercised only by the hub plugin's command
//!   paths, on an explicit user request.
//!
//! The store's raw methods stay `pub` — the remote relay dispatch needs them
//! — but plugins are expected to go through this facade, and the facade is
//! what the code review greps for (`claim_lease` appears exactly once: here).

use std::sync::{Arc, Mutex};

use crate::api::HubStore;
use crate::protocol::Envelope;
use crate::store::{Claim, InstanceView, LeaseHolder};

/// Capability token for `LeaseOwner` — constructed by `LeaseOwner::for_store`,
/// so the compiler-verified path to mutation is the facade.
pub struct LeaseOwner {
    store: Arc<dyn HubStore>,
    /// Owning node identity (the wechat plugin's plain node_id).
    node_id: String,
    /// Display name used when the owner (re)writes lease rows. Interior
    /// mutable: the store may rename the instance at registration time
    /// (name collisions get a `-<pid>` suffix) and the plugin learns its
    /// final name only afterwards — see [`LeaseOwner::set_node_name`].
    node_name: Mutex<String>,
    epoch: String,
    ttl_ms: i64,
}

impl Clone for LeaseOwner {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            node_id: self.node_id.clone(),
            node_name: Mutex::new(self.node_name()),
            epoch: self.epoch.clone(),
            ttl_ms: self.ttl_ms,
        }
    }
}

impl LeaseOwner {
    /// Build an owner token. The wechat plugin constructs exactly one per
    /// process (on its Hub); the hub plugin constructs none.
    pub fn new(
        store: Arc<dyn HubStore>,
        node_id: String,
        node_name: String,
        epoch: String,
        ttl_ms: i64,
    ) -> Self {
        Self {
            store,
            node_id,
            node_name: Mutex::new(node_name),
            epoch,
            ttl_ms,
        }
    }

    /// Record the store-resolved display name (registration may rename us).
    /// Subsequent claim/renew writes carry the corrected name, so lease rows
    /// never mix two instances' display names.
    pub fn set_node_name(&self, name: impl Into<String>) {
        *self.node_name.lock().unwrap_or_else(|p| p.into_inner()) = name.into();
    }

    fn node_name(&self) -> String {
        self.node_name
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Claim (or resume) the capability for the owning node, recording the
    /// dead-owner takeover branch: a lingering lease whose presence row is
    /// gone is force-assigned so the unattended window is seconds, not TTL.
    pub fn claim(&self, capability: &str, now: i64) -> Result<Claim, String> {
        let node_name = self.node_name();
        let claim = self
            .store
            .claim_lease(
                capability,
                &self.node_id,
                &node_name,
                &self.epoch,
                now,
                self.ttl_ms,
            )
            .map_err(|e| e.to_string())?;
        if let Claim::Held { holder } = &claim {
            // Alive means "same-host pid still exists or row is on another
            // host we cannot probe": a C-c'd owner must not gate the
            // takeover for the rest of its heartbeat window.
            let owner_alive = self
                .store
                .get_instance_alive(&holder.owner_node)
                .map(|instance| instance.is_some())
                .unwrap_or(false);
            if !owner_alive {
                let generation = self
                    .store
                    .assign_lease(
                        capability,
                        &self.node_id,
                        &node_name,
                        &self.epoch,
                        now,
                        self.ttl_ms,
                    )
                    .map_err(|e| e.to_string())?;
                return Ok(Claim::Granted {
                    generation,
                    expires_at: now + self.ttl_ms,
                });
            }
        }
        Ok(claim)
    }

    /// Renew the lease for the owning node (fenced by node + epoch +
    /// generation inside the store).
    pub fn renew(&self, capability: &str, generation: i64, now: i64) -> Result<bool, String> {
        self.store
            .renew_lease(
                capability,
                &self.node_id,
                &self.epoch,
                generation,
                now,
                self.ttl_ms,
            )
            .map_err(|e| e.to_string())
    }

    /// Check the lease without extending it (outbound gating).
    pub fn check(&self, capability: &str, generation: i64, now: i64) -> bool {
        self.store
            .check_lease(capability, &self.node_id, &self.epoch, generation, now)
            .unwrap_or(false)
    }

    /// Release the lease (user stop).
    pub fn release(&self, capability: &str, generation: i64) -> Result<bool, String> {
        self.store
            .release_lease(capability, &self.node_id, &self.epoch, generation)
            .map_err(|e| e.to_string())
    }

    /// The current holder of `capability` (an owner also reads its state —
    /// e.g. mark_stopped shows who fenced it out).
    pub fn holder(&self, capability: &str) -> Result<Option<LeaseHolder>, String> {
        self.store
            .lease_holder(capability)
            .map_err(|e| e.to_string())
    }

    /// The identity this owner claims as (for status lines / logging).
    pub fn identity(&self) -> (String, String) {
        (
            self.node_id.clone(),
            self.node_name
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        )
    }

    pub fn ttl_ms(&self) -> i64 {
        self.ttl_ms
    }
}

/// Administrative hand-off: assign a capability to a PRESENT instance on an
/// explicit user request (`/use`). Constructed by the hub plugin for exactly
/// that path; the target must be alive (presence-gated) so we never hand a
/// channel to a ghost.
pub struct LeaseAdmin {
    store: Arc<dyn HubStore>,
    ttl_ms: i64,
}

impl LeaseAdmin {
    pub fn new(store: Arc<dyn HubStore>, ttl_ms: i64) -> Self {
        Self { store, ttl_ms }
    }

    /// Assign `capability` to `target`. `owner_epoch` is empty: the target's
    /// own owner token claims with its epoch on the next claim/renew (same
    /// node_id → resume branch).
    ///
    /// The lease move and the notification are intentionally NOT atomic:
    /// after the store row lands, both sides learn about the hand-off via
    /// `assign`/`unassign` envelopes (best effort — a lost envelope only
    /// costs latency, because renew/claim still converge within one tick).
    pub fn assign_to(
        &self,
        capability: &str,
        target: &InstanceView,
        now: i64,
    ) -> Result<i64, String> {
        // Read the previous owner BEFORE the row moves to the target.
        let previous = self
            .store
            .lease_holder(capability)
            .ok()
            .flatten()
            .filter(|holder| !holder.owner_node.is_empty() && holder.owner_node != target.node_id);
        let generation = self
            .store
            .assign_lease(
                capability,
                &target.node_id,
                &target.name,
                "",
                now,
                self.ttl_ms,
            )
            .map_err(|e| e.to_string())?;
        self.notify(capability, target, previous.as_ref(), generation, now);
        Ok(generation)
    }

    /// Tell the target it now owns `capability`, and tell the previous owner
    /// it was fenced. Best effort: a lost envelope only costs latency,
    /// because renew/claim converge within one OwnerLoop tick anyway.
    fn notify(
        &self,
        capability: &str,
        target: &InstanceView,
        previous: Option<&LeaseHolder>,
        generation: i64,
        now: i64,
    ) {
        let assign = Envelope::assign(
            "lease-admin",
            "lease-admin",
            &target.node_id,
            capability,
            generation,
            now,
        );
        let _ = self.store.send_envelope(&assign);
        if let Some(previous) = previous {
            let unassign = Envelope::unassign(
                "lease-admin",
                "lease-admin",
                &previous.owner_node,
                &target.name,
                capability,
                generation,
                now,
            );
            let _ = self.store.send_envelope(&unassign);
        }
    }
}

/// Read-only view — the hub plugin's lease surface. No mutation methods.
pub struct LeaseObserver {
    store: Arc<dyn HubStore>,
}

impl LeaseObserver {
    pub fn new(store: Arc<dyn HubStore>) -> Self {
        Self { store }
    }

    /// The current holder of `capability`, if any.
    pub fn holder(&self, capability: &str) -> Result<Option<LeaseHolder>, String> {
        self.store
            .lease_holder(capability)
            .map_err(|e| e.to_string())
    }

    /// All capability holders (the `/hub who` path).
    pub fn all(&self) -> Result<Vec<(String, LeaseHolder)>, String> {
        self.store.list_leases().map_err(|e| e.to_string())
    }
}

// Compile-time proof of the capability split: nothing in this module gives
// a LeaseObserver a path to claim/renew/release, and LeaseAdmin's assign is
// the only cross-instance mutation. If someone re-adds a claim call inside
// the hub plugin, it must go through Store directly — grep-visible.
