//! The storage surface the hub uses, so a node can either own the database
//! (`Store`) or talk to a relay that does (`wire::RemoteStore`).
//!
//! Keeping the plugins on this trait is what makes cross-machine a transport
//! detail: worker, tools, commands and events never learn where the rows live.

use serde::{Deserialize, Serialize};

use crate::protocol::Envelope;
use crate::store::{
    Claim, Heartbeat, InstanceRecord, InstanceView, LeaseHolder, PruneReport, Stats, Store,
};
use crate::Result;

/// One RPC method name per operation. Serialized on the relay wire, so these
/// strings are a compatibility surface: add, never rename.
pub mod method {
    pub const REGISTER: &str = "register_instance";
    pub const HEARTBEAT: &str = "heartbeat";
    pub const LIST: &str = "list_instances";
    pub const GET: &str = "get_instance";
    pub const UNREGISTER: &str = "unregister_instance";
    pub const CLAIM: &str = "claim_lease";
    pub const RENEW: &str = "renew_lease";
    pub const CHECK: &str = "check_lease";
    pub const ASSIGN: &str = "assign_lease";
    pub const RELEASE: &str = "release_lease";
    pub const HOLDER: &str = "lease_holder";
    pub const LEASES: &str = "list_leases";
    pub const SEND: &str = "send_envelope";
    pub const INBOX: &str = "inbox";
    pub const ACK: &str = "ack_envelope";
    pub const SEEN: &str = "mark_seen";
    pub const PRUNE: &str = "prune";
    pub const STATS: &str = "stats";
    pub const CHANNEL_STATE: &str = "channel_state";
    pub const SET_CHANNEL_STATE: &str = "set_channel_state";
}

pub trait HubStore: Send + Sync + std::fmt::Debug {
    fn register_instance(
        &self,
        record: &InstanceRecord,
        alive_ms: i64,
    ) -> Result<crate::store::RegisteredInstance>;
    fn heartbeat(&self, node_id: &str, epoch: &str, now: i64, beat: &Heartbeat) -> Result<bool>;
    fn list_instances(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>>;
    fn get_instance(&self, node_id: &str) -> Result<Option<InstanceView>>;
    fn unregister_instance(&self, node_id: &str, epoch: &str) -> Result<bool>;
    fn claim_lease(
        &self,
        capability: &str,
        node_id: &str,
        node_name: &str,
        epoch: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Claim>;
    fn renew_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
        now: i64,
        ttl_ms: i64,
    ) -> Result<bool>;
    fn check_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
        now: i64,
    ) -> Result<bool>;
    fn assign_lease(
        &self,
        capability: &str,
        to_node: &str,
        to_name: &str,
        owner_epoch: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64>;
    fn release_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
    ) -> Result<bool>;
    fn lease_holder(&self, capability: &str) -> Result<Option<LeaseHolder>>;
    fn list_leases(&self) -> Result<Vec<(String, LeaseHolder)>>;
    fn send_envelope(&self, envelope: &Envelope) -> Result<bool>;
    fn inbox(&self, node_id: &str, now: i64, limit: i64) -> Result<Vec<Envelope>>;
    fn ack_envelope(&self, id: &str, node_id: &str, now: i64) -> Result<bool>;
    fn mark_seen(&self, id: &str, now: i64) -> Result<bool>;
    fn prune(&self, now: i64, alive_ms: i64) -> Result<PruneReport>;
    fn stats(&self, now: i64) -> Result<Stats>;

    /// `list_instances` filtered for same-host dead pids — what every
    /// command/tool should show. Default impl: reap locally, then delegate.
    fn list_instances_alive(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        let _ = self.reap_dead_local_instances();
        self.list_instances(now, alive_ms)
    }

    /// `get_instance` with the same filter (read-only: the dead same-host
    /// row is hidden, not deleted).
    fn get_instance_alive(&self, node_id: &str) -> Result<Option<InstanceView>> {
        let Some(view) = self.get_instance(node_id)? else {
            return Ok(None);
        };
        if view.host == crate::store::local_host_name() && !crate::store::pid_alive(view.pid) {
            return Ok(None);
        }
        Ok(Some(view))
    }

    /// Channel state carried with a lease (cursor etc. move atomically with
    /// ownership). None when there is no lease row or no state.
    fn channel_state(&self, _capability: &str) -> Option<serde_json::Value> {
        None
    }
    fn set_channel_state(&self, _capability: &str, _state: &serde_json::Value) {}
    /// Legacy kv cursor (v1 schema) — read-only migration source.
    fn legacy_cursor(&self) -> Option<String> {
        None
    }

    /// Drop presence rows of same-host pids that are demonstrably dead
    /// (`kill(pid, 0)` fails). Only the local Store reaps; remote clients
    /// keep the no-op default (a foreign machine's pids are meaningless
    /// here). A C-c'd process must not keep appearing in `/instances` or
    /// gate the dead-owner lease takeover for the rest of the 30s window.
    fn reap_dead_local_instances(&self) -> Result<usize> {
        Ok(0)
    }
}

impl HubStore for Store {
    fn register_instance(
        &self,
        record: &InstanceRecord,
        alive_ms: i64,
    ) -> Result<crate::store::RegisteredInstance> {
        Store::register_instance(self, record, alive_ms)
    }
    fn heartbeat(&self, node_id: &str, epoch: &str, now: i64, beat: &Heartbeat) -> Result<bool> {
        Store::heartbeat(self, node_id, epoch, now, beat)
    }
    fn list_instances(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        Store::list_instances(self, now, alive_ms)
    }
    fn get_instance(&self, node_id: &str) -> Result<Option<InstanceView>> {
        Store::get_instance(self, node_id)
    }
    /// `list_instances` filtered for same-host dead pids — what every
    /// command/tool should show. Default impl: local reaping + delegate to
    /// `list_instances`. The remote relay client overrides this: reaping is
    /// meaningless on the wrong machine, the relay host does it on its own
    /// worker tick.
    fn list_instances_alive(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        let _ = self.reap_dead_local_instances();
        self.list_instances(now, alive_ms)
    }

    /// `get_instance` filtered the same way (read-only; the dead same-host
    /// row is hidden, not deleted).
    fn get_instance_alive(&self, node_id: &str) -> Result<Option<InstanceView>> {
        let Some(view) = self.get_instance(node_id)? else {
            return Ok(None);
        };
        if view.host == crate::store::local_host_name() && !crate::store::pid_alive(view.pid) {
            return Ok(None);
        }
        Ok(Some(view))
    }

    /// Channel state carried with a lease (cursor etc. move atomically with
    /// ownership). None when there is no lease row or no state.
    fn channel_state(&self, _capability: &str) -> Option<serde_json::Value> {
        None
    }
    fn set_channel_state(&self, _capability: &str, _state: &serde_json::Value) {}
    /// Legacy kv cursor (v1 schema) — read-only migration source.
    fn legacy_cursor(&self) -> Option<String> {
        None
    }

    /// Drop presence rows of same-host pids that are demonstrably dead
    /// (`kill(pid, 0)` fails). Only the local Store reaps; the default (and
    /// the relay client's) is a no-op because a foreign machine's pids are
    /// meaningless here.
    fn reap_dead_local_instances(&self) -> Result<usize> {
        Store::reap_dead_local_instances(self)
    }
    fn unregister_instance(&self, node_id: &str, epoch: &str) -> Result<bool> {
        Store::unregister_instance(self, node_id, epoch)
    }
    fn claim_lease(
        &self,
        capability: &str,
        node_id: &str,
        node_name: &str,
        epoch: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Claim> {
        Store::claim_lease(self, capability, node_id, node_name, epoch, now, ttl_ms)
    }
    fn renew_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
        now: i64,
        ttl_ms: i64,
    ) -> Result<bool> {
        Store::renew_lease(self, capability, node_id, epoch, generation, now, ttl_ms)
    }
    fn check_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
        now: i64,
    ) -> Result<bool> {
        Store::check_lease(self, capability, node_id, epoch, generation, now)
    }
    fn assign_lease(
        &self,
        capability: &str,
        to_node: &str,
        to_name: &str,
        owner_epoch: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64> {
        Store::assign_lease(self, capability, to_node, to_name, owner_epoch, now, ttl_ms)
    }
    fn release_lease(
        &self,
        capability: &str,
        node_id: &str,
        epoch: &str,
        generation: i64,
    ) -> Result<bool> {
        Store::release_lease(self, capability, node_id, epoch, generation)
    }
    fn lease_holder(&self, capability: &str) -> Result<Option<LeaseHolder>> {
        Store::lease_holder(self, capability)
    }
    fn list_leases(&self) -> Result<Vec<(String, LeaseHolder)>> {
        Store::list_leases(self)
    }
    fn send_envelope(&self, envelope: &Envelope) -> Result<bool> {
        Store::send_envelope(self, envelope)
    }
    fn inbox(&self, node_id: &str, now: i64, limit: i64) -> Result<Vec<Envelope>> {
        Store::inbox(self, node_id, now, limit)
    }
    fn ack_envelope(&self, id: &str, node_id: &str, now: i64) -> Result<bool> {
        Store::ack_envelope(self, id, node_id, now)
    }
    fn mark_seen(&self, id: &str, now: i64) -> Result<bool> {
        Store::mark_seen(self, id, now)
    }
    fn prune(&self, now: i64, alive_ms: i64) -> Result<PruneReport> {
        Store::prune(self, now, alive_ms)
    }
    fn stats(&self, now: i64) -> Result<Stats> {
        Store::stats(self, now)
    }
}

// ---------------------------------------------------------------------------
// RPC parameter structs (relay wire format)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterParams {
    pub record: InstanceRecord,
    pub alive_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatParams {
    pub node_id: String,
    pub epoch: String,
    pub now: i64,
    pub beat: Heartbeat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListParams {
    pub now: i64,
    pub alive_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeParams {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnregisterParams {
    pub node_id: String,
    pub epoch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimParams {
    pub capability: String,
    pub node_id: String,
    pub node_name: String,
    pub epoch: String,
    pub now: i64,
    pub ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenewParams {
    pub capability: String,
    pub node_id: String,
    pub epoch: String,
    pub generation: i64,
    pub now: i64,
    pub ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckParams {
    pub capability: String,
    pub node_id: String,
    pub epoch: String,
    pub generation: i64,
    pub now: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignParams {
    pub capability: String,
    pub to_node: String,
    pub to_name: String,
    pub owner_epoch: String,
    pub now: i64,
    pub ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseParams {
    pub capability: String,
    pub node_id: String,
    pub epoch: String,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityParams {
    pub capability: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendParams {
    pub envelope: Envelope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxParams {
    pub node_id: String,
    pub now: i64,
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AckParams {
    pub id: String,
    pub node_id: String,
    pub now: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeenParams {
    pub id: String,
    pub now: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneParams {
    pub now: i64,
    pub alive_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsParams {
    pub now: i64,
}
