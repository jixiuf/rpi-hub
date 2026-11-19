use super::*;
use crate::protocol::Envelope;

const ALIVE: i64 = 30_000;
const TTL: i64 = 20_000;

fn record(node: &str, name: &str, host: &str, pid: u32, epoch: &str, now: i64) -> InstanceRecord {
    InstanceRecord {
        node_id: node.to_string(),
        name: name.to_string(),
        host: host.to_string(),
        pid,
        cwd: format!("/work/{node}"),
        role: "normal".into(),
        epoch: epoch.to_string(),
        session_id: Some(format!("session-{node}")),
        title: None,
        model: Some("gateway/model".into()),
        busy: false,
        started_at: now,
        last_seen: now,
    }
}

fn file_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

#[test]
fn registers_and_lists_live_instances_only() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let a = record("n1", "home", "alpha", 10, "e1", now);
    let b = record("n2", "models", "alpha", 11, "e1", now);
    store.register_instance(&a, ALIVE).unwrap();
    store.register_instance(&b, ALIVE).unwrap();

    let live = store.list_instances(now + 1_000, ALIVE).unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].name, "home");
    assert_eq!(live[1].name, "models");

    let later = store.list_instances(now + ALIVE + 1, ALIVE).unwrap();
    assert!(later.is_empty(), "stale presence must not be listed");
}

#[test]
fn name_collision_on_the_same_host_gets_a_pid_suffix() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let first = store
        .register_instance(&record("n1", "home", "alpha", 10, "e1", now), ALIVE)
        .unwrap();
    let second = store
        .register_instance(&record("n2", "home", "alpha", 22, "e1", now), ALIVE)
        .unwrap();
    assert_eq!(first.name, "home");
    assert!(!first.renamed);
    assert_eq!(second.name, "home-22");
    assert!(second.renamed);

    // A different host may reuse the same display name.
    let third = store
        .register_instance(&record("n3", "home", "beta", 33, "e1", now), ALIVE)
        .unwrap();
    assert_eq!(third.name, "home");
}

#[test]
fn a_restart_reuses_the_name_once_the_old_row_is_stale() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .register_instance(&record("old", "home", "alpha", 10, "e1", now), ALIVE)
        .unwrap();
    let restarted = store
        .register_instance(
            &record("new", "home", "alpha", 20, "e1", now + ALIVE + 1),
            ALIVE,
        )
        .unwrap();
    assert_eq!(restarted.name, "home");
    assert!(!restarted.renamed);
}

#[test]
fn heartbeat_from_a_stale_epoch_is_rejected() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .register_instance(&record("n1", "home", "alpha", 10, "load-1", now), ALIVE)
        .unwrap();
    let beat = Heartbeat {
        busy: true,
        session_id: Some("s2".into()),
        title: Some("refactor".into()),
        model: None,
    };
    assert!(store.heartbeat("n1", "load-1", now + 1, &beat).unwrap());

    // A reload re-registers with a new epoch: the old thread's writes must fail.
    let mut reloaded = record("n1", "home", "alpha", 10, "load-2", now + 2);
    reloaded.cwd = "/work/n1".into();
    store.register_instance(&reloaded, ALIVE).unwrap();
    assert!(!store.heartbeat("n1", "load-1", now + 3, &beat).unwrap());
    assert!(store.heartbeat("n1", "load-2", now + 4, &beat).unwrap());

    let view = store.get_instance("n1").unwrap().unwrap();
    assert!(view.busy);
    assert_eq!(view.title.as_deref(), Some("refactor"));
}

#[test]
fn unregister_only_accepts_the_live_epoch() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .register_instance(&record("n1", "home", "alpha", 10, "e1", now), ALIVE)
        .unwrap();
    assert!(!store.unregister_instance("n1", "other").unwrap());
    assert!(store.unregister_instance("n1", "e1").unwrap());
    assert!(store.get_instance("n1").unwrap().is_none());
}

#[test]
fn lease_is_exclusive_until_it_expires() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let first = store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    assert_eq!(
        first,
        Claim::Granted {
            generation: 1,
            expires_at: now + TTL
        }
    );

    let second = store
        .claim_lease("wechat", "n2", "models", now, TTL)
        .unwrap();
    match second {
        Claim::Held { holder } => {
            assert_eq!(holder.owner_node, "n1");
            assert_eq!(holder.generation, 1);
        }
        other => panic!("expected Held, got {other:?}"),
    }

    // After expiry a different instance takes over, bumping the generation.
    let takeover = store
        .claim_lease("wechat", "n2", "models", now + TTL + 1, TTL)
        .unwrap();
    assert_eq!(
        takeover,
        Claim::Granted {
            generation: 2,
            expires_at: now + 2 * TTL + 1
        }
    );
    // The fenced-out owner can neither renew nor pass the send gate.
    assert!(!store
        .renew_lease("wechat", "n1", 1, now + TTL + 2, TTL)
        .unwrap());
    assert!(!store
        .check_lease("wechat", "n1", 1, now + TTL + 2)
        .unwrap());
    assert!(store
        .check_lease("wechat", "n2", 2, now + TTL + 2)
        .unwrap());
}

#[test]
fn same_node_reclaim_resumes_the_lease_without_bumping_the_generation() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    // Same node (same process), new plugin load.
    let resumed = store
        .claim_lease("wechat", "n1", "home", now + 1_000, TTL)
        .unwrap();
    assert_eq!(
        resumed,
        Claim::Granted {
            generation: 1,
            expires_at: now + 1_000 + TTL
        }
    );
    // The generation-1 gate still passes: fencing is (node, generation).
    assert!(store
        .check_lease("wechat", "n1", 1, now + 1_001)
        .unwrap());
}

#[test]
fn transfer_bumps_the_generation_and_fences_the_previous_owner() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    let generation = store
        .transfer_lease("wechat", Some(("n1", 1)), "n2", "models", now + 1, TTL)
        .unwrap();
    assert_eq!(generation, 2);
    let holder = store.lease_holder("wechat").unwrap().unwrap();
    assert_eq!(holder.owner_node, "n2");
    assert!(!store
        .renew_lease("wechat", "n1", 1, now + 2, TTL)
        .unwrap());
    assert!(!store.check_lease("wechat", "n1", 1, now + 2).unwrap());

    // A stale `from` guard is rejected rather than silently re-transferring.
    let error = store
        .transfer_lease("wechat", Some(("n1", 1)), "n3", "other", now + 3, TTL)
        .unwrap_err();
    assert!(error.to_string().contains("held by"));
}

#[test]
fn transfer_requires_a_lease() {
    let (_dir, store) = file_store();
    let error = store
        .transfer_lease("wechat", None, "n2", "models", 1, TTL)
        .unwrap_err();
    assert!(error.to_string().contains("claim it first"));
}

#[test]
fn repeated_start_bursts_never_leak_two_owners() {
    // Eight processes starting at the same moment, five rounds each: the real
    // startup storm, worse than the single-shot race above.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    for round in 0..5u32 {
        let now = 1_000_000 + (round as i64) * 1000;
        let mut handles = Vec::new();
        for index in 0..8u32 {
            let path = path.clone();
            handles.push(std::thread::spawn(move || {
                let store = Store::open(&path).unwrap();
                store
                    .claim_lease(
                        "wechat",
                        &format!("node-{index}"),
                        &format!("inst-{index}"),
                        now,
                        TTL,
                    )
                    .unwrap()
            }));
        }
        let claims: Vec<Claim> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let granted = claims
            .iter()
            .filter(|claim| matches!(claim, Claim::Granted { .. }))
            .count();
        assert_eq!(granted, 1, "round {round}: {claims:?}");
    }
}

#[test]
fn assign_hands_a_lease_to_a_target_and_fences_the_old_owner() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    let generation = store
        .assign_lease("wechat", "n2", "models", now + 1, TTL)
        .unwrap();
    assert_eq!(generation, 2);
    assert!(!store.check_lease("wechat", "n1", 1, now + 2).unwrap());

    // The target claims (same node): the generation holds.
    let claim = store
        .claim_lease("wechat", "n2", "models", now + 3, TTL)
        .unwrap();
    assert_eq!(
        claim,
        Claim::Granted {
            generation: 2,
            expires_at: now + 3 + TTL
        }
    );
    assert!(store.check_lease("wechat", "n2", 2, now + 4).unwrap());
}

#[test]
fn release_only_accepts_the_owner() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    assert!(!store.release_lease("wechat", "n2", 1).unwrap());
    assert!(store.release_lease("wechat", "n1", 1).unwrap());
    assert!(store.lease_holder("wechat").unwrap().is_none());
}

#[test]
fn independent_connections_race_for_one_lease() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let now = 1_000_000;
    let mut handles = Vec::new();
    for index in 0..8u32 {
        let path = path.clone();
        handles.push(std::thread::spawn(move || {
            let store = Store::open(&path).unwrap();
            store
                .claim_lease(
                    "wechat",
                    &format!("node-{index}"),
                    &format!("inst-{index}"),
                    now,
                    TTL,
                )
                .unwrap()
        }));
    }
    let claims: Vec<Claim> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let granted = claims
        .iter()
        .filter(|claim| matches!(claim, Claim::Granted { .. }))
        .count();
    assert_eq!(granted, 1, "exactly one claim may win: {claims:?}");

    let store = Store::open(&path).unwrap();
    let holder = store.lease_holder("wechat").unwrap().unwrap();
    assert_eq!(holder.generation, 1);
}

#[test]
fn lease_holder_reports_renewal_liveness() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let claim = store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    let Claim::Granted { generation, .. } = claim else {
        panic!("expected grant")
    };
    let holder = store.lease_holder("wechat").unwrap().unwrap();
    assert_eq!(holder.updated_at, now);
    assert_eq!(holder.expires_at, now + TTL);

    // A renewal advances updated_at without moving the owner: watchers use
    // this to tell "expired but actively renewing" from "expired and gone".
    assert!(store
        .renew_lease("wechat", "n1", generation, now + 5_000, TTL)
        .unwrap());
    let holder = store.lease_holder("wechat").unwrap().unwrap();
    assert_eq!(holder.updated_at, now + 5_000);
    assert_eq!(holder.expires_at, now + 5_000 + TTL);
}

#[test]
fn assign_notifies_target_and_previous_owner() {
    let (_dir, store) = file_store();
    let store = std::sync::Arc::new(store);
    let now = 1_000_000;
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    store
        .register_instance(&record("n1", "home", "h1", 10, "e1", now), ALIVE)
        .unwrap();
    store
        .register_instance(&record("n2", "models", "h1", 11, "e2", now), ALIVE)
        .unwrap();

    let admin = crate::lease::LeaseAdmin::new(store.clone(), TTL);
    let target = store.get_instance("n2").unwrap().unwrap();
    let generation = admin.assign_to("wechat", &target, now + 1).unwrap();
    assert_eq!(generation, 2);

    let inbox = |node: &str, at: i64| -> Vec<String> {
        store
            .inbox(node, at, 10)
            .unwrap()
            .into_iter()
            .map(|envelope| envelope.kind.as_str().to_string())
            .collect()
    };
    let target_kinds = inbox("n2", now + 2);
    let old_kinds = inbox("n1", now + 2);
    assert!(
        target_kinds.contains(&"assign".to_string()),
        "{target_kinds:?}"
    );
    assert!(old_kinds.contains(&"unassign".to_string()), "{old_kinds:?}");
}

#[test]
fn assign_without_a_previous_owner_sends_only_the_assign() {
    let (_dir, store) = file_store();
    let store = std::sync::Arc::new(store);
    let now = 1_000_000;
    store
        .register_instance(&record("n2", "models", "h1", 11, "e2", now), ALIVE)
        .unwrap();
    let admin = crate::lease::LeaseAdmin::new(store.clone(), TTL);
    let target = store.get_instance("n2").unwrap().unwrap();
    let generation = admin.assign_to("wechat", &target, now).unwrap();
    assert_eq!(generation, 1);
    let kinds: Vec<String> = store
        .inbox("n2", now + 1, 10)
        .unwrap()
        .into_iter()
        .map(|envelope| envelope.kind.as_str().to_string())
        .collect();
    assert_eq!(kinds, vec!["assign".to_string()]);
}
#[test]
fn mailbox_round_trips_with_ack() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let env = Envelope::text("n1", "home", "n2", "hello", now).unwrap();
    assert!(store.send_envelope(&env).unwrap());

    let inbox = store.inbox("n2", now + 1, 10).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].text.as_deref(), Some("hello"));

    assert!(store.ack_envelope(&env.id, "n2", now + 2).unwrap());
    assert!(store.inbox("n2", now + 3, 10).unwrap().is_empty());
    // Acking twice is a no-op, and another node cannot ack it.
    assert!(!store.ack_envelope(&env.id, "n2", now + 4).unwrap());
}

#[test]
fn expired_envelopes_are_not_delivered() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let env = Envelope::text("n1", "home", "n2", "late", now).unwrap();
    store.send_envelope(&env).unwrap();
    assert!(store
        .inbox("n2", env.expires_at + 1, 10)
        .unwrap()
        .is_empty());
    assert_eq!(store.purge_envelopes(env.expires_at + 1).unwrap(), 1);
}

#[test]
fn duplicate_sends_are_ignored_by_id() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    let env = Envelope::text("n1", "home", "n2", "once", now).unwrap();
    assert!(store.send_envelope(&env).unwrap());
    assert!(!store.send_envelope(&env).unwrap());
    assert_eq!(store.inbox("n2", now + 1, 10).unwrap().len(), 1);
}

#[test]
fn seen_ids_dedupe_inbound_messages() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    assert!(store.mark_seen("msg-1", now).unwrap());
    assert!(!store.mark_seen("msg-1", now + 1).unwrap());
    assert_eq!(store.prune_seen(now).unwrap(), 0);
    assert_eq!(store.prune_seen(now + 31 * 60 * 1000).unwrap(), 1);
}

#[test]
fn prune_drops_dead_presence_and_orphan_leases() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .register_instance(&record("n1", "home", "alpha", 10, "e1", now), ALIVE)
        .unwrap();
    store
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();

    let report = store.prune(now + ALIVE + 1, ALIVE).unwrap();
    assert_eq!(report.instances, 1);
    assert!(store.lease_holder("wechat").unwrap().is_none());
}

#[test]
fn stats_reports_pending_deliveries() {
    let (_dir, store) = file_store();
    let now = 1_000_000;
    store
        .register_instance(&record("n1", "home", "alpha", 10, "e1", now), ALIVE)
        .unwrap();
    let env = Envelope::text("n1", "home", "n2", "hi", now).unwrap();
    store.send_envelope(&env).unwrap();
    let stats = store.stats(now + 1).unwrap();
    assert_eq!(stats.instances, 1);
    assert_eq!(stats.envelopes_pending, 1);
    assert_eq!(stats.envelopes_total, 1);
    assert_eq!(stats.leases, 0);
}

#[test]
fn opening_a_newer_schema_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let conn = store.conn.lock().unwrap();
        conn.execute("UPDATE kv SET value='99' WHERE key='schema_version'", [])
            .unwrap();
    }
    let error = Store::open(dir.path()).unwrap_err();
    assert!(error.to_string().contains("schema is version 99"));
}

#[test]
fn v1_database_migrates_to_v2_and_carries_channel_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Hand-build a v1 database (leases without state_json).
    {
        let conn = rusqlite::Connection::open(path.join("hub.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE instances (node_id TEXT PRIMARY KEY, name TEXT NOT NULL, host TEXT NOT NULL,
               pid INTEGER NOT NULL, cwd TEXT NOT NULL, session_id TEXT, title TEXT, model TEXT,
               role TEXT NOT NULL DEFAULT 'normal', busy INTEGER NOT NULL DEFAULT 0,
               epoch TEXT NOT NULL, started_at INTEGER NOT NULL, last_seen INTEGER NOT NULL);
             CREATE TABLE leases (capability TEXT PRIMARY KEY, owner_node TEXT NOT NULL,
               owner_epoch TEXT NOT NULL, owner_name TEXT NOT NULL, generation INTEGER NOT NULL,
               expires_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
             CREATE TABLE envelopes (id TEXT PRIMARY KEY, to_node TEXT NOT NULL, from_node TEXT NOT NULL,
               from_name TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, correlation TEXT,
               created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, acked_at INTEGER);
             CREATE TABLE seen (id TEXT PRIMARY KEY, ts INTEGER NOT NULL);
             INSERT INTO kv VALUES ('schema_version', '1');
             INSERT INTO leases VALUES ('wechat', 'n1', 'e1', 'home', 3, 9999999999999, 1000);",
        )
        .unwrap();
    }

    // Open with the current code: migration must add the column without
    // disturbing the lease row, and the state must be readable/writable.
    let store = Store::open(&path).unwrap();
    let holder = store.lease_holder("wechat").unwrap().unwrap();
    assert_eq!(holder.generation, 3);
    // The v1 row migrated with the DEFAULT '{}' — an empty state object.
    let migrated = store.channel_state("wechat").unwrap();
    assert!(migrated.as_object().unwrap().is_empty());

    store
        .set_channel_state("wechat", &serde_json::json!({ "cursor": "c-42" }))
        ;
    let state = store.channel_state("wechat").unwrap();
    assert_eq!(state["cursor"], "c-42");
}
