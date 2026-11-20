//! The multi-process mesh store.
//!
//! Everything the mesh needs is four tables in one SQLite database (WAL mode):
//!
//! | table | purpose |
//! | --- | --- |
//! | `instances` | presence: who is running, where, with which session |
//! | `leases` | which instance owns a channel (`wechat`), with fencing |
//! | `envelopes` | the mailbox: ack'd point-to-point deliveries |
//! | `seen` | inbound dedupe for IM message ids |
//!
//! Concurrency model: one `Connection` per rpi process (each running one mesh
//! plugin), `busy_timeout`, and short `BEGIN IMMEDIATE` transactions for
//! read-modify-write operations. There is no daemon, no socket and no lock
//! file: two `rpi` processes in the same project on the same machine simply
//! open the same file.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::api::MeshStore;
use crate::protocol::{Envelope, MAX_TEXT_BYTES};
use crate::{names, paths, MeshError, Result};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instances (
    node_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    host       TEXT NOT NULL,
    pid        INTEGER NOT NULL,
    cwd        TEXT NOT NULL,
    session_id TEXT,
    title      TEXT,
    model      TEXT,
    role       TEXT NOT NULL DEFAULT 'normal',
    busy       INTEGER NOT NULL DEFAULT 0,
    epoch      TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS instances_name_host ON instances(name, host);

CREATE TABLE IF NOT EXISTS leases (
    capability  TEXT PRIMARY KEY,
    owner_node  TEXT NOT NULL,
    owner_name  TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    state_json  TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS envelopes (
    id          TEXT PRIMARY KEY,
    to_node     TEXT NOT NULL,
    from_node   TEXT NOT NULL,
    from_name   TEXT NOT NULL,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL,
    correlation TEXT,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    acked_at    INTEGER
);
CREATE INDEX IF NOT EXISTS envelopes_inbox ON envelopes(to_node, acked_at, created_at);
CREATE INDEX IF NOT EXISTS envelopes_correlation ON envelopes(correlation);

CREATE TABLE IF NOT EXISTS seen (
    id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS seen_ts ON seen(ts);
"#;

pub const SCHEMA_VERSION: i64 = 3;

/// How the store is opened: a file (normal) or memory (tests).
pub struct Store {
    conn: Mutex<Connection>,
    dir: PathBuf,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("dir", &self.dir).finish()
    }
}

/// A presence record as registered by a plugin instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceRecord {
    pub node_id: String,
    pub name: String,
    pub host: String,
    pub pid: u32,
    pub cwd: String,
    /// `normal` or `subagent`.
    pub role: String,
    /// Capabilities this instance can serve, e.g. `{"wechat":{"userId":"…"}}`.
    /// Published on every heartbeat so `/use` can verify the target BEFORE
    /// handing over a channel.
    pub channels: serde_json::Value,
    /// Per-load uuid; a `/reload` produces a new epoch for the same `node_id`.
    pub epoch: String,
    pub session_id: Option<String>,
    pub title: Option<String>,
    pub model: Option<String>,
    pub busy: bool,
    pub started_at: i64,
    pub last_seen: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredInstance {
    pub node_id: String,
    pub name: String,
    pub host: String,
    /// True when a name collision forced a `-<pid>` suffix.
    pub renamed: bool,
}

/// One live instance, as listed by `/instances`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceView {
    pub node_id: String,
    pub name: String,
    pub host: String,
    pub pid: u32,
    pub cwd: String,
    pub role: String,
    pub channels: serde_json::Value,
    pub session_id: Option<String>,
    pub title: Option<String>,
    pub model: Option<String>,
    pub busy: bool,
    pub started_at: i64,
    pub last_seen: i64,
}

impl InstanceView {
    pub fn label(&self) -> String {
        format!("{}@{}", self.name, self.host)
    }
}

/// Fields refreshed on every heartbeat.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Heartbeat {
    pub busy: bool,
    pub session_id: Option<String>,
    pub title: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseHolder {
    pub owner_node: String,
    pub owner_name: String,
    /// The holder's machine, joined from `instances` (empty when the holder
    /// is not currently present — the name then shows without @host).
    pub owner_host: String,
    pub generation: i64,
    pub expires_at: i64,
    /// Last successful renewal by the owner: the liveness signal that
    /// distinguishes "expired but actively serving" from "expired and gone".
    pub updated_at: i64,
}

impl LeaseHolder {
    /// `name@host` when the host is known, else the bare name.
    pub fn full_name(&self) -> String {
        if self.owner_host.is_empty() {
            self.owner_name.clone()
        } else {
            format!("{}@{}", self.owner_name, self.owner_host)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Claim {
    Granted { generation: i64, expires_at: i64 },
    Held { holder: LeaseHolder },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneReport {
    pub instances: usize,
    pub leases: usize,
    pub envelopes: usize,
    pub seen: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub instances: i64,
    pub envelopes_pending: i64,
    pub envelopes_total: i64,
    pub leases: i64,
}

impl Store {
    /// Open (and create) the store in `dir`.
    ///
    /// Two instances starting at the same moment both try to switch a brand-new
    /// database into WAL mode, and SQLite may return `SQLITE_BUSY` for that
    /// pragma *without* honouring `busy_timeout`. Retry the whole open on lock
    /// contention instead of disabling the plugin on a startup race.
    pub fn open(dir: &Path) -> Result<Self> {
        paths::ensure_private_dir(dir)?;
        // At-rest key: env or <dir>/secret.key (generated on first use).
        crate::secret::init(dir).map_err(|e| MeshError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let path = paths::db_path(dir);
        let mut last: Option<MeshError> = None;
        for attempt in 0..40u64 {
            match Self::open_once(dir, &path) {
                Ok(store) => {
                    paths::restrict_permissions(&path, 0o600)?;
                    return Ok(store);
                }
                Err(error) => {
                    if !is_lock_contention(&error) {
                        return Err(error);
                    }
                    last = Some(error);
                }
            }
            std::thread::sleep(Duration::from_millis(25 + 25 * attempt.min(4)));
        }
        Err(last.unwrap_or_else(|| MeshError::invalid("could not open the mesh database")))
    }

    fn open_once(dir: &Path, path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::configure(&conn)?;
        let store = Self {
            conn: Mutex::new(conn),
            dir: dir.to_path_buf(),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    /// In-memory store for tests.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn)?;
        let store = Self {
            conn: Mutex::new(conn),
            dir: PathBuf::new(),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn configure(conn: &Connection) -> Result<()> {
        conn.busy_timeout(Duration::from_millis(3_000))?;
        // WAL + synchronous=FULL: multiple rpi processes (one mesh plugin
        // each) share this database across frequent hard exits (TUI
        // restarts, SIGKILL). NORMAL can leave torn WAL frames on abrupt
        // death; FULL makes every commit durable so a kill never corrupts
        // the database (writes are low-frequency — the cost is negligible).
        conn.pragma_update(None, "synchronous", "FULL")?;
        // WAL is what makes concurrent readers/writers from several plugin
        // processes safe; `memory` is returned for the in-memory test store.
        // Skip the switch when it is already on: the pragma needs a brief
        // exclusive lock and fails with SQLITE_BUSY during a startup race.
        let current: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        if !current.eq_ignore_ascii_case("wal") {
            let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        }
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(())
    }

    fn ensure_schema(&self) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute_batch(SCHEMA)?;
        // v1 → v2: leases gained `state_json` (channel cursor and friends).
        // CREATE TABLE IF NOT EXISTS does not alter an existing table, so
        // migrate explicitly AFTER the (idempotent) create; re-running is a
        // no-op and fresh databases already have the column.
        let has_state_json: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('leases') WHERE name = 'state_json'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if has_state_json == 0 {
            conn.execute_batch(
                "ALTER TABLE leases ADD COLUMN state_json TEXT NOT NULL DEFAULT '{}'",
            )?;
        }
        // owner_epoch was a double-dylib artifact: each plugin load minted its
        // own epoch and the lease row fenced on it. The single-plugin mesh has
        // one identity per process and in-process thread lifecycle, so
        // (owner_node, generation) is the complete fence. Drop the column.
        let has_owner_epoch: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('leases') WHERE name = 'owner_epoch'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if has_owner_epoch != 0 {
            // SQLite 3.35+ supports DROP COLUMN; bundled rusqlite ships newer.
            conn.execute_batch("ALTER TABLE leases DROP COLUMN owner_epoch")?;
        }
        // v3: instances gained `channels` (which capabilities this instance
        // can actually serve — e.g. wechat credentials present).
        let has_channels: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('instances') WHERE name = 'channels'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if has_channels == 0 {
            // Racing openers can both pass the probe and both ALTER; SQLite
            // rejects the second with "duplicate column name" — that is a
            // success (the column now exists), not a failure.
            if let Err(error) = conn.execute_batch(
                "ALTER TABLE instances ADD COLUMN channels TEXT NOT NULL DEFAULT '{}'",
            ) {
                let duplicate = error
                    .to_string()
                    .contains("duplicate column name: channels");
                if !duplicate {
                    return Err(error.into());
                }
            }
        }
        // v3 also switches state_json to encrypted-at-rest: re-encrypt any
        // plaintext rows left by the previous layout (values that are valid
        // base64 ciphertext stay untouched — GCM decryption would fail on
        // '{' anyway, which is how plaintext is detected).
        {
            let rows: Vec<(String, String)> = {
                let mut statement = conn
                    .prepare("SELECT capability, state_json FROM leases WHERE state_json != ''")
                    .unwrap();
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .unwrap();
                rows.collect::<rusqlite::Result<Vec<_>>>().unwrap()
            };
            for (capability, state_json) in rows {
                // Plaintext JSON starts with '{'; ciphertext starts with base64.
                // Encrypt anything that is not already our ciphertext format.
                let looks_encrypted = crate::secret::decrypt(&state_json).is_ok();
                if !looks_encrypted {
                    let encrypted = crate::secret::encrypt(&state_json);
                    let _ = conn.execute(
                        "UPDATE leases SET state_json = ?2 WHERE capability = ?1",
                        rusqlite::params![capability, encrypted],
                    );
                }
            }
        }
        let found: Option<String> = conn
            .query_row(
                "SELECT value FROM kv WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        match found {
            None => {
                // Racing openers both see "no row"; `INSERT OR IGNORE` lets one
                // win and the rest proceed instead of aborting the startup.
                conn.execute(
                    "INSERT OR IGNORE INTO kv(key, value) VALUES ('schema_version', ?1)",
                    params![SCHEMA_VERSION.to_string()],
                )?;
            }
            Some(version) => {
                let version: i64 = version.parse().unwrap_or(0);
                if version > SCHEMA_VERSION {
                    return Err(MeshError::invalid(format!(
                        "mesh database schema is version {version}, this build understands {SCHEMA_VERSION}; upgrade rpi-mesh"
                    )));
                }
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------- presence

    /// Upsert this instance and resolve a display name that is unique per host.
    ///
    /// Dead rows on this host are dropped first, so restarting an instance
    /// usually reuses its old name instead of collecting a `-<pid>` suffix.
    pub fn register_instance(
        &self,
        record: &InstanceRecord,
        alive_ms: i64,
    ) -> Result<RegisteredInstance> {
        retry_busy(|| self.register_instance_once(record, alive_ms))
    }

    fn register_instance_once(
        &self,
        record: &InstanceRecord,
        alive_ms: i64,
    ) -> Result<RegisteredInstance> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM instances WHERE host = ?1 AND last_seen < ?2",
            params![record.host, record.last_seen - alive_ms.max(0)],
        )?;

        let mut chosen = names::sanitize(&record.name);
        let mut renamed = false;
        for candidate in names::candidates(&record.name, record.pid) {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT node_id FROM instances WHERE name = ?1 AND host = ?2",
                    params![candidate, record.host],
                    |row| row.get(0),
                )
                .optional()?;
            match existing {
                Some(node_id) if node_id != record.node_id => renamed = true,
                _ => {
                    chosen = candidate;
                    break;
                }
            }
        }

        let channels_text = serde_json::to_string(&record.channels)
            .unwrap_or_else(|_| "{}".to_string());
        tx.execute(
            r#"INSERT INTO instances
                 (node_id, name, host, pid, cwd, session_id, title, model, role, busy, epoch, started_at, last_seen, channels)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
               ON CONFLICT(node_id) DO UPDATE SET
                 name = excluded.name,
                 host = excluded.host,
                 pid = excluded.pid,
                 cwd = excluded.cwd,
                 session_id = excluded.session_id,
                 title = excluded.title,
                 model = excluded.model,
                 role = excluded.role,
                 busy = excluded.busy,
                 epoch = excluded.epoch,
                 last_seen = excluded.last_seen,
                 channels = excluded.channels"#,
            params![
                record.node_id,
                chosen,
                record.host,
                record.pid as i64,
                record.cwd,
                record.session_id,
                record.title,
                record.model,
                record.role,
                record.busy as i64,
                record.epoch,
                record.started_at,
                record.last_seen,
                channels_text,
            ],
        )?;
        tx.commit()?;

        Ok(RegisteredInstance {
            node_id: record.node_id.clone(),
            name: chosen,
            host: record.host.clone(),
            renamed,
        })
    }

    /// Refresh presence. Returns `false` when the epoch is stale — i.e. this
    /// thread belongs to a plugin load that a `/reload` already replaced.
    pub fn heartbeat(
        &self,
        node_id: &str,
        epoch: &str,
        now: i64,
        beat: &Heartbeat,
    ) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            r#"UPDATE instances SET
                 last_seen = ?3,
                 busy = ?4,
                 session_id = COALESCE(?5, session_id),
                 title = COALESCE(?6, title),
                 model = COALESCE(?7, model)
               WHERE node_id = ?1 AND epoch = ?2"#,
            params![
                node_id,
                epoch,
                now,
                beat.busy as i64,
                beat.session_id,
                beat.title,
                beat.model,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn list_instances(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn.prepare(
            r#"SELECT node_id, name, host, pid, cwd, session_id, title, model, role, busy, started_at, last_seen, channels
               FROM instances WHERE last_seen >= ?1 ORDER BY name, host"#,
        )?;
        let rows = statement.query_map(params![now - alive_ms.max(0)], |row| {
            let channels_text: String = row.get(12)?;
            Ok(InstanceView {
                node_id: row.get(0)?,
                name: row.get(1)?,
                host: row.get(2)?,
                pid: row.get::<_, i64>(3)? as u32,
                cwd: row.get(4)?,
                session_id: row.get(5)?,
                title: row.get(6)?,
                model: row.get(7)?,
                role: row.get(8)?,
                busy: row.get::<_, i64>(9)? != 0,
                started_at: row.get(10)?,
                last_seen: row.get(11)?,
                channels: serde_json::from_str(&channels_text).unwrap_or_default(),
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Drop presence rows of same-host pids that are demonstrably dead
    /// (`kill(pid, 0)` fails). C-c/SIGKILL leaves a row that would otherwise
    /// linger for the full 30s heartbeat window — long enough to keep
    /// appearing in `/instances` and to gate the dead-owner lease takeover.
    /// Only same-host rows are checked: cross-machine pids are meaningless
    /// locally. Cleanup-only; pure reads go through the filtered views.
    pub fn reap_dead_local_instances(&self) -> Result<usize> {
        let rows: Vec<(String, String, i64)> = {
            let conn = self.conn.lock().expect("store mutex poisoned");
            let mut statement = conn.prepare("SELECT node_id, host, pid FROM instances")?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let host = local_host_name();
        let mut dropped = 0;
        for (node_id, row_host, pid) in rows {
            if row_host != host || pid <= 0 {
                continue;
            }
            if !pid_alive(pid.max(0) as u32) {
                let conn = self.conn.lock().expect("store mutex poisoned");
                dropped +=
                    conn.execute("DELETE FROM instances WHERE node_id = ?1", params![node_id])?;
            }
        }
        Ok(dropped)
    }


    /// Concrete-`Store` convenience forwards to the `MeshStore` defaults so
    /// both plugins (wechat holds `Arc<Store>`, mesh holds `Arc<dyn MeshStore>`)
    /// get the identical alive-filtered views.
    pub fn list_instances_alive(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        MeshStore::list_instances_alive(self, now, alive_ms)
    }

    pub fn get_instance_alive(&self, node_id: &str) -> Result<Option<InstanceView>> {
        MeshStore::get_instance_alive(self, node_id)
    }

    pub fn get_instance(&self, node_id: &str) -> Result<Option<InstanceView>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let view = conn
            .query_row(
                r#"SELECT node_id, name, host, pid, cwd, session_id, title, model, role, busy, started_at, last_seen, channels
                   FROM instances WHERE node_id = ?1"#,
                params![node_id],
                |row| {
                    let channels_text: String = row.get(12)?;
                    Ok(InstanceView {
                        node_id: row.get(0)?,
                        name: row.get(1)?,
                        host: row.get(2)?,
                        pid: row.get::<_, i64>(3)? as u32,
                        cwd: row.get(4)?,
                        session_id: row.get(5)?,
                        title: row.get(6)?,
                        model: row.get(7)?,
                        role: row.get(8)?,
                        busy: row.get::<_, i64>(9)? != 0,
                        started_at: row.get(10)?,
                        last_seen: row.get(11)?,
                        channels: serde_json::from_str(&channels_text).unwrap_or_default(),
                    })
                },
            )
            .optional()?;
        Ok(view)
    }

    /// Remove this instance when its plugin unloads. Only the live epoch may.
    pub fn unregister_instance(&self, node_id: &str, epoch: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "DELETE FROM instances WHERE node_id = ?1 AND epoch = ?2",
            params![node_id, epoch],
        )?;
        Ok(changed == 1)
    }

    // ------------------------------------------------------------------ leases

    /// Acquire or renew `capability`.
    ///
    /// - free / expired lease → granted, generation + 1 when the owner changes
    /// - same owner + same epoch → renewal (generation unchanged)
    /// - same owner + new epoch (a `/reload` of the same process) → resumption
    ///   with the generation **unchanged**, so the fresh load continues the
    ///   lease instead of fencing itself out
    /// - live lease held by someone else → `Held`
    pub fn claim_lease(
        &self,
        capability: &str,
        node_id: &str,
        node_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Claim> {
        retry_busy(|| self.claim_lease_once(capability, node_id, node_name, now, ttl_ms))
    }

    fn claim_lease_once(
        &self,
        capability: &str,
        node_id: &str,
        node_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Claim> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String, i64, i64, i64)> = tx
            .query_row(
                "SELECT owner_node, owner_name, generation, expires_at, updated_at
                 FROM leases WHERE capability = ?1",
                params![capability],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let claim = match existing {
            None => {
                let generation = 1;
                let expires_at = now + ttl_ms;
                tx.execute(
                    "INSERT INTO leases(capability, owner_node, owner_name, generation, expires_at, updated_at, state_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}')",
                    params![capability, node_id, node_name, generation, expires_at, now],
                )?;
                Claim::Granted {
                    generation,
                    expires_at,
                }
            }
            Some((owner_node, owner_name, generation, expires_at, updated_at)) => {
                let holder_host = tx
                    .query_row(
                        "SELECT host FROM instances WHERE node_id = ?1",
                        params![owner_node],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap_or_default();
                let same_owner = owner_node == node_id;
                if same_owner {
                    // Same node re-claiming (process restart / owner loop
                    // resume): keep the generation, just extend the deadline.
                    let expires_at = now + ttl_ms;
                    tx.execute(
                        "UPDATE leases SET owner_name = ?2, expires_at = ?3, updated_at = ?4
                         WHERE capability = ?1",
                        params![capability, node_name, expires_at, now],
                    )?;
                    Claim::Granted {
                        generation,
                        expires_at,
                    }
                } else if expires_at <= now {
                    let generation = generation + 1;
                    let expires_at = now + ttl_ms;
                    tx.execute(
                        "UPDATE leases SET owner_node = ?2, owner_name = ?3,
                                generation = ?4, expires_at = ?5, updated_at = ?6
                         WHERE capability = ?1",
                        params![capability, node_id, node_name, generation, expires_at, now],
                    )?;
                    Claim::Granted {
                        generation,
                        expires_at,
                    }
                } else {
                    Claim::Held {
                        holder: LeaseHolder {
                            owner_node: owner_node.clone(),
                            owner_name,
                            owner_host: holder_host,
                            generation,
                            expires_at,
                            updated_at,
                        },
                    }
                }
            }
        };
        tx.commit()?;
        Ok(claim)
    }

    /// Extend a lease this process still owns. Owner, epoch and generation must
    /// all match; an expired-but-unclaimed lease may be renewed (nobody else
    /// took it, so there is no split brain).
    pub fn renew_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
        now: i64,
        ttl_ms: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "UPDATE leases SET expires_at = ?4, updated_at = ?5
             WHERE capability = ?1 AND owner_node = ?2 AND generation = ?3",
            params![capability, node_id, generation, now + ttl_ms, now],
        )?;
        Ok(changed == 1)
    }

    /// The gate every outbound channel send must pass: still owner, same
    /// generation, and the lease has not expired.
    pub fn check_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
        now: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM leases
             WHERE capability = ?1 AND owner_node = ?2 AND generation = ?3
               AND expires_at > ?4",
            params![capability, node_id, generation, now],
            |row| row.get(0),
        )?;
        Ok(count == 1)
    }

    /// Hand a lease to another instance. Bumps the generation, so the previous
    /// owner's `check_lease`/`renew_lease` fail immediately afterwards.
    ///
    /// `from` is `None` for a forced takeover by an operator command.
    pub fn transfer_lease(
        &self,
        capability: &str,
        from: Option<(&str, i64)>,
        to_node: &str,
        to_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String, i64)> = tx
            .query_row(
                "SELECT owner_node, owner_name, generation FROM leases WHERE capability = ?1",
                params![capability],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((owner_node, owner_name, generation)) = existing else {
            return Err(MeshError::invalid(format!(
                "no lease for capability {capability:?}; claim it first"
            )));
        };
        if let Some((from_node, from_gen)) = from {
            if from_node != owner_node || from_gen != generation {
                return Err(MeshError::invalid(format!(
                    "lease {capability:?} is held by {owner_name} (generation {generation})"
                )));
            }
        }
        let generation = generation + 1;
        tx.execute(
            "UPDATE leases SET owner_node = ?2, owner_name = ?3,
                    generation = ?4, expires_at = ?5, updated_at = ?6
             WHERE capability = ?1",
            params![
                capability,
                to_node,
                to_name,
                generation,
                now + ttl_ms,
                now
            ],
        )?;
        tx.commit()?;
        Ok(generation)
    }

    /// Place a lease on a specific instance without requiring the caller to
    /// hold it (`/use <target>`, or a channel hand-off decided out of band).
    ///
    /// The generation is bumped so the previous owner is fenced out immediately.
    pub fn assign_lease(
        &self,
        capability: &str,
        to_node: &str,
        to_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64> {
        retry_busy(|| self.assign_lease_once(capability, to_node, to_name, now, ttl_ms))
    }

    fn assign_lease_once(
        &self,
        capability: &str,
        to_node: &str,
        to_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<i64> = tx
            .query_row(
                "SELECT generation FROM leases WHERE capability = ?1",
                params![capability],
                |row| row.get(0),
            )
            .optional()?;
        let generation = existing.map(|generation| generation + 1).unwrap_or(1);
        tx.execute(
            r#"INSERT INTO leases(capability, owner_node, owner_name, generation, expires_at, updated_at, state_json)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}')
               ON CONFLICT(capability) DO UPDATE SET
                 owner_node = excluded.owner_node,
                 owner_name = excluded.owner_name,
                 generation = excluded.generation,
                 expires_at = excluded.expires_at,
                 updated_at = excluded.updated_at,
                 state_json = excluded.state_json"#,
            params![capability, to_node, to_name, generation, now + ttl_ms, now],
        )?;
        tx.commit()?;
        Ok(generation)
    }

    /// Channel state carried WITH the lease (`state_json` on the lease row):
    /// the cursor moves atomically with ownership, so a hand-off can never
    /// leave "new owner, old cursor" behind. Replaces the kv-based cursor
    /// (kept below read-only for one release's migration).
    pub fn channel_state(&self, capability: &str) -> Option<serde_json::Value> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let text: String = conn
            .query_row(
                "SELECT state_json FROM leases WHERE capability = ?1",
                params![capability],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten()?;
        // Empty = never set. Encrypted at rest (credentials are stored here).
        let text = crate::secret::decrypt(&text).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn set_channel_state(&self, capability: &str, state: &serde_json::Value) {
        let Ok(text) = serde_json::to_string(state) else {
            return;
        };
        // Encrypted at rest: wechat credentials are stored here.
        let text = crate::secret::encrypt(&text);
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                "UPDATE leases SET state_json = ?2 WHERE capability = ?1",
                params![capability, text],
            );
        }
    }

    /// Drop a lease this process owns (`/wechat stop`).
    pub fn release_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "DELETE FROM leases
             WHERE capability = ?1 AND owner_node = ?2 AND generation = ?3",
            params![capability, node_id, generation],
        )?;
        Ok(changed == 1)
    }

    pub fn lease_holder(&self, capability: &str) -> Result<Option<LeaseHolder>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        // LEFT JOIN instances for the holder's host: the lease can outlive
        // its holder's presence row (TTL 20s vs presence 30s windows), in
        // which case the name degrades to the bare form.
        let holder = conn
            .query_row(
                "SELECT l.owner_node, l.owner_name,
                        COALESCE(i.host, ''), l.generation, l.expires_at, l.updated_at
                   FROM leases l
                   LEFT JOIN instances i ON i.node_id = l.owner_node
                  WHERE l.capability = ?1",
                params![capability],
                |row| {
                    Ok(LeaseHolder {
                        owner_node: row.get(0)?,
                        owner_name: row.get(1)?,
                        owner_host: row.get(2)?,
                        generation: row.get(3)?,
                        expires_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .optional()?;
        Ok(holder)
    }

    pub fn list_leases(&self) -> Result<Vec<(String, LeaseHolder)>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn.prepare(
            "SELECT l.capability, l.owner_node, l.owner_name,
                    COALESCE(i.host, ''), l.generation, l.expires_at, l.updated_at
               FROM leases l
               LEFT JOIN instances i ON i.node_id = l.owner_node
              ORDER BY l.capability",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                LeaseHolder {
                    owner_node: row.get(1)?,
                    owner_name: row.get(2)?,
                    owner_host: row.get(3)?,
                    generation: row.get(4)?,
                    expires_at: row.get(5)?,
                    updated_at: row.get(6)?,
                },
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    // ---------------------------------------------------------------- mailbox

    pub fn send_envelope(&self, envelope: &Envelope) -> Result<bool> {
        // Sensitive payloads (message bodies, task texts) are encrypted at
        // rest; routing columns (to_node/kind/correlation) stay plaintext so
        // the relay can route without the key.
        let payload = crate::secret::encrypt(&serde_json::to_string(envelope)?);
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            r#"INSERT OR IGNORE INTO envelopes
                 (id, to_node, from_node, from_name, kind, payload, correlation, created_at, expires_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
            params![
                envelope.id,
                envelope.to_node,
                envelope.from_node,
                envelope.from_name,
                envelope.kind.as_str(),
                payload,
                envelope.correlation,
                envelope.created_at,
                envelope.expires_at,
            ],
        )?;
        Ok(changed == 1)
    }

    /// Deliveries for `node_id` that are still unacknowledged and unexpired.
    pub fn inbox(&self, node_id: &str, now: i64, limit: i64) -> Result<Vec<Envelope>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn.prepare(
            r#"SELECT payload FROM envelopes
               WHERE to_node = ?1 AND acked_at IS NULL AND expires_at > ?2
               ORDER BY created_at, id LIMIT ?3"#,
        )?;
        let rows = statement.query_map(params![node_id, now, limit.max(1)], |row| {
            row.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row?;
            // Encrypted at rest (see send_envelope); a wrong key or tampered
            // row decrypts to an error — skip it like a corrupt row.
            let Ok(decrypted) = crate::secret::decrypt(&payload) else {
                continue;
            };
            match serde_json::from_str::<Envelope>(&decrypted) {
                Ok(envelope) => out.push(envelope),
                // A corrupt row must not wedge the mailbox forever: skip it and
                // let `purge` reap it by TTL.
                Err(_) => continue,
            }
        }
        Ok(out)
    }

    pub fn ack_envelope(&self, id: &str, node_id: &str, now: i64) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "UPDATE envelopes SET acked_at = ?3 WHERE id = ?1 AND to_node = ?2 AND acked_at IS NULL",
            params![id, node_id, now],
        )?;
        Ok(changed == 1)
    }

    /// Remove expired and long-acked rows.
    pub fn purge_envelopes(&self, now: i64) -> Result<usize> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "DELETE FROM envelopes WHERE expires_at <= ?1 OR (acked_at IS NOT NULL AND acked_at < ?2)",
            params![now, now - 60 * 60 * 1000],
        )?;
        Ok(changed)
    }

    // ------------------------------------------------------------------ dedupe

    /// Record an inbound IM message id. Returns `true` the first time.
    pub fn mark_seen(&self, id: &str, now: i64) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "INSERT OR IGNORE INTO seen(id, ts) VALUES (?1, ?2)",
            params![id, now],
        )?;
        Ok(changed == 1)
    }

    pub fn prune_seen(&self, before: i64) -> Result<usize> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute("DELETE FROM seen WHERE ts < ?1", params![before])?;
        Ok(changed)
    }

    // ------------------------------------------------------------- housekeeping

    /// Drop dead presence rows, leases owned by gone instances, and stale mailbox
    /// rows. Called on the worker's slow tick.
    pub fn prune(&self, now: i64, alive_ms: i64) -> Result<PruneReport> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let instances = conn.execute(
            "DELETE FROM instances WHERE last_seen < ?1",
            params![now - alive_ms.max(0)],
        )?;
        let leases = conn.execute(
            "DELETE FROM leases WHERE owner_node NOT IN (SELECT node_id FROM instances) AND expires_at < ?1",
            params![now],
        )?;
        let envelopes = conn.execute(
            "DELETE FROM envelopes WHERE expires_at <= ?1 OR (acked_at IS NOT NULL AND acked_at < ?2)",
            params![now, now - 60 * 60 * 1000],
        )?;
        let seen = conn.execute(
            "DELETE FROM seen WHERE ts < ?1",
            params![now - 30 * 60 * 1000],
        )?;
        Ok(PruneReport {
            instances,
            leases,
            envelopes,
            seen,
        })
    }

    pub fn stats(&self, now: i64) -> Result<Stats> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let instances: i64 =
            conn.query_row("SELECT COUNT(*) FROM instances", [], |row| row.get(0))?;
        let envelopes_pending: i64 = conn.query_row(
            "SELECT COUNT(*) FROM envelopes WHERE acked_at IS NULL AND expires_at > ?1",
            params![now],
            |row| row.get(0),
        )?;
        let envelopes_total: i64 =
            conn.query_row("SELECT COUNT(*) FROM envelopes", [], |row| row.get(0))?;
        let leases: i64 = conn.query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))?;
        Ok(Stats {
            instances,
            envelopes_pending,
            envelopes_total,
            leases,
        })
    }

    pub fn text_limit(&self) -> usize {
        MAX_TEXT_BYTES
    }
}

/// Retry a write that lost a lock race. `busy_timeout` covers most cases, but
/// `BEGIN IMMEDIATE` can still return SQLITE_BUSY under a burst of writers
/// (eight instances starting at once is enough), so back off and retry.
fn retry_busy<T>(mut op: impl FnMut() -> Result<T>) -> Result<T> {
    let mut last: Option<MeshError> = None;
    for attempt in 0..20u64 {
        match op() {
            Ok(value) => return Ok(value),
            Err(error) if is_lock_contention(&error) => {
                last = Some(error);
                std::thread::sleep(Duration::from_millis(10 + 10 * attempt.min(10)));
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.unwrap_or_else(|| MeshError::invalid("database stayed locked")))
}

/// Best-effort hostname without an extra dependency. Also used by the
/// dead-pid reaper to scope `kill(pid, 0)` probes to this machine.
pub fn local_host_name() -> String {
    for key in ["RPI_MESH_HOST", "HOSTNAME", "HOST", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    if let Ok(value) = std::fs::read_to_string("/etc/hostname") {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    if let Ok(output) = std::process::Command::new("hostname").output() {
        if output.status.success() {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !value.is_empty() {
                return value;
            }
        }
    }
    "localhost".to_string()
}

/// `kill(pid, 0)`-style liveness probe: false when the pid demonstrably does
/// not exist. EPERM (exists, no permission to signal) counts as alive.
#[cfg(unix)]
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return true;
    }
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: u32) -> bool {
    true
}

fn is_lock_contention(error: &MeshError) -> bool {
    matches!(
        error,
        MeshError::Sqlite(rusqlite::Error::SqliteFailure(code, _))
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(all(test, unix))]
mod pid_tests {
    use super::*;

    #[test]
    fn pid_alive_tells_live_and_dead_pids_apart() {
        assert!(pid_alive(std::process::id()));
        // Pid 4M is practically certain to be free; on a busy shared machine
        // it could exist — skip rather than flake.
        if pid_alive(4_000_000) {
            return;
        }
        assert!(!pid_alive(4_000_000));
    }
}

#[cfg(test)]
mod tests;
