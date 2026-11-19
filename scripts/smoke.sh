#!/usr/bin/env bash
# End-to-end smoke test for the rpi-hub plugin against a real `rpi` binary.
#
# No model is called and no network is used: each instance runs in `--mode rpc`
# with a dummy key and a dead base URL, which keeps the harness alive and lets
# us drive extension slash commands over stdin.
#
# Asserted:
#   1. two live instances register, with distinct names and a collision log line,
#   2. exactly one owns the wechat lease,
#   3. `/instances` on A lists both instances,
#   4. `/use B` on A hands the lease to B (visible in the store),
#   5. `/hub status` on B reports B's own identity.
#
# Usage: scripts/smoke.sh [path-to-rpi]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RPI_BIN="${1:-rpi}"
KEEP="${KEEP:-0}"

if ! command -v "$RPI_BIN" >/dev/null 2>&1; then
  echo "skip: $RPI_BIN not found" >&2
  exit 0
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "skip: sqlite3 not found" >&2
  exit 0
fi

echo "==> building rpi-mesh"
cargo build -p rpi-mesh --manifest-path "$HERE/Cargo.toml" >/dev/null

WORK="$(mktemp -d)"
HUB_DIR="$WORK/hub"
DB="$HUB_DIR/mesh.db"
LOG="$HUB_DIR/logs/mesh.log"
PIDS=()

# The wechat lease is claimed by the mesh plugin's watcher. It claims only
# when credentials exist and the bridge is not disabled: seed a dummy
# credential file. No network is touched — the claim path never calls iLink.
mkdir -p "$HUB_DIR/wechat"
cat > "$HUB_DIR/wechat/credentials.json" <<'JSON'
{"token": "smoke-dummy", "baseUrl": "http://127.0.0.1:1", "userId": "smoke-user", "savedAt": 0}
JSON
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "${pid:-}" ]] && kill "$pid" 2>/dev/null || true
  done
  exec 3>&- 2>/dev/null || true
  exec 4>&- 2>/dev/null || true
  if [[ "$KEEP" == "1" ]]; then
    echo "kept: $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

export RPI_MESH_DIR="$HUB_DIR"
EXT_DIR="$HERE/target/debug"

start_instance() {
  local tag="$1"
  mkfifo "$WORK/$tag.in"
  "$RPI_BIN" --local-only --extensions-dir "$EXT_DIR" --mode rpc \
    --session-dir "$WORK/sessions-$tag" \
    --api-key smoke --base-url http://127.0.0.1:1 \
    <"$WORK/$tag.in" >"$WORK/$tag.out" 2>&1 &
  PIDS+=("$!")
  # Hold the fifo open so writes do not hit EOF.
  eval "exec $2>\"\$WORK/$tag.in\""
}

wait_for_file() {
  local file="$1" pattern="$2" timeout="${3:-15}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if grep -q "$pattern" "$file" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

send_rpc() {
  local fd="$1" id="$2" command="$3" args="${4:-}"
  local payload
  payload=$(printf '{"id":%s,"type":"execute_command","command":"%s","args":"%s"}' \
    "$id" "$command" "$args")
  printf '%s\n' "$payload" >&"$fd"
}

echo "==> starting A and B"
start_instance a 3
wait_for_file "$WORK/a.out" '"type":"ready"' 15 || {
  echo "FAIL: A never became ready" >&2
  tail -5 "$WORK/a.out" >&2
  exit 1
}
start_instance b 4
wait_for_file "$WORK/b.out" '"type":"ready"' 15 || {
  echo "FAIL: B never became ready" >&2
  tail -5 "$WORK/b.out" >&2
  exit 1
}

# 1. presence + unique names
deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  count="$(sqlite3 "$DB" "SELECT COUNT(*) FROM instances;" 2>/dev/null || echo 0)"
  [[ "$count" == "2" ]] && break
  sleep 0.2
done
count="$(sqlite3 "$DB" "SELECT COUNT(*) FROM instances;")"
names="$(sqlite3 "$DB" "SELECT COUNT(DISTINCT name) FROM instances;")"
echo "instances=$count distinct_names=$names"
if [[ "$count" != "2" || "$names" != "2" ]]; then
  echo "FAIL: expected two live instances with distinct names" >&2
  sqlite3 "$DB" "SELECT name,host,pid FROM instances;" >&2
  exit 1
fi
if ! grep -q "is taken on" "$LOG"; then
  echo "FAIL: expected a name-collision log line" >&2
  cat "$LOG" >&2
  exit 1
fi

# 2. exactly one lease
deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  leases="$(sqlite3 "$DB" "SELECT COUNT(*) FROM leases;" 2>/dev/null || echo 0)"
  [[ "$leases" == "1" ]] && break
  sleep 0.2
done
leases="$(sqlite3 "$DB" "SELECT COUNT(*) FROM leases;")"
if [[ "$leases" != "1" ]]; then
  echo "FAIL: expected exactly one wechat lease, got $leases" >&2
  sqlite3 "$DB" "SELECT * FROM leases;" >&2
  exit 1
fi
name_a="$(sqlite3 "$DB" "SELECT name FROM instances ORDER BY name LIMIT 1;")"
name_b="$(sqlite3 "$DB" "SELECT name FROM instances ORDER BY name DESC LIMIT 1;")"
echo "lease=$(sqlite3 "$DB" "SELECT owner_name FROM leases;") a=$name_a b=$name_b"

# 3. /instances on A sees both
send_rpc 3 1 "/instances"
wait_for_file "$WORK/a.out" '"id":1' 10 || {
  echo "FAIL: /instances did not answer" >&2
  tail -5 "$WORK/a.out" >&2
  exit 1
}
if ! grep -q "$name_b" "$WORK/a.out"; then
  echo "FAIL: /instances on A does not list B" >&2
  grep '"id":1' "$WORK/a.out" >&2
  exit 1
fi

# 4. /use hands the lease to B
send_rpc 3 2 "/use" "$name_b"
wait_for_file "$WORK/a.out" '"id":2' 10 || {
  echo "FAIL: /use did not answer" >&2
  tail -5 "$WORK/a.out" >&2
  exit 1
}
deadline=$((SECONDS + 10))
while (( SECONDS < deadline )); do
  owner="$(sqlite3 "$DB" "SELECT owner_name FROM leases;" 2>/dev/null || echo '')"
  [[ "$owner" == "$name_b" ]] && break
  sleep 0.2
done
owner="$(sqlite3 "$DB" "SELECT owner_name FROM leases;")"
echo "lease_after_use=$owner"
if [[ "$owner" != "$name_b" ]]; then
  echo "FAIL: /use did not move the lease to $name_b" >&2
  sqlite3 "$DB" "SELECT * FROM leases;" >&2
  exit 1
fi

# 5. /hub status on B reports B
send_rpc 4 3 "/mesh" "status"
wait_for_file "$WORK/b.out" '"id":3' 10 || {
  echo "FAIL: /hub status did not answer" >&2
  tail -5 "$WORK/b.out" >&2
  exit 1
}
if ! grep -q "$name_b" "$WORK/b.out"; then
  echo "FAIL: /hub status on B does not mention $name_b" >&2
  grep '"id":3' "$WORK/b.out" >&2
  exit 1
fi

if grep -qiE "panic|rpi-hub disabled" "$WORK"/a.out "$WORK"/b.out; then
  echo "FAIL: plugin reported a fatal problem" >&2
  grep -iE "panic|disabled" "$WORK"/a.out "$WORK"/b.out >&2
  exit 1
fi

# 6. Bidirectional mailbox: B → A. A server-side instance addresses A the
# same way A addresses it (shared mesh.db / relay): inject a text envelope
# from B into A's mailbox and wait for A's worker to drain + ack it.
marker="bidir-$$"
node_a="$(sqlite3 "$DB" "SELECT node_id FROM instances WHERE name='$name_a';")"
node_b="$(sqlite3 "$DB" "SELECT node_id FROM instances WHERE name='$name_b';")"
now_ms="$(($(date +%s) * 1000))"
payload="$(printf '{"id":"bidir-%s","kind":"text","fromNode":"%s","fromName":"%s","toNode":"%s","text":"bidirectional-marker","createdAt":%s,"expiresAt":%s}' "$$" "$node_b" "$name_b" "$node_a" "$now_ms" "$((now_ms + 120000))")"
sqlite3 "$DB" "INSERT INTO envelopes (id, to_node, from_node, from_name, kind, payload, created_at, expires_at)
  VALUES ('bidir-$$', '$node_a', '$node_b', '$name_b', 'text', '$payload', $now_ms, $((now_ms + 120000)));"
deadline=$((SECONDS + 10))
while (( SECONDS < deadline )); do
  acked="$(sqlite3 "$DB" "SELECT acked_at FROM envelopes WHERE id='bidir-$$';" 2>/dev/null)"
  [[ -n "$acked" ]] && break
  sleep 0.3
done
acked="$(sqlite3 "$DB" "SELECT acked_at FROM envelopes WHERE id='bidir-$$';" 2>/dev/null)"
echo "bidir_acked=$([ -n "$acked" ] && echo yes || echo no)"
if [[ -z "$acked" ]]; then
  echo "FAIL: B → A mailbox message was not drained+acked by A" >&2
  sqlite3 "$DB" "SELECT id, to_node, acked_at FROM envelopes;" >&2
  exit 1
fi

echo "OK: presence, unique names, lease ownership, /instances, /use, and bidirectional mailbox verified"
