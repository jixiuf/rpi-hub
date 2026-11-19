#!/usr/bin/env bash
# Cross-machine smoke test WITHOUT ssh: relay serves TCP + token auth on the
# remote host; this machine's instance connects directly (office -> server is
# the direction that always works).
#
# No model call is made: both sides run `--mode rpc` with a dummy key and a dead
# base URL, which keeps the harness alive so extension commands can be driven
# over stdin.
#
# Usage:
#   scripts/smoke-ssh.sh [host]                 # default: apmprod
#   KEEP=1 scripts/smoke-ssh.sh apmprod         # keep the temp dir + tmux session
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${1:-${RELAY_HOST:-apmprod}}"
KEEP="${KEEP:-0}"
SESSION="${SESSION:-rpi-mesh-smoke}"
LOCAL_RPI="${LOCAL_RPI:-rpi}"
# Firewalls here only forward whitelisted ports. The relay can either own a
# dedicated forwarded port (RELAY_PORT, raw JSON-RPC or http:true) or share a
# firewall-forwarded HTTP port: set http:true and have IT/nginx route /hub-rpc
# to 127.0.0.1:<listen>. apmprod's 8088 turns out to be an sshd forward and
# 80/7777/4318/9090 are owned by other services, so the default is 19527 with
# http:true -- switch to raw (http:false) once a dedicated port is whitelisted.
RELAY_PORT="${RELAY_PORT:-19527}"
RELAY_NAME="relayhost-$$"
LOCAL_NAME="office-$$"

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "skip: sqlite3 not found locally" >&2
  exit 0
fi
if ! "${SSH[@]}" true 2>/dev/null; then
  echo "skip: cannot ssh to $HOST" >&2
  exit 0
fi

REMOTE_HOME="$("${SSH[@]}" 'printf %s "$HOME"')"
REMOTE_SRC="$REMOTE_HOME/rpi-mesh-src"
# A run-private hub dir: the default one accumulates rows from earlier runs.
REMOTE_HUB="/tmp/$SESSION-hub"
HUB_TOKEN="$REMOTE_HUB/relay-token"
REMOTE_EXT="$REMOTE_SRC/target/debug"
REMOTE_START="/tmp/$SESSION-start.sh"
# tmux panes do not inherit this shell's PATH, so bake the absolute rpi path
# into the remote start script (resolved over SSH before writing it).
REMOTE_RPI="$("${SSH[@]}" 'command -v rpi' 2>/dev/null || true)"
if [[ -z "$REMOTE_RPI" ]]; then
  echo "skip: rpi not found on $HOST" >&2
  exit 0
fi
REMOTE_OUT="/tmp/$SESSION.out"

# The remote IP the client dials. `ssh host -- hostname -I` needs no reverse dns.
REMOTE_IP="$("${SSH[@]}" 'hostname -I 2>/dev/null || ip -o addr show scope global | head -1' \
  | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+\./){print $i; exit}}')"
if [[ -z "$REMOTE_IP" ]]; then
  echo "FAIL: cannot determine a reachable IP on $HOST" >&2
  exit 1
fi

stop_remote() {
  "${SSH[@]}" "tmux kill-session -t $SESSION 2>/dev/null || true; \
    pkill -f 'session-dir /tmp/$SESSION' 2>/dev/null || true" >/dev/null 2>&1 || true
}

echo "==> syncing source to $HOST:$REMOTE_SRC"
rsync -az --exclude target --exclude .rpi --exclude .git "$HERE/" "$HOST:$REMOTE_SRC/"

echo "==> building rpi-hub on $HOST"
"${SSH[@]}" "cd $REMOTE_SRC && cargo build -p rpi-mesh >/dev/null"

WORK="$(mktemp -d)"
mkfifo "$WORK/in"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "${pid:-}" ]] && kill "$pid" 2>/dev/null || true
  done
  exec 3>&- 2>/dev/null || true
  stop_remote
  if [[ "$KEEP" != "1" ]]; then
    "${SSH[@]}" "rm -rf $REMOTE_HUB" >/dev/null 2>&1 || true
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

stop_remote

echo "==> starting the relay instance on $HOST:$RELAY_PORT (tmux: $SESSION)"
mkdir -p "$WORK"
cat >"$WORK/relay-config.json" <<EOF
{
  "version": 1,
  "instanceName": "$RELAY_NAME",
  "remote": {
    "mode": "relay",
    "relay": { "listen": "0.0.0.0:$RELAY_PORT", "tokenFile": "$HUB_TOKEN", "http": true }
  }
}
EOF
"${SSH[@]}" "rm -rf $REMOTE_HUB && mkdir -p $REMOTE_HUB && cat > $REMOTE_HUB/config.json" <"$WORK/relay-config.json"

cat >"$WORK/remote-start.sh" <<EOF
#!/bin/sh
# Hold stdin open so rpc mode keeps serving instead of exiting on EOF.
export RPI_HUB_DIR="$REMOTE_HUB"
sleep 600 | exec "$REMOTE_RPI" --local-only --extensions-dir "$REMOTE_EXT" \\
  --mode rpc --session-dir "/tmp/$SESSION" \\
  --api-key smoke --base-url http://127.0.0.1:1
EOF
scp -q -o BatchMode=yes "$WORK/remote-start.sh" "$HOST:$REMOTE_START"
# tmux does not forward inline env assignments, so the start script owns
# RPI_HUB_DIR (exported inside) and the absolute rpi path.
"${SSH[@]}" "chmod +x $REMOTE_START && tmux kill-session -t $SESSION 2>/dev/null; tmux new-session -d -s $SESSION 'sh $REMOTE_START > $REMOTE_OUT 2>&1'"

# Relay readiness = the TCP port answers.
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  if "${SSH[@]}" "test -f $HUB_TOKEN" 2>/dev/null; then
    exec 3>&- 2>/dev/null || true
    break
  fi
  sleep 0.5
done
if ! "${SSH[@]}" "test -f $HUB_TOKEN"; then
  echo "FAIL: relay token never appeared on $HOST" >&2
  "${SSH[@]}" "tail -5 $REMOTE_OUT; echo ---; ls -la $REMOTE_HUB; echo ---; pgrep -af rpi | head -3" >&2 || true
  exit 1
fi
relay_log="$("${SSH[@]}" "cat $REMOTE_HUB/logs/hub.log 2>/dev/null" || true)"
if ! grep -q "registered $RELAY_NAME" <<<"$relay_log"; then
  echo "FAIL: relay did not register as $RELAY_NAME" >&2
  echo "$relay_log" >&2
  exit 1
fi
echo "relay ready ($RELAY_NAME on $REMOTE_IP:$RELAY_PORT)"

echo "==> starting the client instance locally"
mkdir -p "$WORK/local"
# The relay generated the token on its side; copy it for the client.
"${SSH[@]}" "cat $HUB_TOKEN" >"$WORK/local/relay-token"
chmod 600 "$WORK/local/relay-token"
cat >"$WORK/local/config.json" <<EOF
{
  "version": 1,
  "instanceName": "$LOCAL_NAME",
  "remote": {
    "mode": "client",
    "relay": { "connect": "$REMOTE_IP:$RELAY_PORT", "tokenFile": "$WORK/local/relay-token", "http": true }
  }
}
EOF
RPI_HUB_DIR="$WORK/local" "$LOCAL_RPI" --local-only --extensions-dir "$HERE/target/debug" \
  --mode rpc --session-dir "$WORK/sessions" --api-key smoke --base-url http://127.0.0.1:1 \
  <"$WORK/in" >"$WORK/out" 2>&1 &
PIDS+=("$!")
exec 3>"$WORK/in"

wait_for() {
  local pattern="$1" timeout="${2:-20}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if grep -q "$pattern" "$WORK/out" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_for '"type":"ready"' 20 || {
  echo "FAIL: client never became ready" >&2
  tail -10 "$WORK/out" >&2
  exit 1
}

# 1. the client sees the remote instance through the relay
printf '{"id":1,"type":"execute_command","command":"/instances","args":""}\n' >&3
wait_for '"id":1' 15 || {
  echo "FAIL: /instances did not answer" >&2
  tail -5 "$WORK/out" >&2
  exit 1
}
if ! grep -q "$RELAY_NAME@" "$WORK/out"; then
  echo "FAIL: /instances does not list $RELAY_NAME" >&2
  grep -o '"id":1.*' "$WORK/out" >&2
  exit 1
fi
echo "cross-machine presence: ok"

# 2. /use moves the lease to the remote instance
remote_label="$(grep -o "$RELAY_NAME[@A-Za-z0-9._-]*" "$WORK/out" | head -1)"
printf '{"id":2,"type":"execute_command","command":"/use","args":"%s"}\n' "$remote_label" >&3
wait_for '"id":2' 15 || {
  echo "FAIL: /use did not answer" >&2
  exit 1
}
printf '{"id":3,"type":"execute_command","command":"/hub","args":"who"}\n' >&3
wait_for '"id":3' 15 || {
  echo "FAIL: /hub who did not answer" >&2
  exit 1
}
remote_bare="${remote_label%%@*}"
if ! grep -q "wechat: $remote_bare" "$WORK/out"; then
  echo "FAIL: lease did not move to $remote_bare" >&2
  grep -o '"id":3.*' "$WORK/out" >&2
  exit 1
fi
echo "lease handoff to $remote_bare: ok"

# 3. the relay's own database agrees
"${SSH[@]}" "python3 - <<'PY'
import sqlite3, sys
rows = sqlite3.connect('$REMOTE_HUB/hub.db').execute(
    'select owner_name from leases where capability=\"wechat\"').fetchall()
sys.exit(0 if rows and rows[0][0] == '$remote_bare' else 1)
PY" || {
  echo "FAIL: relay database does not show $remote_bare owning wechat" >&2
  exit 1
}
echo "relay database: ok"

echo "OK: cross-machine discovery and lease handoff verified ($REMOTE_IP:$RELAY_PORT <-> $(hostname -s))"
