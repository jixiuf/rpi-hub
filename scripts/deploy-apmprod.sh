#!/usr/bin/env bash
# Deploy rpi-mesh to apmprod: sync source, build release, install
# the dylibs into ~/.rpi/agent/extensions, and restart the relay session.
#
# The workspace pins rpi-plugin-sdk to a local path (the argument-suggester
# PR, pi-rust#338); this script rewrites that path for the remote filesystem
# after rsync, because rsync carries the local absolute path over.
set -euo pipefail

HOST="${1:-apmprod}"
KEEP="${KEEP:-0}"
SESSION="${SESSION:-hub-relay}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")

if ! "${SSH[@]}" true 2>/dev/null; then
  echo "skip: cannot ssh to $HOST" >&2
  exit 0
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE_HOME="$("${SSH[@]}" 'printf %s "$HOME"')"
REMOTE_SRC="$REMOTE_HOME/repos/rpi-hub"
REMOTE_PI_RUST="$REMOTE_HOME/repos/pi-rust"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> syncing rpi-mesh source"
rsync -az --exclude target --exclude .git --exclude .rpi \
  --exclude '.hub-pr' \
  "$HERE/" "$HOST:$REMOTE_SRC/"

echo "==> syncing patched rpi-plugin-sdk (PR #338)"
rsync -az --exclude target \
  ~/repos/pi-rust/crates/rpi-plugin-sdk "$HOST:$REMOTE_PI_RUST/crates/"

echo "==> rewriting the SDK path for the remote filesystem"
"${SSH[@]}" "sed -i 's|path = \"/Users/jixiuf/repos/pi-rust/crates/rpi-plugin-sdk\"|path = \"$REMOTE_PI_RUST/crates/rpi-plugin-sdk\"|' $REMOTE_SRC/Cargo.toml"

echo "==> building release"
"${SSH[@]}" "cd $REMOTE_SRC && cargo build --release -p rpi-mesh"

echo "==> installing into ~/.rpi/agent/extensions"
"${SSH[@]}" "rm -f ~/.rpi/agent/extensions/librpi_hub.so ~/.rpi/agent/extensions/librpi_wechat.so && cp $REMOTE_SRC/target/release/librpi_mesh.so ~/.rpi/agent/extensions/"

echo "==> restarting the relay session ($SESSION)"
# The remote command goes through a script file (scp + sh) — inline quoting
# through two shell layers silently dropped the tmux new-session call.
cat >"$WORK/restart.sh" <<REMOTE
#!/bin/sh
tmux kill-session -t "$SESSION" 2>/dev/null || true
pkill -f "session-dir /tmp/$SESSION" 2>/dev/null || true
sleep 1
tmux new-session -d -s "$SESSION" "/home/admin/.cargo/bin/rpi --session-dir /tmp/$SESSION"
REMOTE
scp -q -o BatchMode=yes "$WORK/restart.sh" "$HOST:/tmp/$SESSION-restart.sh"
"${SSH[@]}" "sh /tmp/$SESSION-restart.sh"
sleep 8
if "${SSH[@]}" "ss -tln | grep -q :8085"; then
  echo "OK: relay listening on :8085"
else
  echo "WARN: relay not listening yet — check tmux session $SESSION" >&2
fi
