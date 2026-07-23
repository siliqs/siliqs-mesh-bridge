#!/usr/bin/env bash
# Regression test — a gateway that dies must say so on stdout.
#
# Headless deployments (docker logs, journalctl) can read nothing but this
# process's stdout. Before this test existed, the control panel printed
# "auto-start: started" and then stayed silent forever while the gateway
# failed to open its serial port and exited — three weeks of "running" on a
# box that was bridging nothing.
#
# Scenario: auto-start config pointing at a serial port that does not exist.
# PASS = stdout carries the connection failure AND the process-exited line.
#
#   bash tests/test_stdout_surfaces_failure.sh

set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PY="$REPO/.buildvenv/bin/python"
PORT=8802
GHOST_DEV=/dev/cu.no-such-node-9f3a       # must not exist
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

[ -x "$PY" ] || { echo "SKIP: no $PY (run the build venv setup first)"; exit 0; }
[ -e "$GHOST_DEV" ] && { echo "SKIP: $GHOST_DEV unexpectedly exists"; exit 0; }

cat > "$WORK/config.json" <<EOF
{"iface": "usb", "handler": "serial", "port": "$GHOST_DEV", "peer": "!7d51bdb0", "link": "$WORK/meshtty", "mode": "line", "mtu": 50}
EOF

cd "$REPO"
PYTHONUNBUFFERED=1 SMB_CONFIG_FILE="$WORK/config.json" \
  "$PY" -u siliqs_mesh_bridge_web.py --port $PORT > "$WORK/stdout.log" 2>&1 &
SRV=$!

# The gateway retries 5x before giving up; poll until it exits rather than
# sleeping a fixed 25s, so the test stays as fast as the app allows.
for _ in $(seq 1 60); do
  grep -q "exited" "$WORK/stdout.log" && break
  sleep 0.5
done
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

echo "--- stdout ---"
cat "$WORK/stdout.log"
echo "--------------"

fail=0
grep -q "could not open USB" "$WORK/stdout.log" || { echo "FAIL: 連線失敗的原因沒有出現在 stdout"; fail=1; }
grep -q "exited"             "$WORK/stdout.log" || { echo "FAIL: gateway 結束了,stdout 卻沒說"; fail=1; }

if [ $fail -eq 0 ]; then echo "PASS"; else echo "RED — headless 部署看不到 gateway 已死"; fi
exit $fail
