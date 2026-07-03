#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
app.py — the double-click desktop entry for siliqs-mesh-bridge.

This is what the packaged Windows/macOS/Linux binary runs. It:
  1. starts the local control panel (siliqs_mesh_bridge_web) on 127.0.0.1,
  2. opens your browser at it, and
  3. keeps running (the panel spawns/streams the actual bridge) until you quit.

Frozen-bundle detail: the control panel starts the bridge as a SUBPROCESS. In a
PyInstaller build there is no `python` and no `.py` on disk, so it re-execs THIS
binary with `--sq-run-bridge <bridge args…>` (see siliqs_mesh_bridge_web.Runner.start);
we catch that here and hand off to the bridge CLI. One binary, two roles.
"""
import os
import sys
import threading
import time
import webbrowser

HOST = os.environ.get("SQ_BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("SQ_BRIDGE_PORT", "8765"))


def _run_bridge():
    """Re-exec path: behave exactly like the `siliqs-mesh-bridge` CLI."""
    import siliqs_mesh_bridge
    sys.argv = ["siliqs-mesh-bridge", *sys.argv[2:]]   # drop the --sq-run-bridge flag
    siliqs_mesh_bridge.main()


def _open_browser_when_up():
    """Poll the panel port, then open the browser once it accepts connections."""
    import socket
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.5):
                break
        except OSError:
            time.sleep(0.3)
    try:
        webbrowser.open(f"http://{HOST}:{PORT}")
    except Exception:
        pass


def _run_panel():
    """Normal launch: serve the control panel + open the browser."""
    import siliqs_mesh_bridge_web as web
    threading.Thread(target=_open_browser_when_up, daemon=True).start()
    print(f"siliqs-mesh-bridge → opening http://{HOST}:{PORT}  (close this window to quit)")
    sys.argv = ["siliqs-mesh-bridge-web", "--host", HOST, "--port", str(PORT)]
    web.main()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--sq-run-bridge":
        _run_bridge()
    else:
        _run_panel()


if __name__ == "__main__":
    main()
