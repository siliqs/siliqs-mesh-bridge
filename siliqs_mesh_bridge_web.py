#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_mesh_bridge_web.py — a localhost control panel for the Siliqs Gateway.

Start/stop the Gateway (a bidirectional mesh ⇄ MQTT bridge) and watch it, from a
browser, **no command line**. It needs host OS access (serial ports / BLE / MQTT), so
it is a tiny local HTTP server (stdlib only) that spawns the verified
`siliqs_mesh_bridge` CLI and streams its output. Binds 127.0.0.1 by default.

  siliqs-mesh-bridge-web            # then open http://127.0.0.1:8765

The Gateway does ONE job: carry packets between your node's mesh and an MQTT broker.
Everything downstream (dashboards, decoders, the virtual serial cable) is a separate
MQTT client — the serial cable is now `siliqs-serial-mqtt`, not part of this panel.
"""
import argparse
import base64
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import siliqs_mesh_bridge   # reuse the verified CLI (we spawn this file)
import siliqs_mqtt_broker   # the tiny built-in broker (no external broker needed)

BRIDGE = siliqs_mesh_bridge.__file__


# ── built-in MQTT broker (so a customer with no broker can still finish the job) ──
_builtin = {"srv": None, "port": None}
_builtin_lock = threading.Lock()


def ensure_builtin_broker(port, log=None):
    """Start (or re-point) the embedded broker on 0.0.0.0:<port>. Idempotent: a repeat
    call on the same port is a no-op; a different port restarts it. Binds 0.0.0.0 so the
    customer's dashboards / Node-RED / Grafana on the LAN can reach it too."""
    with _builtin_lock:
        if _builtin["srv"] and _builtin["port"] == port:
            return
        if _builtin["srv"]:
            try:
                _builtin["srv"].stop()
            except Exception:
                pass
            _builtin["srv"] = None
        srv = siliqs_mqtt_broker.MqttBroker("0.0.0.0", port, on_log=log or (lambda m: None))
        srv.start()
        _builtin["srv"] = srv
        _builtin["port"] = port


def lan_ip():
    """Best-effort LAN IP to show the user where to point their dashboards."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
# If set, the last applied config is saved here and auto-started on launch — so a
# deployed appliance survives a reboot without anyone clicking Start.
CONFIG_FILE = os.environ.get("SMB_CONFIG_FILE")


class Telemetry:
    """Subscribes to the MQTT broker (msh/2/json/#) and keeps a live view: latest
    frame per node + a rolling event stream, so the same panel shows data, not just
    controls. paho-mqtt is imported lazily."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latest = {}
        self.events = deque(maxlen=200)
        self.cli = None
        self.broker = None
        # mesh / RF insight, fed from the bridge over siliqs/mesh/#
        self.mesh_nodes = {}      # {"gw","t","nodes":[...]}
        self.mesh_links = {}      # {"t","links":[...]}
        self.traces = {}          # key (dest id / cmd) -> last result

    def watch(self, broker, port, username=None, password=None, tls=False,
              ca_cert=None, client_cert=None, client_key=None):
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            return
        with self.lock:
            if self.cli:
                try:
                    self.cli.loop_stop(); self.cli.disconnect()
                except Exception:
                    pass
                self.cli = None
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            c.on_message = self._on_msg
            try:
                if username:
                    c.username_pw_set(username, password or None)
                if tls or ca_cert or client_cert or client_key:
                    kw = {}
                    if ca_cert:
                        kw["ca_certs"] = ca_cert
                    if client_cert:
                        kw["certfile"] = client_cert
                    if client_key:
                        kw["keyfile"] = client_key
                    c.tls_set(**kw)
                c.connect(broker, int(port), 60)
                c.subscribe("msh/2/json/#")
                c.subscribe("siliqs/mesh/#")     # NodeDB + neighbour graph + traceroute from the bridge
                c.loop_start()
                self.cli = c
                self.broker = f"{broker}:{port}"
            except Exception:
                self.cli = None

    def _on_msg(self, client, userdata, msg):  # noqa: ARG002
        if msg.topic.startswith("siliqs/mesh/"):
            self._on_mesh_msg(msg.topic, msg.payload)
            return
        try:
            d = json.loads(msg.payload)
            sender = d.get("sender") or msg.topic.split("/")[-1]
            raw = base64.b64decode((d.get("payload") or {}).get("raw", ""))
            ev = {"t": time.time(), "node": sender, "len": len(raw), "hex": raw.hex()}
            with self.lock:
                self.latest[sender] = ev
                self.events.append(ev)
        except Exception:
            pass

    def _on_mesh_msg(self, topic, payload):
        try:
            d = json.loads(payload)
        except Exception:
            return
        with self.lock:
            if topic == "siliqs/mesh/nodes":
                self.mesh_nodes = d
            elif topic == "siliqs/mesh/neighbors":
                self.mesh_links = d
            elif topic == "siliqs/mesh/trace":
                self.traces[d.get("to") or d.get("cmd") or "?"] = d

    def publish_cmd(self, obj):
        with self.lock:
            c = self.cli
        if not c:
            return False
        try:
            c.publish("siliqs/mesh/cmd", json.dumps(obj))
            return True
        except Exception:
            return False

    def snapshot(self):
        with self.lock:
            return {"broker": self.broker, "latest": list(self.latest.values()),
                    "events": list(self.events)[-120:]}

    def mesh_snapshot(self):
        with self.lock:
            return {"gw": self.mesh_nodes.get("gw"), "t": self.mesh_nodes.get("t"),
                    "nodes": self.mesh_nodes.get("nodes", []),
                    "links": self.mesh_links.get("links", []),
                    "traces": list(self.traces.values())}


telemetry = Telemetry()


def _save_cfg(cfg):
    if CONFIG_FILE:
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(cfg, f)
        except OSError:
            pass


def _load_cfg():
    if CONFIG_FILE:
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except (OSError, ValueError):
            return None
    return None


class Runner:
    """Owns the single Gateway subprocess + a rolling log."""

    def __init__(self):
        self.proc = None
        self.argv = None
        self.cfg = None
        self.log = deque(maxlen=400)
        self.lock = threading.Lock()

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, cfg):
        if self.running():
            self.stop()
        # Resolve the broker: built-in (we run one) or external (the customer's).
        external = cfg.get("broker_mode", "builtin") != "builtin"
        username = password = None
        tls = False
        ca_cert = client_cert = client_key = None
        if not external:
            bport = int(cfg.get("builtin_port", 1883) or 1883)
            ensure_builtin_broker(bport, log=lambda m: self.log.append("[broker] " + m))
            bhost = "127.0.0.1"
        else:
            bhost = cfg.get("broker")
            bport = int(cfg.get("broker_port", 1883) or 1883)
            if not bhost:
                raise ValueError("MQTT broker host required (or switch to the built-in broker)")
            username = cfg.get("username") or None
            password = cfg.get("password") or None            # kept out of argv/logs (env only)
            tls = bool(cfg.get("tls"))
            ca_cert = cfg.get("ca_cert") or None
            client_cert = cfg.get("client_cert") or None
            client_key = cfg.get("client_key") or None
        # resolved host/port + auth flags for the bridge (password goes via env, not argv)
        rcfg = dict(cfg, broker=bhost, broker_port=bport, username=username, tls=tls,
                    ca_cert=ca_cert, client_cert=client_cert, client_key=client_key)
        with self.lock:
            argv = self._build_argv(rcfg)         # raises ValueError on bad config
            self.argv = argv
            self.cfg = cfg
            self.log.clear()
            if not external:
                self.log.append(f"[broker] built-in MQTT broker on 0.0.0.0:{bport} "
                                f"(dashboards → {lan_ip()}:{bport})")
            self.log.append("$ siliqs-mesh-bridge " + " ".join(argv))
            # Spawn the Gateway CLI. In a normal install we run the .py with the
            # interpreter; inside a PyInstaller bundle there is no python/.py on disk, so
            # sys.executable is the frozen app itself — re-exec it with --sq-run-bridge
            # (handled in app.py) so it runs the bridge instead of the panel.
            cmd = ([sys.executable, "--sq-run-bridge", *argv]
                   if getattr(sys, "frozen", False)
                   else [sys.executable, "-u", BRIDGE, *argv])
            env = dict(os.environ)
            if password:
                env["SMB_MQTT_PASSWORD"] = password           # never on the command line
            self.proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
            threading.Thread(target=self._pump, daemon=True).start()
        _save_cfg(cfg)                              # persist for reboot auto-start
        telemetry.watch(bhost, bport, username, password, tls,
                        ca_cert, client_cert, client_key)        # live view (resolved broker)
        return True, "started"

    def _pump(self):
        try:
            for line in self.proc.stdout:
                self.log.append(line.rstrip("\n"))
        except Exception:
            pass
        self.log.append("— gateway process exited —")

    def stop(self):
        with self.lock:
            if not self.running():
                return False, "not running"
            self.proc.terminate()
            try:
                self.proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            return True, "stopped"

    @staticmethod
    def _build_argv(cfg):
        a = ["--verbose"]
        iface = cfg.get("iface", "usb")
        a += ["--iface", iface]
        if iface == "ble":
            if not cfg.get("ble"):
                raise ValueError("BLE device name/address required")
            a += ["--ble", cfg["ble"]]
        else:
            if not cfg.get("port"):
                raise ValueError("serial port required — pick your node")
            a += ["--port", cfg["port"]]
        if not cfg.get("broker"):
            raise ValueError("MQTT broker host required")
        a += ["--broker", cfg["broker"],
              "--broker-port", str(int(cfg.get("broker_port", 1883)))]
        if cfg.get("channel"):                 # blank → gateway auto-reads it from the node
            a += ["--channel", cfg["channel"]]
        if cfg.get("username"):                # external broker auth (password goes via env)
            a += ["--username", cfg["username"]]
        if cfg.get("tls"):
            a += ["--tls"]
        if cfg.get("ca_cert"):                 # mTLS cert file paths (not secret)
            a += ["--ca-cert", cfg["ca_cert"]]
        if cfg.get("client_cert"):
            a += ["--client-cert", cfg["client_cert"]]
        if cfg.get("client_key"):
            a += ["--client-key", cfg["client_key"]]
        return a


runner = Runner()


def list_ports():
    """Best-effort serial port list, flagging which look like a Meshtastic node.

    pyserial's comports() misses Docker `--device` maps and udev symlinks, so we also
    glob /dev and fold in whatever port is configured/running. Nodes (Espressif native
    USB / CH340 / CP210x) are marked and sorted first; host pseudo-ports last."""
    seen, out = set(), []

    def classify(dev, desc, vid=None):
        d, s = dev.lower(), (desc or "").lower()
        if any(j in d for j in ("debug-console", "bluetooth", "wlan-debug")):
            return False, 9
        node = (vid == 0x303A                                   # Espressif native USB (JTAG/serial)
                or any(k in s for k in ("jtag", "ch340", "cp210", "ch910", "usb serial", "usb single serial"))
                or any(k in d for k in ("usbmodem", "usbserial", "ttyacm", "ttyusb",
                                        "sqc485i", "-meshtastic")))
        return node, (0 if node else 5)

    def add(dev, desc="", vid=None):
        if dev and dev not in seen:
            seen.add(dev)
            node, rank = classify(dev, desc, vid)
            out.append({"device": dev, "desc": desc, "node": node, "rank": rank})

    try:
        from serial.tools import list_ports as lp
        for p in lp.comports():
            add(p.device, p.description or "", getattr(p, "vid", None))
    except Exception:
        pass
    import glob
    for pat in ("/dev/ttyACM*", "/dev/ttyUSB*", "/dev/cu.*", "/dev/serial/by-id/*",
                "/dev/sqc485i-*", "/dev/*-meshtastic"):
        for dev in sorted(glob.glob(pat)):
            add(dev, "device node")
    try:
        cfg = _load_cfg()
        if cfg.get("port"):
            add(cfg["port"], "configured")
    except Exception:
        pass
    out.sort(key=lambda p: (p["rank"], p["device"]))
    return out


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        b = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif self.path == "/api/ports":
            self._send(200, {"ports": list_ports()})
        elif self.path == "/api/state":
            safe_cfg = runner.cfg
            if isinstance(safe_cfg, dict) and safe_cfg.get("password"):
                safe_cfg = dict(safe_cfg, password="")   # never echo the password to the browser
            self._send(200, {"running": runner.running(), "argv": runner.argv,
                             "cfg": safe_cfg, "log": list(runner.log),
                             "lanip": lan_ip(), "builtin_port": _builtin["port"]})
        elif self.path == "/api/telemetry":
            self._send(200, telemetry.snapshot())
        elif self.path == "/api/mesh":
            self._send(200, telemetry.mesh_snapshot())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            cfg = json.loads(raw or b"{}")
        except Exception:
            cfg = {}
        if self.path == "/api/start":
            try:
                ok, msg = runner.start(cfg)
                self._send(200, {"ok": ok, "msg": msg})
            except ValueError as e:
                self._send(400, {"ok": False, "msg": str(e)})
        elif self.path == "/api/stop":
            ok, msg = runner.stop()
            self._send(200, {"ok": ok, "msg": msg})
        elif self.path == "/api/mesh-cmd":
            ok = telemetry.publish_cmd(cfg)
            self._send(200, {"ok": ok})
        elif self.path == "/api/quit":
            # Quit the whole app (needed for the windowless macOS .app / packaged builds,
            # which have no console to Ctrl-C). Reply first, then stop + exit shortly after.
            self._send(200, {"ok": True, "msg": "quitting"})
            def _bye():
                try: runner.stop()
                finally: os._exit(0)
            threading.Timer(0.4, _bye).start()
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *a):   # keep the console quiet
        pass


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Siliqs Gateway · mesh ⇄ MQTT</title><style>
 :root{--bg:#14151a;--panel:#1d1f27;--p2:#23262f;--bd:#2e3140;--tx:#e7e9ee;--mut:#9aa0ad;--ac:#3fa7d6;--ac2:#67ea94;--dn:#ef6d6d}
 *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.55 system-ui,-apple-system,"PingFang TC","Microsoft JhengHei",sans-serif}
 a{color:var(--ac)}
 header{position:sticky;top:0;z-index:6;display:flex;align-items:center;gap:12px;padding:13px 20px;background:var(--panel);border-bottom:1px solid var(--bd)}
 header b{font-size:16px}header .tag{font-size:12px;color:var(--mut)}
 .dot{width:10px;height:10px;border-radius:50%;background:#555;transition:background .2s}
 .dot.on{background:var(--ac2);box-shadow:0 0 8px var(--ac2)}
 .layout{display:grid;grid-template-columns:200px minmax(0,1fr) var(--telw,340px);align-items:start}
 nav{position:sticky;top:52px;align-self:start;padding:22px 16px;border-right:1px solid var(--bd)}
 aside.tel{position:sticky;top:52px;align-self:start;height:calc(100vh - 52px);border-left:1px solid var(--bd);background:var(--panel);display:flex;flex-direction:column;min-width:0}
 .telgrip{position:absolute;left:-6px;top:0;width:12px;height:100%;cursor:col-resize;z-index:6;display:flex;align-items:center;justify-content:center}
 .telgrip::before{content:"";position:absolute;left:5px;top:0;width:2px;height:100%;background:transparent;transition:background .15s}
 .telgrip:hover::before,.telgrip.drag::before{background:var(--ac)}
 .telgrip::after{content:"⋮⋮";writing-mode:vertical-lr;letter-spacing:-3px;font-size:12px;color:var(--mut);background:var(--panel);border:1px solid var(--bd);border-radius:6px;padding:8px 2px;transition:color .15s,border-color .15s}
 .telgrip:hover::after,.telgrip.drag::after{color:var(--ac);border-color:var(--ac)}
 aside.tel .telhead{display:flex;align-items:center;gap:8px;padding:14px 16px;border-bottom:1px solid var(--bd)}
 aside.tel .telhead b{font-size:14px}aside.tel .telhead .note{margin-left:auto}
 aside.tel .telbody{flex:1;overflow:auto;padding:14px 16px}
 aside.tel .telbody h2{font-size:13px;margin:14px 0 6px}aside.tel .telbody .sub{margin:0 0 10px}
 nav .step{display:flex;gap:11px;padding:10px 10px;border-radius:9px;color:var(--mut);text-decoration:none;cursor:pointer}
 nav .step:hover{background:var(--p2)}
 nav .step .n{width:22px;height:22px;border-radius:50%;background:var(--p2);color:var(--tx);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex:0 0 22px}
 nav .step b{color:var(--tx);font-size:14px;display:block}nav .step span{font-size:11.5px}
 nav hr{border:0;border-top:1px solid var(--bd);margin:16px 4px}
 main{min-width:0;padding:22px 24px}
 .card{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:16px 18px;margin-bottom:18px}
 h2{margin:0 0 4px;font-size:15px}.sub{font-size:12.5px;color:var(--mut);margin:0 0 14px}
 label{display:block;font-size:12px;color:var(--mut);margin:0 0 5px}
 input,select{width:100%;background:var(--p2);border:1px solid var(--bd);color:var(--tx);border-radius:8px;padding:9px 11px;font:inherit;font-size:14px}
 .row{display:flex;gap:12px;flex-wrap:wrap}.row>.f{flex:1;min-width:120px}
 .seg{display:inline-flex;background:var(--p2);border:1px solid var(--bd);border-radius:8px;overflow:hidden}
 .seg label{margin:0;padding:8px 16px;color:var(--mut);cursor:pointer;font-size:13px}
 .seg input{display:none}.seg label:has(input:checked){background:var(--ac);color:#fff}
 .btn{border:0;border-radius:999px;padding:10px 22px;font:inherit;font-weight:700;cursor:pointer}
 .btn.go{background:var(--ac);color:#fff}.btn.stop{background:var(--p2);color:var(--tx);border:1px solid var(--bd)}
 .btn:disabled{opacity:.5;cursor:default}
 .hide{display:none}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
 /* flow diagram */
 .flow{display:flex;align-items:stretch;gap:0;background:var(--panel);border:1px solid var(--bd);border-radius:12px;overflow:hidden;margin-bottom:18px}
 .flow .st{flex:1;padding:14px 12px;text-align:center;border-right:1px solid var(--bd);position:relative}
 .flow .st:last-child{border-right:0}
 .flow .st .ic{height:26px;display:flex;align-items:center;justify-content:center}
 .flow .st .ic svg{width:22px;height:22px;color:var(--mut)}
 .flow .st .t{font-size:12.5px;font-weight:700;margin-top:4px}
 .flow .st .d{font-size:11px;color:var(--mut)}
 .flow .st.here{background:linear-gradient(180deg,rgba(63,167,214,.16),transparent)}
 .flow .st.here .t{color:var(--ac2)}.flow .st.here .ic svg{color:var(--ac2)}
 .lang{background:var(--p2);border:1px solid var(--bd);color:var(--tx);border-radius:7px;padding:5px 12px;font:inherit;font-size:12px;cursor:pointer}
 .arrow{color:var(--mut);align-self:center;padding:0 2px;font-size:13px}
 .flow .divide{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;padding:0 8px;align-self:stretch;border-left:1px dashed #3a4152;border-right:1px dashed #3a4152}
 .flow .divide small{font-size:9px;line-height:1.15;color:var(--mut);text-align:center;max-width:52px}
 .msg{font-size:13px;margin-left:12px}
 .gwdgram{margin:0 0 18px;border:1px solid var(--bd);border-radius:12px;overflow:hidden;background:#161a21}
 .gwdgram figcaption{padding:10px 14px;font-size:12px;color:var(--mut);border-bottom:1px solid var(--bd)}
 .gwdgram svg{display:block;width:100%;height:auto}
 table{width:100%;border-collapse:collapse;font-size:13px}
 th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--bd);vertical-align:top}
 th{color:var(--mut);font-weight:500;font-size:11px}.node{color:var(--ac2)}
 pre.log{background:#0e0f13;border:1px solid var(--bd);border-radius:8px;padding:12px;max-height:200px;overflow:auto;font-size:12px;margin:0;white-space:pre-wrap;word-break:break-all}
 .note{font-size:12px;color:var(--mut)}.evs{max-height:280px;overflow:auto}
 /* mesh / RF */
 .meshbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 12px}
 .meshbar select{width:auto;min-width:150px}.meshbar .btn{padding:8px 16px;font-size:13px}
 .btn.mini{padding:6px 13px;font-size:12px;font-weight:600}
 .chip{display:inline-block;padding:1px 7px;border-radius:999px;font-size:11px;font-weight:700}
 .snrG{background:rgba(103,234,148,.18);color:#67ea94}.snrY{background:rgba(240,200,90,.18);color:#f0c85a}
 .snrR{background:rgba(239,109,109,.2);color:#ef6d6d}.snrN{background:var(--p2);color:var(--mut)}
 .hop0{color:#67ea94}.hop1{color:#7fc8ff}.hopN{color:#f0c85a}
 tr.stale td{opacity:.45}
 .meshgraph{background:#0e0f13;border:1px solid var(--bd);border-radius:10px;margin:4px 0 14px}
 .meshgraph svg{display:block;width:100%;height:auto}
 .glegend{font-size:11px;color:var(--mut);margin:2px 2px 12px;display:flex;gap:14px;flex-wrap:wrap}
 .glegend b{color:var(--tx);font-weight:600}
 .traceitem{border:1px solid var(--bd);border-radius:8px;padding:9px 11px;margin:8px 0;font-size:12.5px}
 .traceitem .path{font-family:ui-monospace,monospace;font-size:12px;margin-top:4px}
 .delx{cursor:pointer;color:var(--mut);margin-right:7px;font-size:11px;padding:0 3px;border-radius:4px}
 .delx:hover{color:#fff;background:var(--dn)}
 .nick{cursor:pointer;border-bottom:1px dotted var(--mut)}.nick:hover{color:var(--ac);border-color:var(--ac)}
 @media(max-width:980px){.layout{grid-template-columns:1fr}
  nav{position:static;border-right:0;border-bottom:1px solid var(--bd);display:flex;gap:8px;overflow:auto}nav hr,nav .step span{display:none}
  aside.tel{position:static;height:auto;border-left:0;border-top:1px solid var(--bd)}aside.tel .telbody{max-height:460px}
  .telgrip{display:none}}
</style></head><body>
<header><span id="dot" class="dot"></span><b>Siliqs Gateway</b>
 <span class="tag">mesh ⇄ MQTT bridge</span><span style="flex:1"></span>
 <span id="status" class="note"></span>
 <button id="lang" class="lang" type="button">中文</button>
 <button id="quit" class="btn stop" style="padding:6px 14px;font-size:12px" data-en="⏻ Quit" data-zh="⏻ 離開">⏻ Quit</button></header>

<div class="layout">
 <nav>
  <a class="step" onclick="go('cNode')"><span class="n">1</span><span><b data-en="Your node" data-zh="你的節點">Your node</b><span data-en="how this PC reaches the mesh" data-zh="這台電腦怎麼連上 mesh">how this PC reaches the mesh</span></span></a>
  <a class="step" onclick="go('cBroker')"><span class="n">2</span><span><b data-en="Your broker" data-zh="你的 broker">Your broker</b><span data-en="where the data goes" data-zh="資料送去哪">where the data goes</span></span></a>
  <a class="step" onclick="go('cRun')"><span class="n">3</span><span><b data-en="Run &amp; watch" data-zh="執行與監看">Run &amp; watch</b><span data-en="start + live telemetry" data-zh="啟動 + 即時遙測">start + live telemetry</span></span></a>
  <a class="step" onclick="go('cMesh')"><span class="n">4</span><span><b data-en="Mesh &amp; RF" data-zh="Mesh 網路">Mesh &amp; RF</b><span data-en="signal, hops, plan relays" data-zh="訊號、跳數、規劃中繼">signal, hops, plan relays</span></span></a>
  <hr>
  <p class="note" style="padding:0 6px" data-en-html="Need a wireless <b>serial cable</b>? That’s a separate tool now — <span class=&quot;mono&quot;>siliqs-serial-mqtt</span> — it rides this same broker." data-zh-html="需要無線<b>序列線</b>?那現在是獨立工具 — <span class=&quot;mono&quot;>siliqs-serial-mqtt</span> — 掛在同一個 broker。">Need a wireless <b>serial cable</b>? That’s a separate tool now — <span class="mono">siliqs-serial-mqtt</span> — it rides this same broker.</p>
 </nav>

 <main>
  <!-- the canonical gateway diagram (same picture as mesh.siliqs.net · diagrams/gwhost.svg) -->
  <figure class="gwdgram">
   <figcaption data-en="Collector → Gateway app → MQTT → your dashboard" data-zh="匯集器 → Gateway 應用 → MQTT → 你的儀表板">Collector → Gateway app → MQTT → your dashboard</figcaption>
   <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 75 1060 340" fill="none" stroke="#8b919e" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" font-family="Helvetica, Arial, sans-serif" role="img" aria-label="Sensor data collection over LoRa mesh"><rect x="0" y="0" width="1060" height="450" fill="#161a21" stroke="none"/>
    <g transform="translate(47,117) scale(0.72)" stroke-width="2.6"><path d="M22 10 v4 M28 10 v4 M34 10 v4 M40 10 v4"/><rect x="16" y="14" width="32" height="36" rx="4"/><rect x="21" y="19" width="22" height="11" rx="1.5"/><circle cx="25" cy="39" r="1.6"/><circle cx="32" cy="39" r="1.6"/><circle cx="39" cy="39" r="1.6"/><path d="M22 50 v4 M28 50 v4 M34 50 v4 M40 50 v4"/></g>
    <g transform="translate(47,227) scale(0.72)" stroke-width="2.6"><path d="M18 22 v28 h28 v-28"/><path d="M18 36 h28"/><path d="M32 9 v25"/><rect x="28" y="5" width="8" height="5" rx="1.5"/><circle cx="32" cy="36" r="3.5"/></g>
    <g transform="translate(47,337) scale(0.72)" stroke-width="2.6"><path d="M28 14 v22 a6 6 0 1 0 8 0 V14 a4 4 0 0 0 -8 0 Z"/><path d="M32 19 v19"/><circle cx="32" cy="43" r="2.5" fill="#8b919e" stroke="none"/><path d="M37 20 h3 M37 26 h3 M37 32 h3"/></g>
    <g transform="translate(152,117) scale(0.72)" stroke-width="2.6"><g transform="matrix(1,0,0,1,0,4)"><path d="M52,17L52,44C52,47.311 49.311,50 46,50L18,50C14.689,50 12,47.311 12,44L12,17C12,13.689 14.689,11 18,11L46,11C49.311,11 52,13.689 52,17Z"/></g><g transform="matrix(1,0,0,1,0,4)"><path d="M45,21L45,36C45,37.104 44.104,38 43,38L21,38C19.896,38 19,37.104 19,36L19,21C19,19.896 19.896,19 21,19L43,19C44.104,19 45,19.896 45,21Z"/></g><g transform="matrix(1.571429,0,0,1,-23,4)"><path d="M42,51.5L42,54.5C42,55.328 41.572,56 41.045,56L35.955,56C35.428,56 35,55.328 35,54.5L35,51.5C35,50.672 35.428,50 35.955,50L41.045,50C41.572,50 42,50.672 42,51.5Z"/></g><g transform="matrix(1,0,0,1,0,3)"><path d="M43,11L43,1"/></g></g>
    <g transform="translate(152,227) scale(0.72)" stroke-width="2.6"><g transform="matrix(1,0,0,1,0,4)"><path d="M52,17L52,44C52,47.311 49.311,50 46,50L18,50C14.689,50 12,47.311 12,44L12,17C12,13.689 14.689,11 18,11L46,11C49.311,11 52,13.689 52,17Z"/></g><g transform="matrix(1,0,0,1,0,4)"><path d="M45,21L45,36C45,37.104 44.104,38 43,38L21,38C19.896,38 19,37.104 19,36L19,21C19,19.896 19.896,19 21,19L43,19C44.104,19 45,19.896 45,21Z"/></g><g transform="matrix(1.571429,0,0,1,-23,4)"><path d="M42,51.5L42,54.5C42,55.328 41.572,56 41.045,56L35.955,56C35.428,56 35,55.328 35,54.5L35,51.5C35,50.672 35.428,50 35.955,50L41.045,50C41.572,50 42,50.672 42,51.5Z"/></g><g transform="matrix(1,0,0,1,0,3)"><path d="M43,11L43,1"/></g></g>
    <g transform="translate(152,337) scale(0.72)" stroke-width="2.6"><g transform="matrix(1,0,0,1,0,4)"><path d="M52,17L52,44C52,47.311 49.311,50 46,50L18,50C14.689,50 12,47.311 12,44L12,17C12,13.689 14.689,11 18,11L46,11C49.311,11 52,13.689 52,17Z"/></g><g transform="matrix(1,0,0,1,0,4)"><path d="M45,21L45,36C45,37.104 44.104,38 43,38L21,38C19.896,38 19,37.104 19,36L19,21C19,19.896 19.896,19 21,19L43,19C44.104,19 45,19.896 45,21Z"/></g><g transform="matrix(1.571429,0,0,1,-23,4)"><path d="M42,51.5L42,54.5C42,55.328 41.572,56 41.045,56L35.955,56C35.428,56 35,55.328 35,54.5L35,51.5C35,50.672 35.428,50 35.955,50L41.045,50C41.572,50 42,50.672 42,51.5Z"/></g><g transform="matrix(1,0,0,1,0,3)"><path d="M43,11L43,1"/></g></g>
    <g stroke-width="1.8"><path d="M90 137 H150 M90 143 H150"/><path d="M90 247 H150 M90 253 H150"/><path d="M90 357 H150 M90 363 H150"/></g>
    <text x="120" y="128" font-size="9.5" fill="#8b919e" stroke="none" text-anchor="middle">RS485 A/B</text>
    <text x="120" y="238" font-size="9.5" fill="#8b919e" stroke="none" text-anchor="middle">RS485 A/B</text>
    <text x="120" y="348" font-size="9.5" fill="#8b919e" stroke="none" text-anchor="middle">RS485 A/B</text>
    <text x="63" y="180" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Energy meter" data-zh="電表">Energy meter</text>
    <text x="63" y="290" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Level sensor" data-zh="液位感測器">Level sensor</text>
    <text x="63" y="400" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Temp sensor" data-zh="溫度感測器">Temp sensor</text>
    <text x="175" y="405" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="SQC485Iv2 — Sensor node" data-zh="SQC485Iv2 — 感測節點">SQC485Iv2 — Sensor node</text>
    <g stroke-dasharray="5 6"><path d="M195 133 Q 360 150 512 244"/><path d="M195 250 Q 360 250 512 250"/><path d="M195 367 Q 360 350 512 256"/><path d="M193 150 Q 235 195 193 240"/><path d="M193 260 Q 235 305 193 350"/></g>
    <text x="360" y="108" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Every node is also a relay —" data-zh="每顆節點也是中繼 —">Every node is also a relay —</text>
    <text x="360" y="123" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="forwards its neighbours' data, no dedicated relay hardware" data-zh="替鄰居轉送資料，免專用中繼硬體">forwards its neighbours' data, no dedicated relay hardware</text>
    <g transform="translate(517,227) scale(0.72)" stroke-width="2.6"><g transform="matrix(1,0,0,1,0,4)"><path d="M52,17L52,44C52,47.311 49.311,50 46,50L18,50C14.689,50 12,47.311 12,44L12,17C12,13.689 14.689,11 18,11L46,11C49.311,11 52,13.689 52,17Z"/></g><g transform="matrix(1,0,0,1,0,4)"><path d="M45,21L45,36C45,37.104 44.104,38 43,38L21,38C19.896,38 19,37.104 19,36L19,21C19,19.896 19.896,19 21,19L43,19C44.104,19 45,19.896 45,21Z"/></g><g transform="matrix(1.571429,0,0,1,-23,4)"><path d="M42,51.5L42,54.5C42,55.328 41.572,56 41.045,56L35.955,56C35.428,56 35,55.328 35,54.5L35,51.5C35,50.672 35.428,50 35.955,50L41.045,50C41.572,50 42,50.672 42,51.5Z"/></g><g transform="matrix(1,0,0,1,0,3)"><path d="M43,11L43,1"/></g></g>
    <text x="540" y="300" font-size="12" fill="#8b919e" stroke="none" text-anchor="middle" data-en="SQC485Iv2 — Collector" data-zh="SQC485Iv2 — 匯集器">SQC485Iv2 — Collector</text>
    <path d="M612 95 V405" stroke-dasharray="4 6"/>
    <text x="604" y="88" font-size="11" fill="#8b919e" stroke="none" text-anchor="end" data-en="LoRa side" data-zh="LoRa 側">LoRa side</text>
    <text x="620" y="88" font-size="11" fill="#8b919e" stroke="none" text-anchor="start" data-en="your network" data-zh="你的網路">your network</text>
    <path d="M566 250 H664"/>
    <rect x="592" y="233" width="46" height="13" fill="#161a21" stroke="none"/>
    <text x="615" y="242" font-size="11" fill="#8b919e" stroke="none" text-anchor="middle">USB / BLE</text>
    <g transform="translate(667,227) scale(0.72)" stroke-width="3.0" stroke="#5cc6d4"><rect x="8" y="16" width="30" height="21" rx="2"/><path d="M23 37 v4"/><path d="M17 42 h12"/><rect x="44" y="16" width="12" height="27" rx="2"/><circle cx="50" cy="20.5" r="1.2"/><circle cx="50" cy="24.5" r="1.2"/><path d="M46 34 h8"/></g>
    <text x="690" y="300" font-size="12" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Gateway app" data-zh="Gateway 應用">Gateway app</text>
    <text x="690" y="315" font-size="10.5" fill="#8b919e" stroke="none" text-anchor="middle">Linux / macOS / Windows</text>
    <text x="690" y="333" font-size="10.5" fill="#8b919e" stroke="none" text-anchor="middle" data-en="Only this host touches your IP network" data-zh="只有這台主機碰你的 IP 網路">Only this host touches your IP network</text>
    <path d="M716 250 H806"/><path d="M806 250 l-7 -4 M806 250 l-7 4"/>
    <g transform="translate(830,250)" stroke-width="2.2"><rect x="-16" y="-14" width="32" height="28" rx="3"/><circle cx="0" cy="0" r="3"/><path d="M-16 0 H-6 M6 0 H16 M0 -14 V-8 M0 8 V14"/></g>
    <text x="830" y="300" font-size="12" fill="#8b919e" stroke="none" text-anchor="middle">MQTT broker</text>
    <path d="M854 250 H936"/><path d="M936 250 l-7 -4 M936 250 l-7 4"/>
    <g transform="translate(985,250)" stroke-width="2.2"><rect x="-26" y="-20" width="52" height="34" rx="2"/><path d="M0 14 v6 M-10 24 h20"/><path d="M-16 6 V-4 M-8 6 V-8 M0 6 V-2"/><circle cx="12" cy="-5" r="6"/><path d="M12 -5 V-11 M12 -5 H18"/></g>
    <text x="985" y="300" font-size="11.5" fill="#8b919e" stroke="none" text-anchor="middle">DB · Node-RED · Grafana</text>
   </svg>
  </figure>

  <!-- 1. node -->
  <div class="card" id="cNode">
   <h2 data-en="1 · Your node" data-zh="1 · 你的節點">1 · Your node</h2>
   <p class="sub" data-en="This PC talks to the LoRa mesh through one connected node (your gateway node). Pick how it’s attached." data-zh="這台電腦透過一顆連著的節點(你的閘道節點)連上 LoRa mesh。選它怎麼接。">This PC talks to the LoRa mesh through one connected node (your gateway node). Pick how it’s attached.</p>
   <div class="seg" id="iface">
    <label><input type="radio" name="iface" value="usb" checked> USB</label>
    <label><input type="radio" name="iface" value="ble"> BLE</label></div>
   <div class="row" id="usbRow" style="margin-top:12px">
    <div class="f" style="flex:2"><label data-en="Serial port" data-zh="序列埠">Serial port</label><select id="portSel"></select></div>
    <div class="f" style="flex:0"><label>&nbsp;</label><button id="refresh" class="btn stop" type="button" style="padding:9px 16px">↻</button></div></div>
   <div class="f hide" id="portManual" style="margin-top:8px"><label data-en="Device path" data-zh="裝置路徑">Device path</label><input id="port" placeholder="/dev/ttyACM0"></div>
   <div class="row hide" id="bleRow" style="margin-top:12px">
    <div class="f"><label data-en="BLE device name or address" data-zh="BLE 裝置名稱或位址">BLE device name or address</label><input id="ble" placeholder="e.g. SQC485I"></div></div>
   <p class="note" style="margin:8px 0 0" data-en-html="Nodes are marked <b>◆</b> and sorted first. Not listed? Choose <b>“✎ type a path…”</b> (e.g. a container’s <span class=&quot;mono&quot;>/dev/…</span>)." data-zh-html="節點會標上 <b>◆</b> 並排在最前。沒列出來?選 <b>「✎ type a path…」</b>(例如容器裡的 <span class=&quot;mono&quot;>/dev/…</span>)。">Nodes are marked <b>◆</b> and sorted first. Not listed? Choose <b>“✎ type a path…”</b> (e.g. a container’s <span class="mono">/dev/…</span>).</p>
  </div>

  <!-- 2. broker -->
  <div class="card" id="cBroker">
   <h2 data-en="2 · Your broker" data-zh="2 · 你的 broker">2 · Your broker</h2>
   <p class="sub" data-en="Where the Gateway publishes the telemetry (and listens for downlink). No broker of your own? Use the built-in one — it just works." data-zh="閘道把遙測發佈到這裡(也在這裡聽下行)。沒有自己的 broker?用內建的,開箱即用。">Where the Gateway publishes the telemetry (and listens for downlink). No broker of your own? Use the built-in one — it just works.</p>
   <div class="seg" id="brokerMode">
    <label><input type="radio" name="brokerMode" value="builtin" checked> <span data-en="Built-in broker" data-zh="內建 broker">Built-in broker</span></label>
    <label><input type="radio" name="brokerMode" value="external"> <span data-en="External broker" data-zh="外部 broker">External broker</span></label></div>
   <div class="row" id="builtinRow" style="margin-top:12px">
    <div class="f" style="flex:0"><label data-en="Port" data-zh="埠">Port</label><input id="builtinPort" placeholder="1883" style="width:100px"></div>
    <div class="f" style="flex:2"><label>&nbsp;</label><div class="note" id="builtinHint" style="padding-top:9px">—</div></div>
   </div>
   <div class="hide" id="externalRow">
    <div class="row" style="margin-top:12px">
     <div class="f" style="flex:2"><label data-en="Broker host" data-zh="Broker 主機">Broker host</label><input id="broker" placeholder="broker.example.com"></div>
     <div class="f"><label data-en="Port" data-zh="埠">Port</label><input id="brokerPort" placeholder="1883 / 8883"></div>
    </div>
    <div class="row" style="margin-top:12px">
     <div class="f"><label data-en="Username (optional)" data-zh="帳號(選填)">Username (optional)</label><input id="username" autocomplete="off"></div>
     <div class="f"><label data-en="Password (optional)" data-zh="密碼(選填)">Password (optional)</label><input id="password" type="password" autocomplete="new-password"></div>
     <div class="f" style="flex:0"><label>&nbsp;</label><label style="display:flex;align-items:center;gap:7px;color:var(--tx);padding-top:9px;white-space:nowrap"><input type="checkbox" id="tls" style="width:auto"> <span data-en="Use TLS" data-zh="用 TLS">Use TLS</span></label></div>
    </div>
    <p class="note" style="margin:8px 0 0" data-en="Password is sent to the bridge via an environment variable — never in the command line, the log, or back to this page." data-zh="密碼以環境變數傳給 bridge — 不會出現在命令列、log,也不會回傳到這個頁面。">Password is sent to the bridge via an environment variable — never in the command line, the log, or back to this page.</p>
    <div class="hide" id="certRow" style="margin-top:12px;border-top:1px dashed var(--bd);padding-top:12px">
     <p class="note" style="margin:0 0 8px" data-en="Client-certificate auth (e.g. AWS IoT) — file paths on this machine. Leave blank for username/password only." data-zh="用戶端憑證認證(如 AWS IoT)— 填這台機器上的檔案路徑。只用帳密就留空。">Client-certificate auth (e.g. AWS IoT) — file paths on this machine. Leave blank for username/password only.</p>
     <div class="row">
      <div class="f"><label data-en="CA cert path" data-zh="CA 憑證路徑">CA cert path</label><input id="caCert" placeholder="/path/AmazonRootCA1.pem" autocomplete="off"></div>
      <div class="f"><label data-en="Client cert path" data-zh="用戶端憑證路徑">Client cert path</label><input id="clientCert" placeholder="/path/device.pem.crt" autocomplete="off"></div>
      <div class="f"><label data-en="Client key path" data-zh="私鑰路徑">Client key path</label><input id="clientKey" placeholder="/path/private.pem.key" autocomplete="off"></div>
     </div>
    </div>
   </div>
   <div class="row" style="margin-top:12px">
    <div class="f"><label data-en="Channel — leave blank to auto-read" data-zh="頻道 — 留空自動帶入">Channel — leave blank to auto-read</label><input id="channel" data-ph-en="auto — from the node" data-ph-zh="自動 — 讀自節點" placeholder="auto — from the node"></div>
   </div>
   <p class="note" style="margin:8px 0 0" data-en-html="Leave <b>Channel</b> blank and the gateway reads the node’s primary channel name for you. Full topics — telemetry (PortNum 256): <span class=&quot;mono&quot;>msh/2/json/&lt;channel&gt;/&lt;sender&gt;</span> (e.g. <span class=&quot;mono&quot;>msh/2/json/LongFast/!7d51bdc4</span>) · every packet: <span class=&quot;mono&quot;>siliqs/rx/&lt;your-node&gt;/&lt;portnum&gt;</span> · downlink: <span class=&quot;mono&quot;>siliqs/tx/&lt;your-node&gt;</span>." data-zh-html="<b>頻道</b>留空,閘道會自動讀節點的主頻道名。完整 topic — 遙測(PortNum 256):<span class=&quot;mono&quot;>msh/2/json/&lt;頻道&gt;/&lt;送出的節點&gt;</span>(例:<span class=&quot;mono&quot;>msh/2/json/LongFast/!7d51bdc4</span>)· 每個封包:<span class=&quot;mono&quot;>siliqs/rx/&lt;你的節點&gt;/&lt;portnum&gt;</span> · 下行:<span class=&quot;mono&quot;>siliqs/tx/&lt;你的節點&gt;</span>。">Leave <b>Channel</b> blank and the gateway reads the node’s primary channel name for you. Full topics — telemetry (PortNum 256): <span class="mono">msh/2/json/&lt;channel&gt;/&lt;sender&gt;</span> (e.g. <span class="mono">msh/2/json/LongFast/!7d51bdc4</span>) · every packet: <span class="mono">siliqs/rx/&lt;your-node&gt;/&lt;portnum&gt;</span> · downlink: <span class="mono">siliqs/tx/&lt;your-node&gt;</span>.</p>
  </div>

  <!-- 3. run -->
  <div class="card" id="cRun">
   <h2 data-en="3 · Run &amp; watch" data-zh="3 · 執行與監看">3 · Run &amp; watch</h2>
   <p class="sub" data-en="Start the Gateway; it stays running and forwards packets. Live telemetry appears below." data-zh="啟動閘道;它會持續運行、轉發封包。即時遙測會顯示在下方。">Start the Gateway; it stays running and forwards packets. Live telemetry appears below.</p>
   <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <button id="start" class="btn go" data-en="▶ Start gateway" data-zh="▶ 啟動閘道">▶ Start gateway</button>
    <button id="stop" class="btn stop" disabled data-en="■ Stop" data-zh="■ 停止">■ Stop</button>
    <span id="msg" class="msg note"></span>
   </div>
   <pre class="log" id="log" style="margin-top:14px">—</pre>
  </div>

  <!-- 4. mesh / RF -->
  <div class="card" id="cMesh">
   <h2 data-en="4 · Mesh &amp; RF — plan your deployment" data-zh="4 · Mesh 網路 — 規劃你的部署">4 · Mesh &amp; RF — plan your deployment</h2>
   <p class="sub" data-en-html="Live view of every node your gateway has heard: signal (SNR), how many hops away, and last-heard. Use it to spot weak links and decide where a relay would help. <b>Signal comes from the radio, not MQTT</b> — it needs the gateway running." data-zh-html="閘道聽過的每顆節點即時狀態:訊號(SNR)、離幾跳、多久沒回。用來找出弱連結、判斷該在哪裡加中繼。<b>訊號來自無線電、不是 MQTT</b> — 需要閘道運行中。">Live view of every node your gateway has heard.</p>

   <div class="meshgraph"><svg id="meshSvg" viewBox="0 0 640 360" role="img" aria-label="mesh link map"></svg></div>
   <div class="glegend">
    <span><b data-en="Link" data-zh="連結">Link</b>: <span style="color:#67ea94">━ direct</span> · <span style="color:#f0c85a">╌ via relay</span></span>
    <span><b>SNR</b>: <span class="chip snrG">good</span> <span class="chip snrY">weak</span> <span class="chip snrR">near floor</span></span>
    <span><b data-en="Node" data-zh="節點">Node</b>: <span style="color:#ef6d6d" data-en="red = overdue" data-zh="紅 = 逾時未回">red = overdue</span></span>
   </div>

   <div class="meshbar">
    <select id="traceSel"></select>
    <button class="btn go mini" id="traceBtn" data-en="↳ Traceroute" data-zh="↳ 路徑追蹤">↳ Traceroute</button>
    <button class="btn stop mini" id="meshRefresh" data-en="↻ Refresh" data-zh="↻ 重新整理">↻ Refresh</button>
    <span style="flex:1"></span>
    <button class="btn stop mini" id="niBtn" data-en="Enable NeighborInfo (gateway)" data-zh="開啟 NeighborInfo(閘道)">Enable NeighborInfo (gateway)</button>
   </div>
   <p class="note" style="margin:-4px 0 12px" data-en-html="Traceroute shows the real path a packet takes to a node and the SNR at each hop — the direct answer to “does A still reach home, and through whom?”. NeighborInfo (optional) makes nodes broadcast their direct neighbours every 4–6 h; enabling it here affects the <b>gateway node only</b> — remote nodes need the same set separately." data-zh-html="Traceroute 顯示封包到某節點的真實路徑 + 每一跳的 SNR — 直接回答「A 還通不通、經過誰」。NeighborInfo(選配)讓節點每 4~6 小時廣播自己的直接鄰居;在這裡開只影響<b>閘道節點</b> — 遠端節點要各自開。">Traceroute shows the real path to a node.</p>

   <div id="traceResults"></div>

   <table>
    <thead><tr>
     <th data-en="Node" data-zh="節點">Node</th>
     <th data-en="Role" data-zh="角色">Role</th>
     <th data-en="Hops" data-zh="跳數">Hops</th>
     <th>SNR</th>
     <th>RSSI</th>
     <th data-en="Rx" data-zh="收到數">Rx</th>
     <th data-en="Batt" data-zh="電量">Batt</th>
     <th data-en="First seen" data-zh="首次發現">First seen</th>
     <th data-en="Last heard" data-zh="上次聽到">Last heard</th>
    </tr></thead>
    <tbody id="meshRows"><tr><td colspan="9" class="note" data-en="no node data yet — start the gateway; the RF map builds as nodes are heard" data-zh="尚無節點資料 — 啟動閘道,聽到節點後就會建圖">no node data yet</td></tr></tbody>
   </table>
  </div>
 </main>

 <!-- live telemetry — right activity panel (mesh.siliqs.net style); drag the grip to resize -->
 <aside class="tel">
  <div class="telgrip" id="telgrip" title="Drag to resize"></div>
  <div class="telhead"><span id="dot2" class="dot"></span><b data-en="Live telemetry" data-zh="即時遙測">Live telemetry</b><span class="note" id="telMeta"></span></div>
  <div class="telbody">
   <p class="sub" data-en="Latest frame per node, decoded from the broker. Proof the data is flowing." data-zh="每個節點最新一筆(從 broker 解出)。證明資料在流動。">Latest frame per node, decoded from the broker. Proof the data is flowing.</p>
   <table><thead><tr><th data-en="Node" data-zh="節點">Node</th><th data-en="Heard" data-zh="時間">Heard</th><th data-en="B" data-zh="B">B</th><th data-en="Payload (hex)" data-zh="內容(hex)">Payload (hex)</th></tr></thead>
    <tbody id="telLatest"><tr><td colspan="4" class="note" data-en="no telemetry yet — start the gateway and wait for a field node to report" data-zh="尚無遙測 — 啟動閘道並等現場節點回報">no telemetry yet — start the gateway and wait for a field node to report</td></tr></tbody></table>
   <h2><span data-en="Event stream" data-zh="事件串流">Event stream</span> <span class="note" data-en="newest first" data-zh="最新在上">newest first</span></h2>
   <table><tbody id="telEvents"></tbody></table>
  </div>
 </aside>
</div>

<script>
const $=id=>document.getElementById(id);
const val=id=>($(id)?$(id).value.trim():'');
function go(id){const el=$(id);if(el)el.scrollIntoView({behavior:'smooth',block:'start'});}

/* ---- i18n: EN / 繁中 (data-en/data-zh textContent, data-en-html/data-zh-html innerHTML) ---- */
let LANG='en';
const TX={
 en:{running:'running',stopped:'stopped',starting:'starting…',
     quitAsk:'Stop the gateway and quit the app?',
     quitBye:'Siliqs Gateway has quit. You can close this tab.',
     telEmpty:'no telemetry yet — start the gateway and wait for a field node to report'},
 zh:{running:'運行中',stopped:'已停止',starting:'啟動中…',
     quitAsk:'停止閘道並離開程式?',
     quitBye:'Siliqs Gateway 已離開,可以關掉這個分頁。',
     telEmpty:'尚無遙測 — 啟動閘道並等現場節點回報'}
};
const t=k=>(TX[LANG]||TX.en)[k];
function applyLang(l){
 LANG=(l==='zh')?'zh':'en';
 document.documentElement.lang=LANG==='zh'?'zh-Hant':'en';
 document.querySelectorAll('[data-en]').forEach(el=>{const v=el.getAttribute('data-'+LANG);if(v!=null)el.textContent=v;});
 document.querySelectorAll('[data-en-html]').forEach(el=>{const v=el.getAttribute('data-'+LANG+'-html');if(v!=null)el.innerHTML=v;});
 document.querySelectorAll('[data-ph-en]').forEach(el=>{const v=el.getAttribute('data-ph-'+LANG);if(v!=null)el.placeholder=v;});
 $('lang').textContent=LANG==='en'?'中文':'EN';
 try{localStorage.setItem('smb-lang',LANG);}catch(e){}
}
$('lang').onclick=()=>applyLang(LANG==='en'?'zh':'en');

function ifaceVal(){return document.querySelector('input[name=iface]:checked').value}
function brokerModeVal(){return document.querySelector('input[name=brokerMode]:checked').value}
function syncUI(){
 $('usbRow').classList.toggle('hide', ifaceVal()!=='usb');
 $('portManual').classList.toggle('hide', ifaceVal()!=='usb' || $('portSel').value!=='__type__');
 $('bleRow').classList.toggle('hide', ifaceVal()!=='ble');
 const bm=brokerModeVal();
 $('builtinRow').classList.toggle('hide', bm!=='builtin');
 $('externalRow').classList.toggle('hide', bm!=='external');
 $('certRow').classList.toggle('hide', !(bm==='external' && $('tls').checked));
}
$('iface').onchange=syncUI; $('brokerMode').onchange=syncUI; $('tls').onchange=syncUI;

/* ---- port dropdown (real <select>; nodes marked ◆; “✎ type a path…” for custom) ---- */
const MANUAL='__type__';
function applyPortSel(){
 const sel=$('portSel'), manual=sel.value===MANUAL;
 $('portManual').classList.toggle('hide', !manual || ifaceVal()!=='usb');
 if(manual){ $('port').focus(); } else if(sel.value){ $('port').value=sel.value; }
}
async function loadPorts(){
 let d; try{ d=await (await fetch('/api/ports')).json(); }catch(e){ return; }
 const sel=$('portSel'), cur=$('port').value;
 const opts=(d.ports||[]).map(p=>{const mark=p.node?'◆ ':'';const tail=p.desc?` — ${p.desc}`:'';
   return `<option value="${p.device}">${mark}${p.device}${tail}</option>`;}).join('');
 sel.innerHTML=(opts||'')+`<option value="${MANUAL}">✎ type a path…</option>`;
 const known=(d.ports||[]).some(p=>p.device===cur);
 if(cur&&known) sel.value=cur; else if(cur) sel.value=MANUAL;
 else if((d.ports||[]).length) sel.value=d.ports[0].device;
 applyPortSel();
}
$('refresh').onclick=loadPorts;
$('portSel').onchange=applyPortSel;

function cfg(){
 const c={iface:ifaceVal()};
 if(c.iface==='usb') c.port=val('port'); else c.ble=val('ble');
 c.broker_mode=brokerModeVal();
 if(c.broker_mode==='builtin'){ c.builtin_port=+val('builtinPort')||1883; }
 else {
  c.broker=val('broker')||'127.0.0.1'; c.broker_port=+val('brokerPort')||1883;
  const u=val('username'), p=$('password').value; if(u)c.username=u; if(p)c.password=p;
  if($('tls').checked){ c.tls=true;
   const ca=val('caCert'), cc=val('clientCert'), ck=val('clientKey');
   if(ca)c.ca_cert=ca; if(cc)c.client_cert=cc; if(ck)c.client_key=ck;
  }
 }
 const ch=val('channel'); if(ch) c.channel=ch;   // blank → gateway auto-reads it from the node
 return c;
}
$('start').onclick=async()=>{
 $('msg').textContent=t('starting');$('msg').style.color='';
 const r=await fetch('/api/start',{method:'POST',body:JSON.stringify(cfg())});
 const d=await r.json();$('msg').textContent=d.msg;$('msg').style.color=d.ok?'':'var(--dn)';
};
$('stop').onclick=async()=>{const d=await (await fetch('/api/stop',{method:'POST'})).json();$('msg').textContent=d.msg;};
$('quit').onclick=async()=>{
 if(!confirm(t('quitAsk')))return;
 try{await fetch('/api/quit',{method:'POST'});}catch(e){}
 document.body.innerHTML='<div style="padding:40px;font:16px system-ui">'+t('quitBye')+'</div>';
};

let seeded=false;
function seed(c){
 if(!c||seeded)return;seeded=true;
 const set=(id,v)=>{if(v!=null&&v!=='')$(id).value=v};
 if(c.iface){const el=document.querySelector(`input[name=iface][value="${c.iface}"]`);if(el)el.checked=true}
 if(c.broker_mode){const el=document.querySelector(`input[name=brokerMode][value="${c.broker_mode}"]`);if(el)el.checked=true}
 set('builtinPort',c.builtin_port);
 set('ble',c.ble);set('broker',c.broker);set('brokerPort',c.broker_port);set('channel',c.channel);
 set('username',c.username);            // password is never sent back — user re-enters on reload
 if($('tls'))$('tls').checked=!!c.tls;
 set('caCert',c.ca_cert);set('clientCert',c.client_cert);set('clientKey',c.client_key);
 syncUI();
 if(c.port){$('port').value=c.port;loadPorts();}
}
async function poll(){
 try{const d=await (await fetch('/api/state')).json();
  seed(d.cfg);
  $('dot').classList.toggle('on',d.running);
  $('dot2').classList.toggle('on',d.running);
  $('status').textContent=t(d.running?'running':'stopped');
  $('start').disabled=d.running;$('stop').disabled=!d.running;
  const bp=(+val('builtinPort')||1883), ip=d.lanip||'127.0.0.1';
  $('builtinHint').innerHTML=(LANG==='zh'
    ? `儀表板連這台:<span class="mono">${ip}:${bp}</span>(同機用 <span class="mono">127.0.0.1</span>)`
    : `Point dashboards at <span class="mono">${ip}:${bp}</span> (same machine: <span class="mono">127.0.0.1</span>)`);
  const log=$('log'),bot=log.scrollTop+log.clientHeight>=log.scrollHeight-20;
  log.textContent=(d.log||[]).join('\n')||'—';if(bot)log.scrollTop=log.scrollHeight;
 }catch(e){}
}
const ago=t=>{const s=Math.max(0,Math.round(Date.now()/1000-t));return s<60?s+'s ago':Math.round(s/60)+'m ago'};
const hx=h=>h.replace(/(..)/g,'$1 ').trim();
async function pollTel(){
 try{const d=await (await fetch('/api/telemetry')).json();
  $('telMeta').textContent=d.broker?('· broker '+d.broker):'';
  const L=(d.latest||[]).sort((a,b)=>b.t-a.t);
  $('telLatest').innerHTML=L.length?L.map(e=>`<tr><td class="node mono">${e.node}</td><td class="note">${ago(e.t)}</td><td>${e.len}</td><td class="mono">${hx(e.hex)}</td></tr>`).join(''):`<tr><td colspan="4" class="note">${t('telEmpty')}</td></tr>`;
  const E=(d.events||[]).slice().reverse();
  $('telEvents').innerHTML=E.map(e=>`<tr><td class="note" style="white-space:nowrap">${new Date(e.t*1000).toLocaleTimeString()}</td><td class="node mono">${e.node}</td><td class="mono">${hx(e.hex)}</td></tr>`).join('');
 }catch(e){}
}
/* ---- mesh / RF insight ---- */
const MESH_STALE=180;                       // node overdue after ~2× the 85s report interval
const idTail=id=>(id||'').replace(/^!/,'').slice(-4);
const lastByte=id=>parseInt((id||'').replace('!',''),16)&0xff;   // node-num low byte (for relay_node)
const escH=s=>(s==null?'':(''+s)).replace(/[<>&"]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]));
let NICK={}; try{NICK=JSON.parse(localStorage.getItem('smb-nick')||'{}');}catch(e){}
const nodeName=n=>NICK[n.id]||n.short||idTail(n.id);
function setNick(id){
 const cur=NICK[id]||'';
 const v=prompt(LANG==='zh'?('節點 '+id+' 的暱稱(留空清除):'):('Nickname for '+id+' (blank clears):'),cur);
 if(v===null)return;
 if(v.trim())NICK[id]=v.trim(); else delete NICK[id];
 try{localStorage.setItem('smb-nick',JSON.stringify(NICK));}catch(e){}
 renderMeshTable(); drawMeshGraph();
}
function relayOf(n){                         // who relayed n's traffic to us?
 const tr=MESH.traces.find(x=>x.to===n.id && x.ok && Array.isArray(x.routeBack) && x.routeBack.length);
 if(tr){const r=tr.routeBack.find(x=>x && x!=='!ffffffff' && x!==n.id); if(r)return r;}   // authoritative
 if(n.hops===0)return null;                 // truly direct — ignore relay_node noise
 if(n.relay){const m=MESH.nodes.find(x=>x.id!==n.id && lastByte(x.id)===n.relay); if(m)return m.id;}
 return null;                               // relay_node byte match (best-effort hint; hops≥1 only)
}
function snrChip(s){
 if(s==null) return {c:'snrN',t:'—'};
 const cls=s>0?'snrG':(s>-10?'snrY':'snrR');
 return {c:cls,t:s.toFixed(1)};
}
let MESH={nodes:[],links:[],traces:[],gw:null};
function meshParent(n,byId){                 // draw the edge to the relay if we know it, else gateway
 const r=relayOf(n);
 return (r && byId[r]) ? r : MESH.gw;
}
function drawMeshGraph(){
 const svg=$('meshSvg'); if(!svg) return;
 const W=640,H=360,cx=320,cy=182,rx=250,ry=132;
 const gw=MESH.gw, byId={}; MESH.nodes.forEach(n=>byId[n.id]=n);
 const others=MESH.nodes.filter(n=>!n.self);
 const pos={}; if(gw) pos[gw]={x:cx,y:cy};
 others.forEach((n,i)=>{const a=-Math.PI/2 + i*2*Math.PI/Math.max(1,others.length);
   pos[n.id]={x:cx+rx*Math.cos(a),y:cy+ry*Math.sin(a)};});
 const now=Date.now()/1000, esc=s=>(s||'').replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
 let edges='',nodes='';
 others.forEach(n=>{
  const p=pos[n.id]; if(!p) return;
  const par=meshParent(n,byId), pp=pos[par]||pos[gw]; if(!pp) return;
  const direct=(par===gw)&&(n.hops===0);   // solid ONLY for a true 0-hop direct link; ≥1 hop = dashed
  const sc=snrChip(n.snr).c, col=sc==='snrG'?'#67ea94':sc==='snrY'?'#f0c85a':sc==='snrR'?'#ef6d6d':'#5b6270';
  edges+=`<line x1="${p.x.toFixed(0)}" y1="${p.y.toFixed(0)}" x2="${pp.x.toFixed(0)}" y2="${pp.y.toFixed(0)}" stroke="${col}" stroke-width="${direct?2.4:1.6}" ${direct?'':'stroke-dasharray="5 4"'} opacity=".8"/>`;
 });
 function nodeSvg(n,x,y,isgw){
  const stale=n.last&&(now-n.last>MESH_STALE), r=isgw?22:17;
  const fill=isgw?'#243b52':(stale?'#3a2226':'#1d1f27'), stroke=isgw?'#3fa7d6':(stale?'#ef6d6d':'#3a4152');
  const lbl=esc(nodeName(n)), sub=isgw?'GW':('!'+idTail(n.id));
  let hlabel='';
  if(!isgw&&n.hops!=null){
   hlabel=LANG==='zh'?`${n.hops} 跳`:`${n.hops} hop${n.hops===1?'':'s'}`;
   if(n.hops>=1){const rid=relayOf(n); if(rid&&byId[rid]) hlabel+=' ⤳'+esc(nodeName(byId[rid]));}
  }
  const slabel=(!isgw&&n.snr!=null)?`${n.snr.toFixed(0)} dB`:'';
  const sub2=[hlabel,slabel].filter(Boolean).join(' · ');
  const hopTxt=sub2?`<text x="${x}" y="${y+r+22}" font-size="9" fill="#9aa0ad" text-anchor="middle">${sub2}</text>`:'';
  return `<circle cx="${x}" cy="${y}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="2"/>`
    +`<text x="${x}" y="${y-1}" font-size="10" font-weight="700" fill="#e7e9ee" text-anchor="middle">${lbl}</text>`
    +`<text x="${x}" y="${y+10}" font-size="8" fill="#9aa0ad" text-anchor="middle">${sub}</text>`+hopTxt;
 }
 others.forEach(n=>{const p=pos[n.id]; if(p) nodes+=nodeSvg(n,p.x,p.y,false);});
 const g=MESH.nodes.find(n=>n.self); if(g&&pos[gw]) nodes+=nodeSvg(g,cx,cy,true);
 svg.innerHTML=edges+nodes || `<text x="320" y="180" font-size="12" fill="#5b6270" text-anchor="middle">—</text>`;
}
function renderTraces(){
 const box=$('traceResults'); if(!box) return;
 const ts=MESH.traces.filter(x=>x.to).sort((a,b)=>(b.t||0)-(a.t||0)).slice(0,4);
 if(!ts.length){box.innerHTML='';return;}
 const fmt=a=>(a==null?'·':a.toFixed(1));
 box.innerHTML=ts.map(x=>{
  if(x.state==='sent') return `<div class="traceitem"><b class="mono">${x.to}</b> — ${LANG==='zh'?'已送出,等回覆…':'sent, waiting…'}</div>`;
  if(!x.ok) return `<div class="traceitem"><b class="mono">${x.to}</b> — <span style="color:var(--dn)">${LANG==='zh'?'失敗':'failed'}: ${x.err||''}</span></div>`;
  const fwd=(x.snrTowards||[]).map(fmt).join(' → ');
  const back=(x.routeBack||[]).filter(v=>v!=='!ffffffff');
  const backPath=[x.to,...back,MESH.gw||'home'].map(idTail).join(' → ');
  const backSnr=(x.snrBack||[]).map(fmt).join(' / ');
  return `<div class="traceitem"><b class="mono">${x.to}</b> — ${LANG==='zh'?'回程路徑':'return path'}:`
    +`<div class="path">${backPath}</div>`
    +`<div class="note" style="margin-top:3px">${LANG==='zh'?'去程 SNR':'toward SNR'}: ${fwd||'·'} dB · ${LANG==='zh'?'回程 SNR':'back SNR'}: ${backSnr||'·'} dB</div></div>`;
 }).join('');
}
function renderMeshTable(){
 const tb=$('meshRows'); if(!tb) return;
 const now=Date.now()/1000;
 const ns=MESH.nodes.slice().sort((a,b)=>(a.self?-1:b.self?1:0)||((a.hops??9)-(b.hops??9)));
 if(!ns.length) return;
 tb.innerHTML=ns.map(n=>{
  const stale=n.last&&(now-n.last>MESH_STALE);
  const sc=snrChip(n.snr);
  const hopCls=n.self?'':(n.hops===0||n.hops===1?'hop'+n.hops:(n.hops!=null?'hopN':''));
  const hopTxt=n.self?'—':(n.hops!=null?n.hops:'?');
  const role=(n.role||'').replace('CLIENT_','').replace('_',' ')||(n.self?'GATEWAY':'—');
  const batt=n.batt!=null&&n.batt<=100?n.batt+'%':(n.batt===101?'⚡':'—');
  const rssi=n.rssi!=null?n.rssi+' dBm':'—';
  const rx=n.count!=null&&n.count>0?n.count:'—';
  const first=n.first?new Date(n.first*1000).toLocaleTimeString():'—';
  const del=n.self?'':`<span class="delx" title="${LANG==='zh'?'刪除此節點資料':'forget this node'}" onclick="meshForget('${n.id}')">✕</span>`;
  const nm=`<span class="nick" title="${LANG==='zh'?'點擊設定暱稱':'click to set a nickname'}" onclick="setNick('${n.id}')">${escH(nodeName(n))}</span>`;
  return `<tr class="${stale?'stale':''}"><td class="node mono">${del}${n.self?'★ ':''}${nm} <span class="note">!${idTail(n.id)}</span></td>`
   +`<td class="note">${role}</td><td class="${hopCls}">${hopTxt}</td>`
   +`<td><span class="chip ${sc.c}">${sc.t}</span></td><td class="note">${rssi}</td>`
   +`<td class="note">${rx}</td><td class="note">${batt}</td>`
   +`<td class="note">${first}</td><td class="note">${n.last?ago(n.last):'—'}</td></tr>`;
 }).join('');
}
function fillTraceSel(){
 const sel=$('traceSel'); if(!sel) return;
 const cur=sel.value, opts=MESH.nodes.filter(n=>!n.self)
   .map(n=>`<option value="${n.id}">${escH(nodeName(n))} · !${idTail(n.id)}</option>`).join('');
 const want=(LANG==='zh'?'選節點做 traceroute…':'pick a node to traceroute…');
 sel.innerHTML=`<option value="">${want}</option>`+opts;
 if(cur) sel.value=cur;
}
async function meshCmd(obj){
 try{await fetch('/api/mesh-cmd',{method:'POST',body:JSON.stringify(obj)});}catch(e){}
}
function meshForget(id){
 const msg=LANG==='zh'?('刪除節點 '+id+' 的所有資料?\n(清除計數/首次發現,並從節點 NodeDB 移除)'):('Forget all data for '+id+'?\n(clears counters/first-seen and removes it from the node’s NodeDB)');
 if(!confirm(msg))return;
 MESH.nodes=MESH.nodes.filter(n=>n.id!==id);   // optimistic: drop it from the view now
 renderMeshTable(); drawMeshGraph();
 meshCmd({cmd:'forget',node:id});
}
async function pollMesh(){
 try{const d=await (await fetch('/api/mesh')).json();
  MESH={nodes:d.nodes||[],links:d.links||[],traces:d.traces||[],gw:d.gw};
  renderMeshTable(); drawMeshGraph(); renderTraces(); fillTraceSel();
 }catch(e){}
}
$('traceBtn').onclick=()=>{const to=$('traceSel').value; if(to) meshCmd({cmd:'traceroute',to});};
$('meshRefresh').onclick=()=>meshCmd({cmd:'refresh'});
$('niBtn').onclick=()=>{
 const on=$('niBtn').dataset.on==='1';
 meshCmd({cmd:'neighborinfo',enable:!on});
 $('niBtn').dataset.on=on?'0':'1';
 $('niBtn').textContent=(!on)?(LANG==='zh'?'關閉 NeighborInfo(閘道)':'Disable NeighborInfo (gateway)')
                              :(LANG==='zh'?'開啟 NeighborInfo(閘道)':'Enable NeighborInfo (gateway)');
};

/* ---- drag the grip to resize the telemetry panel (persisted) ---- */
const _layout=document.querySelector('.layout');
const _defTelw=()=>Math.round(window.innerWidth*0.30);   // default = 30% of the browser width
let telw, telwCustom=false;
try{const s=parseInt(localStorage.getItem('smb-telw')); if(s){telw=s; telwCustom=true;}}catch(e){}
if(!telw) telw=_defTelw();
function setTelw(w){
 const maxw=Math.max(360,window.innerWidth-460);   // grow up to viewport − (nav + a minimal main)
 telw=Math.max(280,Math.min(maxw,Math.round(w)));
 _layout.style.setProperty('--telw',telw+'px');
}
setTelw(telw);
// on resize: keep the 30% proportion until the user drags a custom width, then just re-clamp
window.addEventListener('resize',()=>setTelw(telwCustom?telw:_defTelw()));
$('telgrip').addEventListener('mousedown',e=>{
 e.preventDefault();
 const startX=e.clientX, startW=telw, grip=$('telgrip');
 grip.classList.add('drag'); document.body.style.cursor='col-resize'; document.body.style.userSelect='none';
 function mv(ev){ setTelw(startW + (startX - ev.clientX)); }   // drag left → wider
 function up(){ document.removeEventListener('mousemove',mv); document.removeEventListener('mouseup',up);
   grip.classList.remove('drag'); document.body.style.cursor=''; document.body.style.userSelect='';
   telwCustom=true; try{localStorage.setItem('smb-telw',telw);}catch(e){} }
 document.addEventListener('mousemove',mv); document.addEventListener('mouseup',up);
});
$('telgrip').addEventListener('dblclick',()=>{   // dbl-click = reset to the 30% default
 telwCustom=false; setTelw(_defTelw()); try{localStorage.removeItem('smb-telw');}catch(e){}
});

let initLang='en';try{initLang=localStorage.getItem('smb-lang')||((navigator.language||'').toLowerCase().indexOf('zh')===0?'zh':'en');}catch(e){}
applyLang(initLang);
syncUI();loadPorts();poll();setInterval(poll,1000);pollTel();setInterval(pollTel,2000);
pollMesh();setInterval(pollMesh,4000);
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser(description="Local control panel for the Siliqs Gateway.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Siliqs Gateway control panel → http://{args.host}:{args.port}  (Ctrl-C to stop)")
    saved = _load_cfg()           # appliance: auto-start the last applied config on boot
    if saved:
        try:
            ok, msg = runner.start(saved)
            print(f"  auto-start: {msg}")
        except ValueError as e:
            print(f"  auto-start skipped: {e}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping.")


if __name__ == "__main__":
    main()
