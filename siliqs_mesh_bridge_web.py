#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_mesh_bridge_web.py — a localhost control panel for the bridge.

Start/stop the bridge and watch its log from a browser, **no command line**. It
needs host OS access (serial ports / PTY / BLE / MQTT), so it is a tiny local HTTP
server (stdlib only — no extra deps) that spawns the verified `siliqs_mesh_bridge`
CLI as a subprocess and streams its output. Binds 127.0.0.1 by default.

  siliqs-mesh-bridge-web            # then open http://127.0.0.1:8765
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import siliqs_mesh_bridge   # reuse the verified CLI (we spawn this file)

BRIDGE = siliqs_mesh_bridge.__file__
# If set, the last applied config is saved here and auto-started on launch — so a
# deployed appliance survives a reboot without anyone clicking Start.
CONFIG_FILE = os.environ.get("SMB_CONFIG_FILE")


class Telemetry:
    """Subscribes to the MQTT broker (msh/2/json/#) and keeps a live view: latest
    frame per node + a rolling event stream. Used by the telemetry panel so the same
    web UI shows data, not just controls. paho-mqtt is imported lazily."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latest = {}
        self.events = deque(maxlen=200)
        self.cli = None
        self.broker = None

    def watch(self, broker, port):
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
                c.connect(broker, int(port), 60)
                c.subscribe("msh/2/json/#")
                c.loop_start()
                self.cli = c
                self.broker = f"{broker}:{port}"
            except Exception:
                self.cli = None

    def _on_msg(self, client, userdata, msg):  # noqa: ARG002
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

    def snapshot(self):
        with self.lock:
            return {"broker": self.broker, "latest": list(self.latest.values()),
                    "events": list(self.events)[-120:]}


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
    """Owns the single bridge subprocess + a rolling log."""

    def __init__(self):
        self.proc = None
        self.argv = None
        self.cfg = None
        self.log = deque(maxlen=400)
        self.lock = threading.Lock()

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, cfg):
        # Mode switch: only one handler can hold the single USB at a time, so
        # starting a new config first stops whatever is currently running. This
        # makes "pick a mode → Start" a one-click switch (no separate Stop).
        if self.running():
            self.stop()
        with self.lock:
            argv = self._build_argv(cfg)          # raises ValueError on bad config
            self.argv = argv
            self.cfg = cfg
            self.log.clear()
            self.log.append("$ siliqs-mesh-bridge " + " ".join(argv))
            # Spawn the bridge CLI. In a normal Python install we run the .py with the
            # interpreter; inside a PyInstaller bundle there IS no python and no .py on
            # disk, so sys.executable is the frozen app itself — re-exec it with the
            # --sq-run-bridge flag (handled in app.py) so it runs the bridge instead.
            cmd = ([sys.executable, "--sq-run-bridge", *argv]
                   if getattr(sys, "frozen", False)
                   else [sys.executable, "-u", BRIDGE, *argv])
            self.proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            threading.Thread(target=self._pump, daemon=True).start()
        _save_cfg(cfg)                              # persist for reboot auto-start
        if cfg.get("handler") == "mqtt" and cfg.get("broker"):
            telemetry.watch(cfg["broker"], cfg.get("broker_port", 1883))  # live view
        return True, "started"

    def _pump(self):
        try:
            for line in self.proc.stdout:
                self.log.append(line.rstrip("\n"))
        except Exception:
            pass
        self.log.append("— bridge process exited —")

    def stop(self):
        console.close()                     # drop any web-terminal attach to the PTY
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
                raise ValueError("USB serial port required")
            a += ["--port", cfg["port"]]
        h = cfg.get("handler", "serial")
        a += ["--handler", h]
        if h == "serial":
            if not cfg.get("peer"):
                raise ValueError("peer node required (e.g. !7d51bdc4)")
            a += ["--peer", cfg["peer"]]
            if cfg.get("link"):
                a += ["--link", cfg["link"]]
            a += ["--mode", cfg.get("mode", "line"), "--mtu", str(int(cfg.get("mtu", 200)))]
        elif h == "mqtt":
            if not cfg.get("broker"):
                raise ValueError("MQTT broker host required")
            a += ["--broker", cfg["broker"],
                  "--broker-port", str(int(cfg.get("broker_port", 1883))),
                  "--channel", cfg.get("channel", "LongFast")]
        return a


runner = Runner()


class TtyConsole:
    """Browser terminal for a running serial-tunnel handler.

    A serial handler exposes a PTY slave at cfg['link']; this opens that slave on a
    *separate* fd so the browser can read what the peer sent and write lines back —
    i.e. screen/miniterm, but in the web UI. Only meaningful while a serial handler
    runs; the mqtt gateway has no PTY. If a CLI screen/miniterm is also attached to
    the same link they share (race) the byte stream — use one at a time."""
    MAXBUF = 64 * 1024

    def __init__(self):
        self.lock = threading.Lock()
        self.ser = None
        self.path = None
        self.buf = bytearray()
        self.base = 0          # absolute stream offset of buf[0]

    def ensure(self, path):
        with self.lock:
            if self.ser is not None and self.path == path:
                return True
            self._close_locked()
            if not path:
                return False
            try:
                import serial
                self.ser = serial.Serial(path, 115200, timeout=0.3)
            except Exception:
                self.ser = None
                self.path = None
                return False
            self.path = path
            self.base = 0
            self.buf = bytearray()
            ser = self.ser
            threading.Thread(target=self._read_loop, args=(ser,), daemon=True).start()
            return True

    def _read_loop(self, ser):
        while True:
            with self.lock:
                if self.ser is not ser:
                    return
            try:
                d = ser.read(256)
            except Exception:
                return
            if d:
                with self.lock:
                    if self.ser is not ser:
                        return
                    self.buf += d
                    if len(self.buf) > self.MAXBUF:
                        drop = len(self.buf) - self.MAXBUF
                        del self.buf[:drop]
                        self.base += drop

    def send(self, data):
        with self.lock:
            if self.ser is None:
                return False
            try:
                self.ser.write(data)
                return True
            except Exception:
                return False

    def read(self, off):
        with self.lock:
            total = self.base + len(self.buf)
            if off < self.base:
                off = self.base
            return bytes(self.buf[off - self.base:]), total

    def _close_locked(self):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
        self.ser = None
        self.path = None

    def close(self):
        with self.lock:
            self._close_locked()


console = TtyConsole()


def _serial_link():
    """The PTY link path iff a serial tunnel is currently running, else None."""
    c = runner.cfg or {}
    if runner.running() and c.get("handler") == "serial":
        return c.get("link")
    return None


def _tty_read(path):
    off = 0
    if "?" in path:
        for kv in path.split("?", 1)[1].split("&"):
            if kv.startswith("off="):
                try:
                    off = int(kv[4:])
                except ValueError:
                    off = 0
    link = _serial_link()
    if not link:
        return {"attached": False, "off": 0, "data": "",
                "note": "start a serial tunnel (handler = serial, with a link path) to use the terminal"}
    if not console.ensure(link):
        return {"attached": False, "off": 0, "data": "", "note": f"opening {link}…"}
    chunk, total = console.read(off)
    return {"attached": True, "off": total, "data": chunk.decode("utf-8", "replace"),
            "note": f"attached to {link}"}


def list_ports():
    """Best-effort serial port list.

    pyserial's comports() enumerates via /sys/class/tty, which is NOT populated
    for a Docker `--device`-mapped node — so inside a container the dropdown comes
    up empty even though the device works. To avoid that confusion we also glob
    /dev for the usual serial nodes and fold in whatever port is configured/running.
    """
    seen, out = set(), []

    def add(dev, desc=""):
        if dev and dev not in seen:
            seen.add(dev)
            out.append({"device": dev, "desc": desc})

    try:
        from serial.tools import list_ports as lp
        for p in lp.comports():
            add(p.device, p.description or "")
    except Exception:
        pass
    # raw /dev scan — catches container --device maps and udev symlinks pyserial misses
    import glob
    for pat in ("/dev/ttyACM*", "/dev/ttyUSB*", "/dev/cu.*", "/dev/serial/by-id/*",
                "/dev/sqc485i-*", "/dev/*-meshtastic"):
        for dev in sorted(glob.glob(pat)):
            add(dev, "device node")
    # always surface the configured/running port so it can be re-selected
    try:
        cfg = _load_cfg()
        if cfg.get("port"):
            add(cfg["port"], "configured")
    except Exception:
        pass
    return out


def scan_nodes(iface_kind, port, ble):
    """Briefly open the radio and return its node DB so the UI can offer a peer
    pick-list. Needs the (single) radio, so it only works while the bridge is
    stopped — returns an error otherwise. Reuses the verified CLI connect helpers."""
    if runner.running():
        return None, "stop the bridge first — scanning the mesh needs the radio (shared with the bridge)"
    iface = None
    try:
        if iface_kind == "ble":
            if not ble:
                return None, "set the BLE name/address first"
            iface = siliqs_mesh_bridge.open_ble(ble)
        else:
            if not port:
                return None, "set the serial port first"
            iface = siliqs_mesh_bridge.open_usb(port)
        my = iface.myInfo.my_node_num & 0xffffffff
        out = []
        for n in (iface.nodes or {}).values():
            num = n.get("num")
            if num is None or (num & 0xffffffff) == my:   # skip self — can't pipe to yourself
                continue
            u = n.get("user") or {}
            out.append({"id": "!%08x" % (num & 0xffffffff),
                        "short": u.get("shortName") or "", "long": u.get("longName") or "",
                        "hops": n.get("hopsAway"), "lastHeard": n.get("lastHeard") or 0})
        out.sort(key=lambda x: x["lastHeard"], reverse=True)
        return out, None
    except BaseException as e:   # open_usb raises SystemExit on failure
        return None, f"scan failed: {e}"
    finally:
        if iface is not None:
            try:
                iface.close()
            except Exception:
                pass


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
            self._send(200, {"running": runner.running(), "argv": runner.argv,
                             "cfg": runner.cfg, "log": list(runner.log)})
        elif self.path == "/api/telemetry":
            self._send(200, telemetry.snapshot())
        elif self.path.startswith("/api/tty/read"):
            self._send(200, _tty_read(self.path))
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
        elif self.path == "/api/nodes":
            nodes, err = scan_nodes(cfg.get("iface", "usb"), cfg.get("port"), cfg.get("ble"))
            self._send(200, {"nodes": nodes or [], "error": err})
        elif self.path == "/api/tty/send":
            link = _serial_link()
            ok = False
            if link and console.ensure(link):
                data = cfg.get("text", "").encode("utf-8", "replace")
                if data and not data.endswith((b"\n", b"\r")):
                    data += b"\n"          # line-mode handler flushes on a terminator
                ok = console.send(data)
            self._send(200, {"ok": ok})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *a):   # keep the console quiet
        pass


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>siliqs-mesh-bridge · control panel</title>
<style>
 :root{--bg:#14151a;--panel:#1d1f27;--panel2:#23262f;--bd:#2e3140;--tx:#e7e9ee;--mut:#9aa0ad;--ac:#67ea94;--acd:#3fbf6e;--dn:#ff6b6b}
 *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
 header{display:flex;align-items:center;gap:12px;padding:14px 20px;background:var(--panel);border-bottom:1px solid var(--bd)}
 header b{font-size:16px} .dot{width:10px;height:10px;border-radius:50%;background:var(--mut)} .dot.on{background:var(--ac);box-shadow:0 0 8px var(--ac)}
 main{max-width:780px;margin:0 auto;padding:22px 20px}
 .card{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:18px 20px;margin-bottom:18px}
 h2{margin:0 0 12px;font-size:15px} label{font-size:12px;color:var(--mut);display:block;margin-bottom:4px}
 .row{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:12px} .field{display:flex;flex-direction:column;flex:1;min-width:160px}
 input,select{background:var(--panel2);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:8px 10px;font:inherit}
 .seg{display:flex;gap:8px} .seg label{display:flex;align-items:center;gap:6px;color:var(--tx);background:var(--panel2);border:1px solid var(--bd);border-radius:8px;padding:8px 12px;cursor:pointer;margin:0}
 button{background:var(--panel2);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:9px 16px;font:inherit;cursor:pointer}
 button.primary{background:var(--ac);color:#06210f;border-color:var(--ac);font-weight:600} button.danger{color:var(--dn);border-color:#5a2b2b}
 button:disabled{opacity:.45;cursor:not-allowed} .note{font-size:12px;color:var(--mut)}
 pre{background:#0e0f13;border:1px solid var(--bd);border-radius:8px;padding:12px;height:280px;overflow:auto;font:12px ui-monospace,monospace;white-space:pre-wrap;color:#cfd3da}
 table{width:100%;border-collapse:collapse;font-size:13px} th,td{text-align:left;padding:6px 9px;border-bottom:1px solid var(--bd);vertical-align:top} th{color:var(--mut);font-weight:500;font-size:11px}
 .mono{font-family:ui-monospace,monospace;font-size:12px;word-break:break-all} .node{color:var(--ac);font-family:ui-monospace,monospace}
 .hide{display:none}
</style></head><body>
<header><span id="dot" class="dot"></span><b>siliqs-mesh-bridge</b><span class="note">control panel</span>
 <span style="flex:1"></span><span id="status" class="note">stopped</span></header>
<main>
 <div class="card">
  <h2>Transport (the node)</h2>
  <div class="seg" id="iface">
   <label><input type="radio" name="iface" value="usb" checked> USB</label>
   <label><input type="radio" name="iface" value="ble"> BLE</label></div>
  <div class="row" id="usbRow" style="margin-top:12px">
   <div class="field" style="flex:2"><label>Serial port</label><input id="port" list="portlist" placeholder="/dev/ttyACM0 — or type a path"><datalist id="portlist"></datalist></div>
   <div class="field" style="flex:0"><label>&nbsp;</label><button id="refresh" type="button">↻ refresh</button></div></div>
  <p class="note" id="portHint" style="margin:2px 0 0">Pick from the list, or <b>type the device path</b> — in a container the auto-list is often empty even when the device works (e.g. <code>/dev/sqc485i-meshtastic</code>).</p>
  <div class="row hide" id="bleRow" style="margin-top:12px">
   <div class="field"><label>BLE device name or address</label><input id="ble" placeholder="e.g. SQC485I"></div></div>
 </div>

 <div class="card">
  <h2>What to run</h2>
  <div class="seg" id="handler">
   <label><input type="radio" name="handler" value="serial" checked> Serial pipe</label>
   <label><input type="radio" name="handler" value="mqtt"> MQTT gateway</label></div>

  <div id="serialCfg" style="margin-top:12px">
   <div class="row">
    <div class="field"><label>Peer node (the other end)</label><input id="peer" placeholder="!7d51bdc4 — or pick below"><select id="peerPick" style="margin-top:6px"><option value="">— scan, then pick a node —</option></select></div>
    <div class="field"><label>Virtual port path (link)</label><input id="link" placeholder="/tmp/meshtty"></div></div>
   <div class="row">
    <div class="field"><label>Framing</label><select id="mode"><option value="line">line (per Enter)</option><option value="stream">stream (binary)</option></select></div>
    <div class="field"><label>Max bytes / packet</label><input id="mtu" type="number" value="50" min="1" max="233"></div></div>
   <div class="row" style="align-items:center;margin-top:4px">
    <button id="scanNodes" type="button">↻ scan mesh for nodes</button>
    <span class="note" id="scanNote">Pick the peer from the dropdown, or type a node id. Scanning briefly opens the radio — stop the bridge first.</span></div>
   <p class="note">Run this on <b>both</b> hosts, each peer pointing at the other. Open the printed
    <code>/dev/pts/…</code> (or the link) with your serial software.</p>
  </div>

  <div id="mqttCfg" class="hide" style="margin-top:12px">
   <div class="row">
    <div class="field" style="flex:2"><label>Broker host</label><input id="broker" placeholder="192.168.0.9"></div>
    <div class="field"><label>Port</label><input id="brokerPort" type="number" value="1883"></div>
    <div class="field"><label>Channel</label><input id="channel" placeholder="LongFast"></div></div>
   <p class="note">Run on a gateway node (role CLIENT_MUTE). Forwards Modbus telemetry to MQTT.</p>
  </div>
 </div>

 <div class="card">
  <div class="row" style="align-items:center;margin:0">
   <button id="start" class="primary">Start</button>
   <button id="stop" class="danger" disabled>Stop</button>
   <span id="msg" class="note"></span></div>
  <h2 style="margin-top:16px">Log</h2>
  <pre id="log">—</pre>
 </div>

 <div class="card hide" id="ttyCard">
  <h2>Terminal <span class="note" id="ttyNote">serial tunnel — type a line, Enter sends it over the mesh</span></h2>
  <pre id="ttyOut" style="height:240px;overflow:auto;white-space:pre-wrap;word-break:break-all">—</pre>
  <div class="row" style="align-items:center;margin:8px 0 0">
   <input id="ttyIn" style="flex:1" placeholder="type a line, press Enter…" disabled autocomplete="off">
   <button id="ttySend" disabled>Send</button>
   <button id="ttyClear" type="button">Clear</button></div>
 </div>

 <div class="card" id="telCard">
  <h2>Telemetry <span class="note" id="telBroker"></span></h2>
  <table><thead><tr><th>Node</th><th>Last heard</th><th>Bytes</th><th>Payload (raw hex)</th></tr></thead>
   <tbody id="telLatest"><tr><td colspan="4" class="note">no telemetry yet — start an MQTT gateway</td></tr></tbody></table>
  <h2 style="margin-top:14px">Event stream <span class="note">newest first</span></h2>
  <div style="max-height:240px;overflow:auto"><table><tbody id="telEvents"></tbody></table></div>
 </div>
</main>
<script>
const $=id=>document.getElementById(id);
const val=id=>$(id).value.trim();
function ifaceVal(){return document.querySelector('input[name=iface]:checked').value}
function handlerVal(){return document.querySelector('input[name=handler]:checked').value}
function syncUI(){
 $('usbRow').classList.toggle('hide', ifaceVal()!=='usb');
 $('bleRow').classList.toggle('hide', ifaceVal()!=='ble');
 $('serialCfg').classList.toggle('hide', handlerVal()!=='serial');
 $('mqttCfg').classList.toggle('hide', handlerVal()!=='mqtt');
 $('ttyCard').classList.toggle('hide', handlerVal()!=='serial');
 $('telCard').classList.toggle('hide', handlerVal()!=='mqtt');
}
$('iface').onchange=syncUI; $('handler').onchange=syncUI;
async function loadPorts(){
 const r=await fetch('/api/ports'); const d=await r.json();
 const dl=$('portlist');                       // datalist suggestions; the input keeps its typed value
 dl.innerHTML=d.ports.map(p=>`<option value="${p.device}">${p.desc}</option>`).join('');
 if(!$('port').value && d.ports.length) $('port').value=d.ports[0].device;
}
$('refresh').onclick=loadPorts;
function cfg(){
 const c={iface:ifaceVal(),handler:handlerVal()};
 if(c.iface==='usb') c.port=val('port'); else c.ble=val('ble');
 if(c.handler==='serial'){c.peer=val('peer');c.link=val('link');c.mode=val('mode');c.mtu=+val('mtu')||50;}
 else {c.broker=val('broker');c.broker_port=+val('brokerPort')||1883;c.channel=val('channel')||'LongFast';}
 return c;
}
$('start').onclick=async()=>{
 $('msg').textContent='starting…';
 const r=await fetch('/api/start',{method:'POST',body:JSON.stringify(cfg())});
 const d=await r.json(); $('msg').textContent=d.msg; $('msg').style.color=d.ok?'var(--mut)':'var(--dn)';
};
$('stop').onclick=async()=>{const r=await fetch('/api/stop',{method:'POST'});const d=await r.json();$('msg').textContent=d.msg;};
let formSeeded=false;
function seedForm(c){           // populate the form from the saved/running config (once)
 if(!c||formSeeded) return; formSeeded=true;
 const set=(id,v)=>{if(v!=null&&v!=='')$(id).value=v};
 if(c.iface){const el=document.querySelector(`input[name=iface][value="${c.iface}"]`);if(el)el.checked=true}
 if(c.handler){const el=document.querySelector(`input[name=handler][value="${c.handler}"]`);if(el)el.checked=true}
 set('ble',c.ble);set('peer',c.peer);set('link',c.link);set('mode',c.mode);set('mtu',c.mtu);
 set('broker',c.broker);set('brokerPort',c.broker_port);set('channel',c.channel);
 syncUI();
 if(c.port)$('port').value=c.port;   // free-text input now — just restore the path
}
async function poll(){
 try{const r=await fetch('/api/state');const d=await r.json();
  seedForm(d.cfg);
  $('dot').classList.toggle('on',d.running);
  $('status').textContent=d.running?'running':'stopped';
  $('start').disabled=d.running; $('stop').disabled=!d.running;
  const log=$('log'); const atBottom=log.scrollTop+log.clientHeight>=log.scrollHeight-20;
  log.textContent=(d.log||[]).join('\n')||'—';
  if(atBottom) log.scrollTop=log.scrollHeight;
 }catch(e){}
}
const fago=t=>{const s=Math.max(0,Math.round(Date.now()/1000-t));return s<60?s+'s ago':Math.round(s/60)+'m ago'};
const fhex=h=>h.replace(/(..)/g,'$1 ').trim();
async function pollTel(){
 try{const d=await (await fetch('/api/telemetry')).json();
  $('telBroker').textContent=d.broker?('· '+d.broker):'';
  const L=(d.latest||[]).sort((a,b)=>b.t-a.t);
  $('telLatest').innerHTML=L.length?L.map(e=>`<tr><td class="node">${e.node}</td><td class="note">${fago(e.t)}</td><td>${e.len}</td><td class="mono">${fhex(e.hex)}</td></tr>`).join(''):'<tr><td colspan="4" class="note">no telemetry yet — start an MQTT gateway</td></tr>';
  const E=(d.events||[]).slice().reverse();
  $('telEvents').innerHTML=E.map(e=>`<tr><td class="note" style="white-space:nowrap">${new Date(e.t*1000).toLocaleTimeString()}</td><td class="node">${e.node}</td><td class="mono">${fhex(e.hex)}</td></tr>`).join('');
 }catch(e){}
}
let ttyOff=0;
async function pollTty(){
 if(handlerVal()!=='serial') return;
 try{const d=await (await fetch('/api/tty/read?off='+ttyOff)).json();
  $('ttyNote').textContent=d.note||'';
  $('ttyIn').disabled=!d.attached; $('ttySend').disabled=!d.attached;
  if(d.attached){
   if(d.data){const o=$('ttyOut'); if(o.textContent==='—')o.textContent='';
    const atBottom=o.scrollHeight-o.scrollTop-o.clientHeight<24; o.textContent+=d.data;
    if(atBottom)o.scrollTop=o.scrollHeight;}
   if(typeof d.off==='number') ttyOff=d.off;
  }
 }catch(e){}
}
async function ttySend(){
 const t=$('ttyIn').value; if(!t)return;
 const o=$('ttyOut'); if(o.textContent==='—')o.textContent='';
 o.textContent+='» '+t+'\n'; o.scrollTop=o.scrollHeight;   // local echo (handler doesn't echo back)
 $('ttyIn').value='';
 try{await fetch('/api/tty/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:t})});}catch(e){}
}
async function scanNodes(){
 const b=$('scanNodes'), old=b.textContent; b.disabled=true; b.textContent='scanning…';
 $('scanNote').textContent='opening the radio… (cold handshake can take a few seconds)';
 try{
  const body={iface:ifaceVal(),port:val('port'),ble:val('ble')};
  const d=await (await fetch('/api/nodes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  if(d.error){$('scanNote').textContent=d.error; }
  else{
   const ns=d.nodes||[];
   $('peerPick').innerHTML='<option value="">— pick a node —</option>'+ns.map(n=>{
    const nm=[n.short,n.long].filter(Boolean).join(' / ')||n.id;
    const hop=(n.hops==null)?'':` · ${n.hops} hop`;
    return `<option value="${n.id}">${nm}${hop} — ${n.id}</option>`;}).join('');
   $('scanNote').textContent=ns.length?`found ${ns.length} node(s) — pick from the dropdown below the Peer box`:'no other nodes heard yet';
  }
 }catch(e){$('scanNote').textContent='scan failed';}
 b.disabled=false; b.textContent=old;
}
$('scanNodes').onclick=scanNodes;
$('peerPick').onchange=()=>{ if($('peerPick').value) $('peer').value=$('peerPick').value; };
$('ttySend').onclick=ttySend;
$('ttyIn').addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();ttySend();}});
$('ttyClear').onclick=()=>{$('ttyOut').textContent='—';};
syncUI(); loadPorts(); poll(); setInterval(poll,1000); pollTel(); setInterval(pollTel,2000); pollTty(); setInterval(pollTty,1000);
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser(description="Local control panel for siliqs-mesh-bridge.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"siliqs-mesh-bridge control panel → http://{args.host}:{args.port}  (Ctrl-C to stop)")
    saved = _load_cfg()           # appliance: auto-start the last applied config on boot
    if saved:
        try:
            ok, msg = runner.start(saved)
            print(f"auto-start from {CONFIG_FILE}: {msg}")
        except ValueError as e:
            print(f"auto-start skipped (bad saved config): {e}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        runner.stop()


if __name__ == "__main__":
    main()
