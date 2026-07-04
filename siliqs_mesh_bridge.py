#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_mesh_bridge.py — the Siliqs Gateway: a bidirectional mesh ⇄ MQTT bridge.

ONE job. The connected Meshtastic node (USB or BLE) is your host's access point onto
the LoRa mesh; this program carries packets between that mesh and an MQTT broker, both
ways. Everything else — dashboards, decoders, a database writer, even a virtual serial
cable — is just an ordinary MQTT client hanging off the broker. The node runs the NORMAL
firmware: no firmware change, no custom BLE service, no console takeover.

    field edge nodes ──mesh──▶ your node (USB/BLE) ──▶ THIS (mesh⇄MQTT) ──▶ broker ──▶ sub/pub clients

Let G = the connected node's id ("!hexhexhx"). The bridge does:

  uplink  (mesh → MQTT), for every received data packet:
    • PortNum 256 (SQC485I Modbus telemetry) also → nafco-compatible JSON on
      msh/2/json/<channel>/<node>  (so the existing MQTT→decoder→InfluxDB pipeline is
      reusable unchanged), and
    • generic → siliqs/rx/<G>/<portnum>  as {from,to,portnum,channel,rssi,snr,t,data(b64)}.

  downlink (MQTT → mesh):
    • subscribe siliqs/tx/<G>  ({to,portnum,data(b64),wantAck?,channelIndex?}) and send it
      out through node G. This is what lets a downstream MQTT client transmit on the mesh.

The virtual-serial-cable feature is NO LONGER built in here — it moved OUT to a separate
MQTT client, `siliqs_serial_mqtt.py` (a "serial-over-MQTT" sub-program). It opens a PTY and
maps it to siliqs/tx|rx/<G> on PortNum 260, so the serial data rides the same broker as
everything else and can run on ANY host that can reach the broker.

⚠ LoRa is best-effort and low-bandwidth (BW500/SF9 ≈ a few hundred B/s, high latency).
Suits LOW-RATE / request-reply traffic (Modbus, console), NOT high-throughput streams.
Each packet carries ≤ ~233 bytes. Also note: siliqs/tx/<G> lets anyone with broker access
transmit on your mesh — secure the broker accordingly.

Examples:
  # Gateway on a CLIENT_MUTE node, forwarding telemetry + bridging to a local broker:
  python siliqs_mesh_bridge.py --iface usb --port /dev/ttyACM0 --broker 127.0.0.1
  # BLE instead of USB (RPi/Linux host with no RS485):
  python siliqs_mesh_bridge.py --iface ble --ble 'SQC485I' --broker mybroker.lan
  # Add a live telemetry web view:
  python siliqs_mesh_bridge.py --iface usb --port /dev/ttyACM0 --broker 127.0.0.1 --web-port 9090

Needs the `meshtastic` package (BLE also needs `bleak`) and `paho-mqtt`.
"""
import argparse
import base64
import json
import os
import sys
import time
from collections import deque

from pubsub import pub

MODBUS_PORTNUM = 256   # PRIVATE_APP — where the SQC485I Modbus telemetry arrives
SERIAL_PORTNUM = 260   # the serial-over-MQTT client uses this (unnamed private port)


# ── south side: open the node connection ──────────────────────────────────────
def open_usb(port, retries=5):
    """Serial (USB CDC) interface, with the ESP32-C3 cold-handshake workaround
    (connectNow=False + flush + connect + retry) — see reference_meshtastic_lib."""
    import meshtastic.serial_interface as si
    last = None
    for attempt in range(1, retries + 1):
        iface = None
        try:
            iface = si.SerialInterface(devPath=port, connectNow=False)
            try:
                iface.stream.reset_input_buffer()
            except Exception:
                pass
            iface.connect()
            return iface
        except Exception as e:  # noqa: BLE001
            last = e
            if iface is not None:        # release the port before retrying, else the
                try:                     # half-open handle keeps the exclusive lock and
                    iface.close()        # every later attempt fails with "Resource busy"
                except Exception:
                    pass
            print(f"  connect attempt {attempt}/{retries} failed: {e}", file=sys.stderr)
            time.sleep(1.5)
    raise SystemExit(f"could not open USB {port}: {last}")


def open_ble(addr):
    """BLE interface (bleak under the hood). addr = device name or BLE address."""
    import meshtastic.ble_interface as bi
    return bi.BLEInterface(addr)


def open_south(args):
    if args.iface == "ble":
        if not args.ble:
            raise SystemExit("--iface ble needs --ble <name|address>")
        print(f"connecting over BLE to {args.ble} …", file=sys.stderr)
        return open_ble(args.ble)
    if not args.port:
        raise SystemExit("--iface usb needs --port <devPath>")
    print(f"connecting over USB to {args.port} …", file=sys.stderr)
    return open_usb(args.port)


def _nid(num):
    """0x7d51bdc4 → '!7d51bdc4' (Meshtastic string node id)."""
    try:
        return f"!{int(num) & 0xffffffff:08x}"
    except (ValueError, TypeError):
        return None


def own_node_id(iface):
    """The connected node's own id ('!hex'), used to key its MQTT topics."""
    try:
        return _nid(iface.myInfo.my_node_num)
    except Exception:
        pass
    try:
        info = iface.getMyNodeInfo() or {}
        return _nid(info.get("num"))
    except Exception:
        return None


def own_channel_name(iface, fallback="LongFast"):
    """The connected node's primary channel name — used as the MQTT topic segment so
    the topic reflects the node's real channel. If the channel has an explicit name
    (custom channels usually do) use it; for the unnamed default channel, derive the
    name from the LoRa modem preset (LONG_FAST → 'LongFast'). Falls back on any error."""
    try:
        name = (iface.localNode.channels[0].settings.name or "").strip()
        if name:
            return name
    except Exception:
        pass
    try:
        from meshtastic import config_pb2
        preset = iface.localNode.localConfig.lora.modem_preset
        raw = config_pb2.Config.LoRaConfig.ModemPreset.Name(preset)   # e.g. LONG_FAST
        derived = "".join(w.capitalize() for w in raw.split("_"))
        return derived or fallback
    except Exception:
        return fallback


# ── the bridge: bidirectional mesh ⇄ MQTT ─────────────────────────────────────
class MeshMqttBridge:
    """Carry packets both ways between the connected node's mesh and an MQTT broker.
    Uplink → MQTT (nafco JSON for 256 + generic siliqs/rx/<G>/<pn>); downlink from
    siliqs/tx/<G> → sent out on the mesh. Run on a CLIENT_MUTE gateway node."""

    def __init__(self, iface, broker, broker_port=1883, channel=None, verbose=False,
                 web_port=None, username=None, password=None, tls=False,
                 ca_cert=None, client_cert=None, client_key=None):
        import threading
        import paho.mqtt.client as mqtt   # lazy: only the bridge needs paho
        self.iface = iface
        # channel = the topic segment. None/"" → read it from the node (auto).
        self.channel = channel.strip() if isinstance(channel, str) and channel.strip() \
            else own_channel_name(iface)
        self.verbose = verbose
        self.n_up = 0
        self.n_dn = 0
        self.broker = f"{broker}:{broker_port}"
        self.web_port = web_port
        self.node = own_node_id(iface) or "!00000000"
        self.rx_prefix = f"siliqs/rx/{self.node}"     # + /<portnum>
        self.tx_topic = f"siliqs/tx/{self.node}"
        # In-memory store for the optional telemetry view: latest per node + a stream.
        self.lock = threading.Lock()
        self.latest = {}
        self.events = deque(maxlen=200)
        self.cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.cli.on_message = self._on_mqtt_tx
        if username:                                  # external broker auth (optional)
            self.cli.username_pw_set(username, password or None)
        if tls or ca_cert or client_cert or client_key:
            kw = {}
            if ca_cert:
                kw["ca_certs"] = ca_cert              # verify the broker with this CA (e.g. AmazonRootCA1.pem)
            if client_cert:
                kw["certfile"] = client_cert          # client cert (mTLS, e.g. AWS IoT device cert)
            if client_key:
                kw["keyfile"] = client_key            # client private key
            self.cli.tls_set(**kw)                    # no kw → TLS with the system CA store
        self.cli.connect(broker, broker_port, 60)
        self.cli.subscribe(self.tx_topic, qos=0)      # downlink: MQTT → mesh
        self.cli.loop_start()
        pub.subscribe(self._on_mesh_rx, "meshtastic.receive.data")
        if web_port:
            self._start_web(web_port)

    def banner(self):
        print(f"mesh⇄MQTT bridge ready — node {self.node}  ⇄  {self.broker}")
        print(f"  channel (topic segment): {self.channel}   (auto-read from the node)")
        print(f"  uplink   256 → msh/2/json/{self.channel}/<sender>   "
              f"e.g. msh/2/json/{self.channel}/!7d51bdc4")
        print(f"  uplink   any → {self.rx_prefix}/<portnum>   e.g. {self.rx_prefix}/256")
        print(f"  downlink     ← {self.tx_topic}   {{to,portnum,data(b64)}}")
        if self.web_port:
            print(f"  telemetry view: http://0.0.0.0:{self.web_port}")
        print("  Ctrl-C to stop.")

    # uplink: mesh → MQTT
    def _on_mesh_rx(self, packet, interface=None):  # noqa: ARG002
        dec = packet.get("decoded") or {}
        pn = dec.get("portnum")
        payload = dec.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return
        payload = bytes(payload)
        frm = packet.get("from", 0)
        sender = packet.get("fromId") or _nid(frm)
        # normalise the portnum to an int where we can (named private ports come as a name)
        pn_int = MODBUS_PORTNUM if pn in (MODBUS_PORTNUM, str(MODBUS_PORTNUM), "PRIVATE_APP") \
            else (int(pn) if isinstance(pn, int) or (isinstance(pn, str) and pn.isdigit()) else pn)

        # generic uplink — every packet, keyed by this gateway node
        env = {
            "from": sender, "fromNum": frm,
            "to": packet.get("toId") or _nid(packet.get("to", 0)),
            "portnum": pn_int, "channel": packet.get("channel", 0),
            "rssi": packet.get("rxRssi"), "snr": packet.get("rxSnr"),
            "t": packet.get("rxTime", 0) or int(time.time()),
            "data": base64.b64encode(payload).decode(),
        }
        self.cli.publish(f"{self.rx_prefix}/{pn_int}", json.dumps(env))
        self.n_up += 1

        # 256 telemetry — also emit the nafco-compatible JSON + record for the web view
        if pn_int == MODBUS_PORTNUM:
            envelope = {
                "portnum": "PRIVATE_APP",
                "payload": {"raw": base64.b64encode(payload).decode()},
                "sender": sender, "from": frm,
                "channel": packet.get("channel", 0),
                "timestamp": packet.get("rxTime", 0),
            }
            self.cli.publish(f"msh/2/json/{self.channel}/{sender}", json.dumps(envelope))
            ev = {"t": time.time(), "node": sender, "len": len(payload), "hex": payload.hex()}
            with self.lock:
                self.latest[sender] = ev
                self.events.append(ev)
        if self.verbose:
            print(f"  [up {self.n_up}] {sender} pn={pn_int} {len(payload)}B "
                  f"-> {self.rx_prefix}/{pn_int}", file=sys.stderr)

    # downlink: MQTT → mesh
    def _on_mqtt_tx(self, client, userdata, msg):  # noqa: ARG002
        try:
            m = json.loads(msg.payload.decode())
            data = base64.b64decode(m["data"])
            dest = m.get("to") or "^all"
            port = int(m.get("portnum", SERIAL_PORTNUM))
            self.iface.sendData(data, destinationId=dest, portNum=port,
                                wantAck=bool(m.get("wantAck", False)),
                                channelIndex=int(m.get("channelIndex", 0)))
            self.n_dn += 1
            if self.verbose:
                print(f"  [dn {self.n_dn}] {self.tx_topic} -> mesh {dest} "
                      f"pn={port} {len(data)}B", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            print(f"  downlink drop: {e}", file=sys.stderr)

    def _start_web(self, port):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        store = self

        class WebH(BaseHTTPRequestHandler):
            def _send(self, code, body, ctype):
                b = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
                self.send_response(code); self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(b))); self.end_headers()
                self.wfile.write(b)

            def do_GET(self):
                if self.path == "/" or self.path.startswith("/?"):
                    self._send(200, TELEM_PAGE.encode(), "text/html; charset=utf-8")
                elif self.path == "/api/telemetry":
                    with store.lock:
                        data = {"count": store.n_up, "broker": store.broker,
                                "node": store.node,
                                "latest": list(store.latest.values()),
                                "events": list(store.events)[-120:]}
                    self._send(200, data, "application/json")
                else:
                    self._send(404, {"error": "not found"}, "application/json")

            def log_message(self, *a):
                pass

        srv = ThreadingHTTPServer(("0.0.0.0", port), WebH)
        threading.Thread(target=srv.serve_forever, daemon=True).start()

    def run(self):
        self.banner()
        try:
            while True:
                time.sleep(1)
        finally:
            self.cli.loop_stop()
            self.cli.disconnect()


# Backwards-compatible alias (older imports / the control panel referenced MqttHandler).
MqttHandler = MeshMqttBridge


TELEM_PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>siliqs-mesh-bridge · telemetry</title><style>
 :root{--bg:#14151a;--panel:#1d1f27;--p2:#23262f;--bd:#2e3140;--tx:#e7e9ee;--mut:#9aa0ad;--ac:#67ea94}
 *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.5 system-ui,-apple-system,sans-serif}
 header{display:flex;align-items:center;gap:12px;padding:14px 20px;background:var(--panel);border-bottom:1px solid var(--bd)}
 header b{font-size:16px}.dot{width:10px;height:10px;border-radius:50%;background:var(--ac);box-shadow:0 0 8px var(--ac)}
 main{max-width:920px;margin:0 auto;padding:20px}.card{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:16px 18px;margin-bottom:18px}
 h2{margin:0 0 10px;font-size:14px}.note{font-size:12px;color:var(--mut)}
 table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--bd);vertical-align:top}
 th{color:var(--mut);font-weight:500;font-size:11px}.mono{font-family:ui-monospace,monospace;font-size:12px;word-break:break-all}
 .node{color:var(--ac);font-family:ui-monospace,monospace}.evs{max-height:340px;overflow:auto}
</style></head><body>
<header><span class="dot"></span><b>siliqs-mesh-bridge</b><span class="note">telemetry</span>
 <span style="flex:1"></span><span id="meta" class="note"></span></header>
<main>
 <div class="card"><h2>Latest by node</h2>
  <table><thead><tr><th>Node</th><th>Last heard</th><th>Bytes</th><th>Payload (raw hex)</th></tr></thead>
  <tbody id="latest"><tr><td colspan="4" class="note">waiting for telemetry…</td></tr></tbody></table></div>
 <div class="card"><h2>Event stream <span class="note">newest first</span></h2>
  <div class="evs"><table><tbody id="events"></tbody></table></div></div>
</main>
<script>
const $=id=>document.getElementById(id);
const ago=t=>{const s=Math.max(0,Math.round(Date.now()/1000-t));return s<60?s+'s ago':Math.round(s/60)+'m ago'};
const hx=h=>h.replace(/(..)/g,'$1 ').trim();
async function poll(){
 try{const d=await (await fetch('/api/telemetry')).json();
  $('meta').textContent=`${d.count} frame(s) · node ${d.node||''} · broker ${d.broker}`;
  const L=d.latest.sort((a,b)=>b.t-a.t);
  $('latest').innerHTML=L.length?L.map(e=>`<tr><td class="node">${e.node}</td><td class="note">${ago(e.t)}</td><td>${e.len}</td><td class="mono">${hx(e.hex)}</td></tr>`).join(''):'<tr><td colspan="4" class="note">waiting for telemetry…</td></tr>';
  const E=d.events.slice().reverse();
  $('events').innerHTML=E.map(e=>`<tr><td class="note" style="white-space:nowrap">${new Date(e.t*1000).toLocaleTimeString()}</td><td class="node">${e.node}</td><td class="mono">${hx(e.hex)}</td></tr>`).join('');
 }catch(e){}
}
poll();setInterval(poll,2000);
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser(description="Siliqs Gateway — a bidirectional mesh ⇄ MQTT bridge.")
    ap.add_argument("--iface", choices=["usb", "ble"], default="usb", help="south-side transport")
    ap.add_argument("--port", help="USB devPath, e.g. /dev/ttyACM0 or /dev/cu.usbmodemXXX")
    ap.add_argument("--ble", help="BLE device name or address")
    ap.add_argument("--broker", default="127.0.0.1", help="MQTT broker host")
    ap.add_argument("--broker-port", type=int, default=1883, help="MQTT broker port")
    ap.add_argument("--channel", default=None,
                    help="MQTT topic segment; omit to auto-read the node's primary channel name")
    ap.add_argument("--username", default=None, help="MQTT username (external broker auth)")
    ap.add_argument("--password", default=None,
                    help="MQTT password; prefer the env var SMB_MQTT_PASSWORD (argv is visible in ps)")
    ap.add_argument("--tls", action="store_true", help="connect to the broker over TLS")
    ap.add_argument("--ca-cert", default=None, help="CA cert file to verify the broker (e.g. AmazonRootCA1.pem)")
    ap.add_argument("--client-cert", default=None, help="client cert file for mTLS (e.g. AWS IoT device cert)")
    ap.add_argument("--client-key", default=None, help="client private-key file for mTLS")
    ap.add_argument("--web-port", type=int, default=None,
                    help="also serve a live telemetry view on this port (e.g. 9090)")
    ap.add_argument("--verbose", action="store_true", help="log every up/down packet")
    args = ap.parse_args()

    password = args.password or os.environ.get("SMB_MQTT_PASSWORD")   # env keeps it out of ps/logs
    iface = open_south(args)
    try:
        MeshMqttBridge(iface, args.broker, broker_port=args.broker_port,
                       channel=args.channel, verbose=args.verbose, web_port=args.web_port,
                       username=args.username, password=password, tls=args.tls,
                       ca_cert=args.ca_cert, client_cert=args.client_cert,
                       client_key=args.client_key).run()
    except KeyboardInterrupt:
        print("\nstopping.", file=sys.stderr)
    finally:
        try:
            iface.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
