#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_mesh_bridge.py — host-side Meshtastic bridge / gateway app.

ONE south-side connection to a Meshtastic node (USB or BLE) + a pluggable NORTH
handler. Conceptually a "gateway bridge": it adapts the mesh radio access point to
host-side services. The node runs the NORMAL firmware — no firmware change, no
custom BLE service, no console takeover.

Handlers (north):
  serial   — expose a virtual serial port (PTY) and pipe it transparently to a
             PEER node's host over the mesh: a wireless serial cable. Works
             USB↔USB, BLE↔BLE or mixed (both ends run this with --handler serial,
             each pointing --peer at the other). Uses PortNum 260 (private, unnamed, != the
             ModbusModule's 256) so Meshtastic just routes it node-to-node and our
             Modbus module ignores it.
  mqtt     — gateway: forward received PortNum-256 (Modbus telemetry) packets to an
             MQTT broker, in the nafco-compatible JSON the cloud decoder consumes
             (msh/2/json/<ch>/<node>, {portnum,payload:{raw:b64},sender,...}). Run it
             on a gateway node (role CLIENT_MUTE). Needs `paho-mqtt`.

⚠ LoRa is best-effort and low-bandwidth (BW500/SF9 ≈ a few hundred B/s, high
latency). The serial pipe suits LOW-RATE / request-reply traffic (e.g. Modbus over
serial), NOT high-throughput ordered streams. Each packet carries ≤ ~233 bytes.

Examples:
  # USB↔USB serial cable — run on BOTH hosts, peers crossed:
  #   host A (node !81b8aaf8) ── mesh ── host B (node !7d51bdc4)
  python siliqs_mesh_bridge.py --iface usb --port /dev/ttyACM0 \
      --handler serial --peer '!7d51bdc4' --link /tmp/meshtty
  #   (on host B: --port /dev/ttyACM0 --peer '!81b8aaf8' ...)

  # BLE instead of USB (RPi/Linux host with no RS485):
  python siliqs_mesh_bridge.py --iface ble --ble 'SQC485I' \
      --handler serial --peer '!7d51bdc4' --link /tmp/meshtty

Then point your unmodified serial software at the printed /dev/pts/N (or --link).

Needs the `meshtastic` Python package (in this folder's .venv); BLE also needs
`bleak` (pulled in by meshtastic's BLE support).
"""
import argparse
import base64
import json
import os
import pty
import select
import sys
import time
import tty

from pubsub import pub

# Serial-pipe PortNum: an UNNAMED value in Meshtastic's private range (256–510) so
# it collides with NO Meshtastic service (256=our Modbus module, 257=ATAK_FORWARDER,
# 511=MAX are all taken). Unnamed portnums are still received fine — the lib reports
# them as the raw int (named ones come through as their name string).
SERIAL_PORTNUM = 260
MODBUS_PORTNUM = 256   # PRIVATE_APP — where the SQC485I Modbus telemetry arrives


# ── south side: open the node connection ──────────────────────────────────────
def open_usb(port, retries=5):
    """Serial (USB CDC) interface, with the ESP32-C3 cold-handshake workaround
    (connectNow=False + flush + connect + retry) — see reference_meshtastic_lib."""
    import meshtastic.serial_interface as si
    last = None
    for attempt in range(1, retries + 1):
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


# ── north handler: transparent serial pipe over the mesh ──────────────────────
class SerialHandler:
    """A virtual serial port (PTY) bridged to a peer node's host over the mesh."""

    def __init__(self, iface, peer, link=None, mtu=200, want_ack=False, verbose=False,
                 mode="line", coalesce_ms=50):
        self.iface = iface
        self.peer = peer            # '!hex' or node-num int (destination)
        self.mtu = max(1, min(mtu, 233))
        self.want_ack = want_ack
        self.verbose = verbose
        # Framing — LoRa is far slower than USB, so we never stream byte-by-byte:
        #   line   : buffer until a line ending (Enter, '\n') or mtu bytes, then send the
        #            whole line as ONE packet (keeps the line ending). Human/text/console.
        #   stream : binary — no line endings, so batch by an idle gap or mtu instead.
        self.mode = mode
        self.coalesce = max(0.0, coalesce_ms / 1000.0)   # stream-mode idle gap
        # A PTY: we hold the master; the user's software opens the slave (/dev/pts/N).
        self.master_fd, self.slave_fd = pty.openpty()
        tty.setraw(self.slave_fd)   # transparent: no echo / line-editing
        self.pts = os.ttyname(self.slave_fd)
        self.link = link
        if link:
            try:
                if os.path.islink(link) or os.path.exists(link):
                    os.remove(link)
                os.symlink(self.pts, link)
            except OSError as e:
                print(f"  (could not create symlink {link}: {e})", file=sys.stderr)
        pub.subscribe(self._on_receive, "meshtastic.receive.data")

    def banner(self):
        where = f"{self.link}  →  {self.pts}" if self.link else self.pts
        frame = ("line — one packet per line (Enter), ≤%d B" % self.mtu if self.mode == "line"
                 else "stream — batch by %d ms idle / ≤%d B" % (int(self.coalesce * 1000), self.mtu))
        print(f"serial pipe ready:  open  {where}")
        print(f"  ↔ peer {self.peer} over the mesh (PortNum {SERIAL_PORTNUM})")
        print(f"  framing: {frame}")
        print("  point your serial software at the path above. Ctrl-C to stop.")

    def _on_receive(self, packet, interface=None):  # noqa: ARG002
        dec = packet.get("decoded") or {}
        if self.verbose:
            print(f"  [rx] portnum={dec.get('portnum')} from={packet.get('fromId')} "
                  f"len={len(dec.get('payload') or b'')}", file=sys.stderr)
        # Unnamed portnums arrive as the int; accept the str form too, just in case.
        pn = dec.get("portnum")
        if pn not in (SERIAL_PORTNUM, str(SERIAL_PORTNUM)):
            return
        # Only accept data from our configured peer (ignore unrelated traffic).
        src = packet.get("fromId") or packet.get("from")
        if self.peer not in (None, "^all") and src not in (self.peer, _num(self.peer)):
            return
        payload = dec.get("payload")
        if payload:
            try:
                os.write(self.master_fd, payload)
            except OSError:
                pass

    def _flush(self, data):
        if not data:
            return
        if self.verbose:
            print(f"  [tx] {len(data)}B from PTY -> {self.peer}", file=sys.stderr)
        try:
            self.iface.sendData(bytes(data), destinationId=self.peer,
                                portNum=SERIAL_PORTNUM, wantAck=self.want_ack)
        except Exception as e:  # noqa: BLE001
            print(f"  send failed ({len(data)} B): {e}", file=sys.stderr)

    @staticmethod
    def _line_end(buf):
        """Index just past the first line terminator, or -1. Enter is CR ('\\r') from a
        terminal (screen/minicom) but LF ('\\n') from printf/software — accept either,
        and treat a CR+LF / LF+CR pair as one terminator."""
        cr, lf = buf.find(b"\r"), buf.find(b"\n")
        cands = [x for x in (cr, lf) if x != -1]
        if not cands:
            return -1
        i = min(cands)
        end = i + 1
        nxt = buf[end:end + 1]
        if nxt in (b"\r", b"\n") and nxt != buf[i:i + 1]:
            end += 1                       # swallow the paired CR/LF
        return end

    def run(self):
        self.banner()
        buf = bytearray()
        last = 0.0
        while True:
            # line mode: just wait for data (we flush on a line ending / mtu, never on a
            # timer, so nothing is sent before Enter). stream mode: wait out the idle gap.
            wait = 0.5
            if buf and self.mode == "stream":
                wait = max(0.0, self.coalesce - (time.monotonic() - last))
            r, _, _ = select.select([self.master_fd], [], [], wait)
            now = time.monotonic()
            if r:
                try:
                    chunk = os.read(self.master_fd, 4096)
                except OSError:
                    chunk = b""
                if chunk:
                    buf += chunk
                    last = now
                    if self.mode == "line":
                        # one packet per line (through its terminator); a line longer
                        # than mtu is split at mtu.
                        while True:
                            end = self._line_end(buf)
                            if end != -1 and end <= self.mtu:
                                self._flush(buf[:end]); del buf[:end]
                            elif len(buf) >= self.mtu:
                                self._flush(buf[:self.mtu]); del buf[:self.mtu]
                            else:
                                break
                    elif len(buf) >= self.mtu:
                        self._flush(buf); buf = bytearray()
                    continue
            # stream mode only: flush the batch once the idle gap elapses.
            if buf and self.mode == "stream" and (now - last) >= self.coalesce:
                self._flush(buf); buf = bytearray()


# ── north handler: forward Modbus telemetry to MQTT (gateway) ─────────────────
class MqttHandler:
    """Publish received PortNum-256 (Modbus telemetry) packets to an MQTT broker, in
    the nafco-compatible JSON the cloud decoder consumes — so the existing MQTT →
    decoder → InfluxDB pipeline is reusable unchanged. Run on a CLIENT_MUTE gateway."""

    def __init__(self, iface, broker, broker_port=1883, channel="LongFast", verbose=False):
        import paho.mqtt.client as mqtt   # lazy: only the mqtt handler needs paho
        self.iface = iface
        self.channel = channel
        self.verbose = verbose
        self.n = 0
        self.broker = f"{broker}:{broker_port}"
        self.cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.cli.connect(broker, broker_port, 60)
        self.cli.loop_start()
        pub.subscribe(self._on_receive, "meshtastic.receive.data")

    def banner(self):
        print(f"mqtt gateway ready:  PortNum {MODBUS_PORTNUM} -> {self.broker}")
        print(f"  topic: msh/2/json/{self.channel}/<node>   Ctrl-C to stop.")

    def _on_receive(self, packet, interface=None):  # noqa: ARG002
        dec = packet.get("decoded") or {}
        # 256 is named PRIVATE_APP, so the lib reports it as the name string.
        if dec.get("portnum") not in (MODBUS_PORTNUM, str(MODBUS_PORTNUM), "PRIVATE_APP"):
            return
        payload = dec.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return
        payload = bytes(payload)
        frm = packet.get("from", 0)
        sender = packet.get("fromId") or f"!{frm:08x}"
        envelope = {
            "portnum": "PRIVATE_APP",
            "payload": {"raw": base64.b64encode(payload).decode()},
            "sender": sender, "from": frm,
            "channel": packet.get("channel", 0),
            "timestamp": packet.get("rxTime", 0),
        }
        topic = f"msh/2/json/{self.channel}/{sender}"
        self.cli.publish(topic, json.dumps(envelope))
        self.n += 1
        if self.verbose:
            print(f"  [mqtt {self.n}] {sender} -> {topic}  {len(payload)}B {payload.hex()}",
                  file=sys.stderr)

    def run(self):
        self.banner()
        try:
            while True:
                time.sleep(1)
        finally:
            self.cli.loop_stop()
            self.cli.disconnect()


def _num(peer):
    """'!7d51bdc4' → 0x7d51bdc4 int, for matching packet['from']."""
    try:
        return int(peer[1:], 16) if isinstance(peer, str) and peer.startswith("!") else int(peer)
    except (ValueError, TypeError):
        return None


def main():
    ap = argparse.ArgumentParser(description="Host-side Meshtastic bridge (serial pipe, …).")
    ap.add_argument("--iface", choices=["usb", "ble"], default="usb", help="south-side transport")
    ap.add_argument("--port", help="USB devPath, e.g. /dev/ttyACM0 or /dev/cu.usbmodemXXX")
    ap.add_argument("--ble", help="BLE device name or address")
    ap.add_argument("--handler", choices=["serial", "mqtt"], default="serial",
                    help="north-side handler")
    ap.add_argument("--peer", help="serial: peer node for the pipe, e.g. '!7d51bdc4'")
    ap.add_argument("--link", help="serial: symlink the PTY to this path (e.g. /tmp/meshtty)")
    ap.add_argument("--mode", choices=["line", "stream"], default="line",
                    help="serial: line (one packet per Enter) or stream (idle-batched binary)")
    ap.add_argument("--mtu", type=int, default=200, help="serial: max bytes per packet / line (≤233)")
    ap.add_argument("--coalesce-ms", type=int, default=50,
                    help="serial stream mode: idle gap (ms) that ends a batch")
    ap.add_argument("--want-ack", action="store_true", help="serial: reliable (slower) delivery")
    # mqtt handler
    ap.add_argument("--broker", default="127.0.0.1", help="mqtt: broker host")
    ap.add_argument("--broker-port", type=int, default=1883, help="mqtt: broker port")
    ap.add_argument("--channel", default="LongFast", help="mqtt: topic channel segment")
    ap.add_argument("--verbose", action="store_true", help="log every tx/rx for debugging")
    args = ap.parse_args()

    iface = open_south(args)
    try:
        if args.handler == "serial":
            if not args.peer:
                raise SystemExit("--handler serial needs --peer <node>")
            SerialHandler(iface, args.peer, link=args.link, mtu=args.mtu,
                          want_ack=args.want_ack, verbose=args.verbose,
                          mode=args.mode, coalesce_ms=args.coalesce_ms).run()
        elif args.handler == "mqtt":
            MqttHandler(iface, args.broker, broker_port=args.broker_port,
                        channel=args.channel, verbose=args.verbose).run()
    except KeyboardInterrupt:
        print("\nstopping.", file=sys.stderr)
    finally:
        try:
            iface.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
