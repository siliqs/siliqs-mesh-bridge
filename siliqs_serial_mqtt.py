#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_serial_mqtt.py — serial-over-MQTT: a virtual serial cable that rides the broker.

This is a plain MQTT client. It opens a local PTY (a virtual serial port) and maps it to a
Siliqs Gateway's MQTT topics, so bytes typed into the serial port travel:

    your serial software ⇄ PTY ⇄ (this) ⇄ MQTT broker ⇄ Gateway (siliqs_mesh_bridge) ⇄ mesh ⇄ peer node

It replaces the old "serial pipe" that the Gateway used to do by injecting mesh packets
directly. Now the Gateway does ONE job (mesh ⇄ MQTT) and this rides the same broker, so it
can run on ANY host that can reach the broker — not only the one wired to the node.

You point it at:
  --gateway <G>   the node id ('!hex') of the Gateway you route through (its "your node");
                  the Gateway prints it, and its control panel shows it.
  --peer <P>      the remote node id ('!hex') you're talking to.
It publishes your keystrokes to  siliqs/tx/<G>   ({to:P, portnum:260, data}) and reads the
peer's replies from  siliqs/rx/<G>/260  (filtering from == P). PortNum 260 is a private,
unnamed Meshtastic port, so the Gateway's Modbus module ignores it.

For a two-host serial cable, run a Gateway + this on BOTH hosts, --peer crossed:
    host A (node A): siliqs_mesh_bridge --port …            # Gateway A
                     siliqs-serial-mqtt --gateway '!A' --peer '!B' --link /tmp/meshtty
    host B (node B): siliqs_mesh_bridge --port …            # Gateway B
                     siliqs-serial-mqtt --gateway '!B' --peer '!A' --link /tmp/meshtty
Then point your unmodified serial software at the printed /dev/pts/N (or --link path).

⚠ LoRa is best-effort / low-bandwidth — use for LOW-RATE, request-reply traffic (Modbus,
console), not high-throughput streams. Each packet carries ≤ ~233 bytes.

Needs `paho-mqtt` (pip install paho-mqtt).
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

SERIAL_PORTNUM = 260   # must match the Gateway / firmware serial port


def _norm(nid):
    """Accept '!7d51bdc4', '7d51bdc4' or an int → canonical '!7d51bdc4'."""
    if isinstance(nid, int):
        return f"!{nid & 0xffffffff:08x}"
    s = str(nid).strip()
    if s.startswith("!"):
        return s.lower()
    try:
        return f"!{int(s, 16) & 0xffffffff:08x}"
    except ValueError:
        return s.lower()


class SerialOverMqtt:
    def __init__(self, broker, broker_port, gateway, peer, link=None, mtu=200,
                 mode="line", coalesce_ms=50, want_ack=False, verbose=False,
                 username=None, password=None, tls=False,
                 ca_cert=None, client_cert=None, client_key=None):
        import paho.mqtt.client as mqtt
        self.gateway = _norm(gateway)
        self.peer = _norm(peer)
        self.mtu = max(1, min(mtu, 233))
        self.mode = mode
        self.coalesce = max(0.0, coalesce_ms / 1000.0)
        self.want_ack = want_ack
        self.verbose = verbose
        self.tx_topic = f"siliqs/tx/{self.gateway}"
        self.rx_topic = f"siliqs/rx/{self.gateway}/{SERIAL_PORTNUM}"
        self.broker = f"{broker}:{broker_port}"
        # A PTY: we hold the master; the user's software opens the slave (/dev/pts/N).
        self.master_fd, self.slave_fd = pty.openpty()
        tty.setraw(self.slave_fd)          # transparent: no echo / line-editing
        self.pts = os.ttyname(self.slave_fd)
        self.link = link
        if link:
            try:
                if os.path.islink(link) or os.path.exists(link):
                    os.remove(link)
                os.symlink(self.pts, link)
            except OSError as e:
                print(f"  (could not create symlink {link}: {e})", file=sys.stderr)
        self.cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.cli.on_message = self._on_mqtt
        if username:
            self.cli.username_pw_set(username, password or None)
        if tls or ca_cert or client_cert or client_key:
            kw = {}
            if ca_cert:
                kw["ca_certs"] = ca_cert
            if client_cert:
                kw["certfile"] = client_cert
            if client_key:
                kw["keyfile"] = client_key
            self.cli.tls_set(**kw)
        self.cli.connect(broker, broker_port, 60)
        self.cli.subscribe(self.rx_topic, qos=0)
        self.cli.loop_start()

    def banner(self):
        where = f"{self.link}  →  {self.pts}" if self.link else self.pts
        frame = ("line — one packet per line (Enter), ≤%d B" % self.mtu if self.mode == "line"
                 else "stream — batch by %d ms idle / ≤%d B" % (int(self.coalesce * 1000), self.mtu))
        print(f"serial-over-MQTT ready:  open  {where}")
        print(f"  gateway {self.gateway}  ⇄  peer {self.peer}   via broker {self.broker}")
        print(f"  tx → {self.tx_topic}   rx ← {self.rx_topic}")
        print(f"  framing: {frame}")
        print("  point your serial software at the path above. Ctrl-C to stop.")

    # broker → PTY (peer's replies)
    def _on_mqtt(self, client, userdata, msg):  # noqa: ARG002
        try:
            m = json.loads(msg.payload.decode())
        except Exception:
            return
        src = _norm(m.get("from") or m.get("fromNum") or "")
        if self.peer not in (None, "!ffffffff") and src != self.peer:
            return                          # only this peer's traffic
        data = m.get("data")
        if not data:
            return
        try:
            os.write(self.master_fd, base64.b64decode(data))
        except OSError:
            pass

    # PTY → broker (your keystrokes)
    def _flush(self, data):
        if not data:
            return
        env = {"to": self.peer, "portnum": SERIAL_PORTNUM,
               "data": base64.b64encode(bytes(data)).decode(),
               "wantAck": self.want_ack}
        self.cli.publish(self.tx_topic, json.dumps(env))
        if self.verbose:
            print(f"  [tx] {len(data)}B PTY -> {self.tx_topic} (to {self.peer})", file=sys.stderr)

    @staticmethod
    def _line_end(buf):
        """Index just past the first line terminator, or -1. Enter is CR from a terminal
        (screen/minicom) but LF from printf/software — accept either; treat CR+LF/LF+CR as one."""
        cr, lf = buf.find(b"\r"), buf.find(b"\n")
        cands = [x for x in (cr, lf) if x != -1]
        if not cands:
            return -1
        i = min(cands)
        end = i + 1
        nxt = buf[end:end + 1]
        if nxt in (b"\r", b"\n") and nxt != buf[i:i + 1]:
            end += 1
        return end

    def run(self):
        self.banner()
        buf = bytearray()
        last = 0.0
        try:
            while True:
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
                if buf and self.mode == "stream" and (now - last) >= self.coalesce:
                    self._flush(buf); buf = bytearray()
        finally:
            self.cli.loop_stop()
            self.cli.disconnect()


def main():
    ap = argparse.ArgumentParser(description="serial-over-MQTT — a virtual serial cable over a Siliqs Gateway broker.")
    ap.add_argument("--broker", default="127.0.0.1", help="MQTT broker host")
    ap.add_argument("--broker-port", type=int, default=1883, help="MQTT broker port")
    ap.add_argument("--gateway", required=True, help="Gateway node id you route through, e.g. '!81b8aaf8'")
    ap.add_argument("--peer", required=True, help="remote node id, e.g. '!7d51bdc4'")
    ap.add_argument("--link", help="symlink the PTY to this path (e.g. /tmp/meshtty)")
    ap.add_argument("--mode", choices=["line", "stream"], default="line",
                    help="line (one packet per Enter) or stream (idle-batched binary)")
    ap.add_argument("--mtu", type=int, default=200, help="max bytes per packet / line (≤233)")
    ap.add_argument("--coalesce-ms", type=int, default=50, help="stream mode: idle gap (ms) that ends a batch")
    ap.add_argument("--want-ack", action="store_true", help="reliable (slower) delivery")
    ap.add_argument("--username", default=None, help="MQTT username (external broker auth)")
    ap.add_argument("--password", default=None,
                    help="MQTT password; prefer env SMB_MQTT_PASSWORD (argv is visible in ps)")
    ap.add_argument("--tls", action="store_true", help="connect to the broker over TLS")
    ap.add_argument("--ca-cert", default=None, help="CA cert file to verify the broker")
    ap.add_argument("--client-cert", default=None, help="client cert file for mTLS")
    ap.add_argument("--client-key", default=None, help="client private-key file for mTLS")
    ap.add_argument("--verbose", action="store_true", help="log every tx/rx")
    args = ap.parse_args()
    password = args.password or os.environ.get("SMB_MQTT_PASSWORD")
    try:
        SerialOverMqtt(args.broker, args.broker_port, args.gateway, args.peer,
                       username=args.username, password=password, tls=args.tls,
                       ca_cert=args.ca_cert, client_cert=args.client_cert, client_key=args.client_key,
                       link=args.link, mtu=args.mtu, mode=args.mode,
                       coalesce_ms=args.coalesce_ms, want_ack=args.want_ack,
                       verbose=args.verbose).run()
    except KeyboardInterrupt:
        print("\nstopping.", file=sys.stderr)


if __name__ == "__main__":
    main()
