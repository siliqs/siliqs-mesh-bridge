#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_mqtt_broker.py — a tiny, dependency-free MQTT broker built into the Gateway.

So a customer with **no MQTT broker of their own** can still finish the job: the Gateway
app runs this broker for them, and all the pieces (the mesh⇄MQTT bridge, the telemetry
view, a serial-over-MQTT client, and the customer's own dashboards / Node-RED / Grafana)
connect to it like any MQTT broker. If the customer *does* have a broker, point the
Gateway at theirs instead and don't start this one.

Scope: MQTT 3.1.1, **QoS 0** publish/subscribe with `+` / `#` wildcards — which is all the
Gateway needs (best-effort telemetry + serial). It handles CONNECT/SUBSCRIBE/PUBLISH/
PING/UNSUBSCRIBE/DISCONNECT; QoS 1 PUBLISH from a client is acked and delivered as QoS 0.
No TLS, no auth, no retained/will — keep the broker on a trusted LAN.

Pure stdlib (socket + threads), so it bundles into the PyInstaller app with no extra deps.
Standalone:  siliqs-mqtt-broker --host 0.0.0.0 --port 1883
"""
import argparse
import socket
import threading

# MQTT control packet types (high nibble of byte 1)
CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK = 1, 2, 3, 4, 8, 9
UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT = 10, 11, 12, 13, 14


def _encode_len(n):
    """MQTT 'remaining length' variable-byte encoding."""
    out = bytearray()
    while True:
        d = n % 128
        n //= 128
        if n > 0:
            d |= 0x80
        out.append(d)
        if n == 0:
            return bytes(out)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


def _read_packet(sock):
    """Return (ptype, flags, body) or None on EOF/error."""
    b1 = _recv_exact(sock, 1)
    if not b1:
        return None
    mult, val = 1, 0
    while True:
        eb = _recv_exact(sock, 1)
        if eb is None:
            return None
        byte = eb[0]
        val += (byte & 0x7F) * mult
        if (byte & 0x80) == 0:
            break
        mult *= 128
        if mult > 128 ** 4:
            return None
    body = _recv_exact(sock, val) if val else b""
    if val and body is None:
        return None
    return (b1[0] >> 4, b1[0] & 0x0F, body or b"")


def topic_matches(filt, topic):
    """MQTT topic filter match with '+' (one level) and '#' (rest, incl. parent)."""
    f, t = filt.split("/"), topic.split("/")
    i = 0
    while i < len(f):
        if f[i] == "#":
            return True
        if i >= len(t):
            return False
        if f[i] == "+" or f[i] == t[i]:
            i += 1
            continue
        return False
    return i == len(t)


class _Client:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.subs = set()
        self.slock = threading.Lock()

    def send(self, data):
        with self.slock:
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                return False


class MqttBroker:
    def __init__(self, host="0.0.0.0", port=1883, on_log=None):
        self.host = host
        self.port = port
        self.on_log = on_log or (lambda m: None)
        self.clients = set()
        self.lock = threading.Lock()
        self.srv = None
        self._stop = False

    # ---- lifecycle ----
    def start(self):
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind((self.host, self.port))
        self.srv.listen(64)
        threading.Thread(target=self._accept_loop, daemon=True).start()
        self.on_log(f"broker listening on {self.host}:{self.port}")
        return self

    def stop(self):
        self._stop = True
        try:
            self.srv.close()
        except Exception:
            pass
        with self.lock:
            for c in list(self.clients):
                try:
                    c.sock.close()
                except Exception:
                    pass
            self.clients.clear()

    def _accept_loop(self):
        while not self._stop:
            try:
                sock, addr = self.srv.accept()
            except OSError:
                return
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            c = _Client(sock, addr)
            with self.lock:
                self.clients.add(c)
            threading.Thread(target=self._client_loop, args=(c,), daemon=True).start()

    # ---- per-client ----
    def _client_loop(self, c):
        try:
            while not self._stop:
                pkt = _read_packet(c.sock)
                if pkt is None:
                    break
                ptype, flags, body = pkt
                if ptype == CONNECT:
                    c.send(bytes([CONNACK << 4, 2, 0, 0]))          # session-present 0, accepted
                elif ptype == PINGREQ:
                    c.send(bytes([PINGRESP << 4, 0]))
                elif ptype == SUBSCRIBE:
                    self._on_subscribe(c, body)
                elif ptype == UNSUBSCRIBE:
                    self._on_unsubscribe(c, body)
                elif ptype == PUBLISH:
                    self._on_publish(c, flags, body)
                elif ptype == DISCONNECT:
                    break
        finally:
            with self.lock:
                self.clients.discard(c)
            try:
                c.sock.close()
            except Exception:
                pass

    def _on_subscribe(self, c, body):
        pid = body[:2]
        i, codes = 2, []
        while i + 2 <= len(body):
            tl = (body[i] << 8) | body[i + 1]
            i += 2
            topic = body[i:i + tl].decode("utf-8", "replace")
            i += tl + 1                                            # skip the requested-QoS byte
            c.subs.add(topic)
            codes.append(0)                                        # granted QoS 0
        payload = pid + bytes(codes)
        c.send(bytes([SUBACK << 4]) + _encode_len(len(payload)) + payload)

    def _on_unsubscribe(self, c, body):
        pid = body[:2]
        i = 2
        while i + 2 <= len(body):
            tl = (body[i] << 8) | body[i + 1]
            i += 2
            c.subs.discard(body[i:i + tl].decode("utf-8", "replace"))
            i += tl
        c.send(bytes([UNSUBACK << 4]) + _encode_len(2) + pid)

    def _on_publish(self, c, flags, body):
        qos = (flags >> 1) & 0x03
        tl = (body[0] << 8) | body[1]
        i = 2
        topic = body[i:i + tl].decode("utf-8", "replace")
        i += tl
        if qos > 0:
            pid = body[i:i + 2]
            i += 2
            c.send(bytes([PUBACK << 4]) + _encode_len(2) + pid)    # ack QoS1 (we deliver as QoS0)
        payload = body[i:]
        self._fanout(topic, payload)

    def _fanout(self, topic, payload):
        tb = topic.encode("utf-8")
        frame = bytes([PUBLISH << 4]) + _encode_len(2 + len(tb) + len(payload)) \
            + bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
        with self.lock:
            targets = [c for c in self.clients if any(topic_matches(f, topic) for f in c.subs)]
        for c in targets:
            c.send(frame)


def main():
    ap = argparse.ArgumentParser(description="Tiny built-in MQTT broker for the Siliqs Gateway.")
    ap.add_argument("--host", default="0.0.0.0", help="bind address (0.0.0.0 = reachable on the LAN)")
    ap.add_argument("--port", type=int, default=1883)
    args = ap.parse_args()
    b = MqttBroker(args.host, args.port, on_log=print).start()
    print("  Ctrl-C to stop.")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        b.stop()
        print("\nstopped.")


if __name__ == "__main__":
    main()
