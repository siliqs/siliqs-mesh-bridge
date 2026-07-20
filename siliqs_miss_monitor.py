#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Siliqs / Guinea Technology Corporation
"""
siliqs_miss_monitor.py — per-node packet-miss monitor for the Siliqs mesh⇄MQTT gateway.

It subscribes to the gateway's uplink telemetry on your MQTT broker and, for every
sensor node, estimates how many reports it MISSED.

How "missed" is decided
------------------------
The mesh payload carries no sequence number, so misses are inferred from cadence:
each sensor reports on a fixed timer (≈ every INTERVAL seconds). When a packet
arrives after a gap much larger than one interval, the whole-intervals inside that
gap are counted as missed. A node that has been silent for longer than the overdue
threshold is flagged DOWN. Learn the interval automatically, or pin it with --interval.

Topic
-----
The gateway publishes every uplink to  siliqs/rx/<gateway-node>/<portnum>  as JSON:
  {"from":"!81b94800","portnum":256,"rssi":-98,"snr":6.5,"t":..., "data":"<b64>"}
We subscribe to  siliqs/rx/#  and key on the envelope's "from" (the real sensor node).

Usage
-----
  siliqs_miss_monitor.py                         # 127.0.0.1:1883, auto-learn interval
  siliqs_miss_monitor.py --host 192.168.0.140    # broker on the LAN
  siliqs_miss_monitor.py --interval 85           # pin the expected report interval (s)
  siliqs_miss_monitor.py --all                   # count every portnum, not just 256
  siliqs_miss_monitor.py --csv misses.csv        # also append every miss event to CSV
"""
import argparse
import collections
import json
import os
import statistics
import sys
import time

import paho.mqtt.client as mqtt

MODBUS_PORTNUM = 256


def fmt_ago(sec):
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60:02d}s"
    return f"{sec // 3600}h{(sec % 3600) // 60:02d}m"


class NodeStat:
    def __init__(self, fixed_interval):
        self.first = None
        self.last = None
        self.recv = 0
        self.missed = 0
        self.rssi = None
        self.snr = None
        self.gaps = collections.deque(maxlen=40)   # recent inter-arrival gaps (s)
        self.fixed = fixed_interval

    def interval(self):
        if self.fixed:
            return self.fixed
        # robust auto-estimate: median of gaps, ignoring ones that look like a
        # multi-interval gap (a miss) or a relay duplicate.
        good = [g for g in self.gaps if g > 1.0]
        if len(good) < 3:
            return None
        med = statistics.median(good)
        trimmed = [g for g in good if 0.5 * med <= g <= 1.6 * med] or good
        return statistics.median(trimmed)


class Monitor:
    def __init__(self, args):
        self.args = args
        self.nodes = {}                       # node_id -> NodeStat
        self.events = collections.deque(maxlen=12)
        self.csv = None
        if args.csv:
            new = not os.path.exists(args.csv)
            self.csv = open(args.csv, "a", buffering=1)
            if new:
                self.csv.write("iso,epoch,node,gap_s,interval_s,missed\n")
        self.started = time.time()

    def on_connect(self, cli, userdata=None, flags=None, reason_code=None, properties=None):
        cli.subscribe("siliqs/rx/#", qos=0)
        self.log_event(f"subscribed siliqs/rx/#  on {self.args.host}:{self.args.port}")

    def on_message(self, cli, userdata, msg):
        try:
            env = json.loads(msg.payload.decode())
        except Exception:
            return
        node = env.get("from")
        if not node:
            return
        pn = env.get("portnum")
        if not self.args.all and pn != MODBUS_PORTNUM:
            return
        now = time.time()
        st = self.nodes.get(node)
        if st is None:
            st = self.nodes[node] = NodeStat(self.args.interval)
            st.first = now
        # rssi/snr for the table
        if env.get("rssi") is not None:
            st.rssi = env["rssi"]
        if env.get("snr") is not None:
            st.snr = env["snr"]

        iv = st.interval()
        if st.last is not None:
            gap = now - st.last
            dedup = max(self.args.dedup, 0.25 * iv) if iv else self.args.dedup
            if iv and gap < dedup:
                return                         # relay duplicate / retry — ignore
            st.gaps.append(gap)
            if iv and gap > self.args.miss_factor * iv:
                n_missed = max(1, round(gap / iv) - 1)
                st.missed += n_missed
                self.record_miss(node, gap, iv, n_missed)
        else:
            st.gaps.append(0)
        st.last = now
        st.recv += 1

    def record_miss(self, node, gap, iv, n):
        self.log_event(f"MISS  {node}  gap {fmt_ago(gap)} ≈ {n}×{iv:.0f}s  (+{n})")
        if self.csv:
            iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
            self.csv.write(f"{iso},{time.time():.0f},{node},{gap:.0f},{iv:.0f},{n}\n")

    def log_event(self, txt):
        self.events.append((time.strftime("%H:%M:%S"), txt))

    def render(self):
        now = time.time()
        out = ["\033[2J\033[H"]
        out.append("\033[1mSiliqs 節點漏包監測 · packet-miss monitor\033[0m")
        out.append(f"broker {self.args.host}:{self.args.port} · topic siliqs/rx/# · "
                   f"{'all portnums' if self.args.all else 'portnum 256'} · "
                   f"uptime {fmt_ago(now - self.started)}")
        out.append("")
        hdr = f"{'NODE':<13}{'LAST':>9}{'INTERVAL':>10}{'RECV':>7}{'MISS':>6}{'LOSS%':>8}{'RSSI':>6}{'SNR':>6}  STATUS"
        out.append("\033[1m" + hdr + "\033[0m")
        for node in sorted(self.nodes):
            st = self.nodes[node]
            iv = st.interval()
            ago = now - st.last if st.last else 0
            # overdue intervals not yet "closed" by an arrival — show as pending
            pending = 0
            status, color = "OK", "\033[32m"
            if iv:
                if ago > self.args.overdue * iv:
                    pending = max(0, round(ago / iv) - 1)
                    status, color = f"DOWN (+{pending}?)", "\033[31m"
                elif ago > self.args.miss_factor * iv:
                    status, color = "LATE", "\033[33m"
            total = st.recv + st.missed
            loss = (st.missed / total * 100) if total else 0.0
            iv_s = f"~{iv:.0f}s" if iv else "learning"
            rssi = f"{st.rssi:.0f}" if st.rssi is not None else "-"
            snr = f"{st.snr:.1f}" if st.snr is not None else "-"
            row = (f"{node:<13}{fmt_ago(ago):>9}{iv_s:>10}{st.recv:>7}"
                   f"{st.missed:>6}{loss:>7.1f}%{rssi:>6}{snr:>6}  ")
            out.append(color + row + status + "\033[0m")
        if not self.nodes:
            out.append("  (waiting for telemetry… make sure the gateway is running & forwarding)")
        out.append("")
        out.append("\033[2m最近事件 / recent events:\033[0m")
        for ts, txt in list(self.events)[-8:]:
            out.append(f"  \033[2m{ts}\033[0m  {txt}")
        out.append("")
        out.append("\033[2mCtrl-C 離開 · LOSS%=missed/(recv+missed) · DOWN(+n?)=已靜默 n 個週期未回\033[0m")
        sys.stdout.write("\n".join(out) + "\n")
        sys.stdout.flush()

    def run(self):
        try:                                   # paho-mqtt 2.x wants an explicit API version
            cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:                 # paho-mqtt 1.x
            cli = mqtt.Client()
        cli.on_connect = self.on_connect
        cli.on_message = self.on_message
        if self.args.username:
            cli.username_pw_set(self.args.username, self.args.password or "")
        cli.connect(self.args.host, self.args.port, keepalive=30)
        cli.loop_start()
        try:
            while True:
                self.render()
                time.sleep(self.args.refresh)
        except KeyboardInterrupt:
            print("\nbye.")
        finally:
            cli.loop_stop()
            if self.csv:
                self.csv.close()


def main():
    ap = argparse.ArgumentParser(description="Per-node packet-miss monitor for the Siliqs gateway.")
    ap.add_argument("--host", default="127.0.0.1", help="MQTT broker host (default 127.0.0.1)")
    ap.add_argument("--port", type=int, default=1883, help="MQTT broker port (default 1883)")
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=os.environ.get("SMB_MQTT_PASSWORD"))
    ap.add_argument("--interval", type=float, default=None,
                    help="expected report interval in seconds (default: auto-learn per node)")
    ap.add_argument("--miss-factor", type=float, default=1.5, dest="miss_factor",
                    help="a gap beyond this × interval starts counting misses (default 1.5)")
    ap.add_argument("--overdue", type=float, default=2.5,
                    help="silent for this × interval → flag node DOWN (default 2.5)")
    ap.add_argument("--dedup", type=float, default=3.0,
                    help="ignore repeats closer than this many seconds (relay copies)")
    ap.add_argument("--all", action="store_true", help="count every portnum, not just 256")
    ap.add_argument("--refresh", type=float, default=1.0, help="screen refresh seconds")
    ap.add_argument("--csv", default=None, help="also append every miss event to this CSV file")
    args = ap.parse_args()
    Monitor(args).run()


if __name__ == "__main__":
    main()
