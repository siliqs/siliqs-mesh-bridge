# siliqs-mesh-bridge

A **host-side bridge** that turns a Meshtastic node (over **USB or BLE**) into a
**transparent serial port that tunnels to another node's host over the LoRa mesh** —
a wireless serial cable. The node runs **stock Meshtastic firmware**: no firmware
change, no custom BLE service, no console takeover. Meshtastic's own routing carries
the data node-to-node.

Built for the Siliqs **SQC485I** (ESP32-C3 + SX1262) but works with any Meshtastic
node. Useful when a host (RPi / Linux / Linxdot-OpenWrt / PC) isn't wired to RS485
but needs to reach a remote serial/RS485 device over LoRa.

> ⚠️ **LoRa is slow and best-effort.** This is for **low-rate, request/reply** traffic
> (Modbus over serial, console, occasional messages) — **not** high-throughput or
> latency-sensitive streams.

## Install

```sh
# pip (in a venv) — Linux / RPi / Mac
python3 -m venv venv && venv/bin/pip install .
venv/bin/siliqs-mesh-bridge --help

# or run the single file directly (just needs `meshtastic`)
pip install meshtastic && python siliqs_mesh_bridge.py --help

# Docker (good for Linxdot / OpenWrt — see Dockerfile)
docker build -t siliqs-mesh-bridge .
```
BLE (`--iface ble`) additionally needs `bleak` (pulled in by `meshtastic`) and host
Bluetooth; on Linux it just works, in Docker it needs dbus/Bluetooth passthrough.

## Use — a serial cable between two hosts

Run it on **both** hosts, each pointing `--peer` at the *other* node:

```sh
# host A, whose node is !81b8aaf8  ── LoRa ──  host B, whose node is !7d51bdc4
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 \
    --handler serial --peer '!7d51bdc4' --link /tmp/meshtty --mtu 50
# host B:
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 \
    --handler serial --peer '!81b8aaf8' --link /tmp/meshtty --mtu 50
```

Then point your serial software at the printed `/dev/pts/N` (or the `--link` path).
Both nodes must be on the **same Meshtastic channel**.

## Use — MQTT gateway (mesh telemetry → cloud)

Run on a **gateway node** (role `CLIENT_MUTE`) wired to the host by USB; it forwards
every received PortNum-256 (Modbus telemetry) packet to MQTT in the nafco-compatible
JSON (`msh/2/json/<channel>/<node>`, `{portnum, payload:{raw:b64}, sender, …}`) so an
existing MQTT → decoder → InfluxDB pipeline consumes it unchanged.

```sh
pip install '.[mqtt]'      # the mqtt handler needs paho-mqtt
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0     --handler mqtt --broker <broker-host> --channel BW500SF9
```
No WiFi/MQTT on the node — the host does the MQTT step (the node stays pure-mesh).

Add `--web-port 9090` to also serve a **live telemetry view** (latest-by-node +
event stream of the raw PortNum-256 frames) at `http://<host>:9090` — a lightweight
replacement for a dashboard.

## No command line — the control panel

A small **localhost web UI** to start/stop the bridge and watch its log, for users
who don't want the CLI. It needs host OS access (serial/PTY/BLE/MQTT), so it's a tiny
local server (stdlib only) that spawns the bridge for you.

```sh
siliqs-mesh-bridge-web        # then open http://127.0.0.1:8765
```
Pick the transport + handler in the form, click **Start**, and open the printed
`/dev/pts/…` (or your `link` path) with your serial software. Binds 127.0.0.1 only.

### Framing (it does **not** stream byte-by-byte)

| `--mode` | behaviour |
|---|---|
| `line` (default) | buffer until **Enter** (CR or LF) or `--mtu` bytes, then send the **whole line as one packet** (line ending kept). For consoles / text / type-and-Enter. |
| `stream` | binary with no line endings — batch by a `--coalesce-ms` idle gap or `--mtu`. |

Why: USB is far faster than LoRa, so sending one packet per byte is hopeless. Line
mode paces it to one packet per line.

### Testing with `screen`
`screen /tmp/meshtty` works, but note: it's a **raw** line (no local echo) — you won't
see your own typing; the line appears on the **other** end after Enter. That's normal.
Don't test with `cat` (PTY/SIGTTIN quirk); real serial software is fine.

## PortNum

The serial pipe uses **PortNum 260** — an *unnamed* value in Meshtastic's private
range (256–510), so it collides with no Meshtastic service (256 = `PRIVATE_APP`,
257 = `ATAK_FORWARDER`, 511 = `MAX`).

## Roadmap

- `mqtt` handler (forward received telemetry to MQTT) — one app does serial + MQTT.
- `pip install siliqs-mesh-bridge` from a package index.

## License

**GPL-3.0-or-later** (see `LICENSE`) — it builds on the GPL-3.0 `meshtastic` Python
library.
