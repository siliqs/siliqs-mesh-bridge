# siliqs-mesh-bridge — the Siliqs Gateway

A **host-side gateway** with **one job**: carry packets **both ways between a Meshtastic
node's LoRa mesh and an MQTT broker**. The connected node (over **USB or BLE**) is your
host's access point onto the mesh; this program is the bridge. The node runs **stock
Meshtastic firmware** — no firmware change, no custom BLE service, no console takeover.

```
field edge nodes ──mesh──▶ your node (USB/BLE) ──▶ THIS (mesh⇄MQTT) ──▶ broker ──▶ sub/pub clients
```

Everything downstream — dashboards, a decoder, a database writer, even a **virtual serial
cable** — is just an ordinary **MQTT client** on the broker. The serial cable is a separate
sub-program, [`siliqs-serial-mqtt`](#virtual-serial-cable-over-mqtt), that rides this same
broker (so it can run on *any* host that can reach it, not only the one wired to the node).

**No broker of your own?** The Gateway has a [**built-in one**](#broker-built-in-or-your-own-with-auth)
so it works out of the box — or point it at your own broker (username/password, TLS, or mTLS
client certs for AWS IoT-style setups).

Built for the Siliqs **SQC485I** but works with any Meshtastic node. Useful when a host
(RPi / Linux / OpenWrt / PC) needs the mesh's data in MQTT, or a remote serial/RS485 device
reachable over LoRa.

> ⚠️ **LoRa is slow and best-effort.** For **low-rate, request/reply** traffic (Modbus,
> console, occasional messages) — **not** high-throughput or latency-sensitive streams.
> Also: `siliqs/tx/<node>` lets anyone with broker access transmit on your mesh — secure
> the broker accordingly.

## Charter — the one problem this solves

Siliqs ships three sibling tools, one per step of a node's life: **provision → configure → integrate** (裝 → 設 → 接). This is **step 3, integrate**.

- **Who** — someone who needs the mesh data **in their own system** (integrator / IT / has a cloud or plant control).
- **The pain (user's words)** — *"The node runs on the mesh, but the data is **stuck in the mesh** — my cloud / database / SCADA can't reach it. And I don't want to put WiFi/MQTT on the node."*
- **What it removes** — the node never has to be online; the **host** translates the mesh data and delivers it onward.
- **In → out** — a node on USB/BLE → the mesh⇄MQTT bridge → data on your **MQTT broker**, where any sub/pub client consumes it (dashboards, a DB writer, a serial cable, or a future Modbus-TCP adapter).
- **Done when** — the remote RS485 device's values show up in the user's **existing** system.

> **One-liner: "Bring mesh data into the system you already run."**

### For future maintainers — stay on this target

This app is a **data on-ramp** — it gets mesh data *out* to somewhere the user already runs. Before adding a feature, ask: *does it help move data from a connected node into an external system?* If not, it belongs to a sibling tool:

- Getting **firmware onto the board** → **Flasher** (flasher.siliqs.net).
- Deciding **what the node does** (role / Modbus poll plan / deep sleep) → **Configurator** (mesh.siliqs.net).

Adding a **new north-bound output** (MQTT, Modbus-TCP, HTTP webhook, InfluxDB) is squarely in scope — that's the same job, one more exit. **Re-implementing node configuration or firmware flashing here is not** — it duplicates a sibling and blurs the boundary. One legitimate grey area: a **read-only "is it working?" view** (nodes seen, last-heard, decoded values). That's observability; it's fine *as a window on the data already flowing through this app*, but if it grows into managing the fleet it has become its own tool — split it out. When in doubt, keep this focused on *node in → external system out.*

## Download & run (no Python needed)

Grab the single-file app for your OS from the
[**Releases**](https://github.com/livinghuang/siliqs-mesh-bridge/releases) page and run it —
it opens a **browser control panel** (`http://127.0.0.1:8765`) where you pick your node
(USB/BLE), your MQTT broker, and Start. No install, no command line.

| OS | File | Run |
|----|------|-----|
| **Windows** | `siliqs-mesh-bridge-windows-x86_64.exe` | double-click (SmartScreen: *More info → Run anyway*) |
| **macOS** | `siliqs-mesh-bridge-macos-arm64.zip` | unzip → double-click **siliqs-mesh-bridge.app**; first launch: **right-click → Open** (unsigned) |
| **Linux** | `siliqs-mesh-bridge-linux-x86_64` | `chmod +x ./siliqs-mesh-bridge-linux-x86_64 && ./…` |

Close the app with the **"⏻ Quit app"** button in the control panel (or, on Windows/Linux,
close the console window). The builds are unsigned (community app) — the "unknown developer"
prompt is expected; the right-click/*Run anyway* step is a one-time bypass. Serial devices may
need a driver (CH340/CP2102) and, on Linux, your user in the `dialout` group; BLE needs host
Bluetooth.

Prefer a package manager or a headless box? See **Install** below (pip / Docker / systemd).

## Install

```sh
# pip (in a venv) — Linux / RPi / Mac
python3 -m venv venv && venv/bin/pip install .
venv/bin/siliqs-mesh-bridge --help

# or run the single file directly (needs `meshtastic` + `paho-mqtt`)
pip install meshtastic paho-mqtt && python siliqs_mesh_bridge.py --help

# Docker (good for Linxdot / OpenWrt — see Dockerfile)
docker build -t siliqs-mesh-bridge .
```
BLE (`--iface ble`) additionally needs `bleak` (pulled in by `meshtastic`) and host
Bluetooth; on Linux it just works, in Docker it needs dbus/Bluetooth passthrough.

## Run — the Gateway (mesh ⇄ MQTT)

Run on a node wired to the host by USB (or BLE). It forwards every received packet to
MQTT and injects anything published to its downlink topic back onto the mesh. For a
telemetry gateway use a **`CLIENT_MUTE`** node.

```sh
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 --broker <broker-host> --channel BW500SF9
# BLE instead of USB:
siliqs-mesh-bridge --iface ble --ble 'SQC485I' --broker 127.0.0.1
# add a live telemetry web view on :9090
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 --broker 127.0.0.1 --web-port 9090
```
No WiFi/MQTT on the node — the host does the MQTT step (the node stays pure-mesh).
`--channel` is optional — omit it and the Gateway **auto-reads the node's primary channel
name** for the topic segment.

### Broker: built-in, or your own (with auth)

**No broker of your own?** The Gateway ships a tiny **built-in MQTT broker** so it works out
of the box — start it and point your dashboards / Node-RED / Grafana at this machine:

```sh
siliqs-mqtt-broker --host 0.0.0.0 --port 1883      # standalone; or the control panel runs it for you
```
(Zero dependencies, pure-Python, QoS 0 + `+`/`#` wildcards — enough for telemetry + serial.
No TLS/auth, so keep it on a trusted LAN.)

**Have your own broker?** Point the Gateway at it. External brokers usually need auth:

```sh
# username / password (password via env — keeps it out of `ps` and logs)
SMB_MQTT_PASSWORD=secret siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 \
    --broker broker.example.com --broker-port 8883 --tls --username alice

# client-certificate auth (mTLS — e.g. AWS IoT Core)
siliqs-mesh-bridge --iface usb --port /dev/ttyACM0 \
    --broker xxxx.iot.ap-northeast-1.amazonaws.com --broker-port 8883 --tls \
    --ca-cert AmazonRootCA1.pem --client-cert device.pem.crt --client-key private.pem.key
```

The same `--username/--password/--tls/--ca-cert/--client-cert/--client-key` (and the
`SMB_MQTT_PASSWORD` env var) work on `siliqs-serial-mqtt` too. In the control panel, all of
this is a **Built-in / External** toggle with the auth fields — and the password is never put
in the command line, the log, or sent back to the browser.

### The MQTT topics

Let `G` = the connected node's id (`!hex`), which the Gateway prints on start.

| Direction | Topic | Payload |
|---|---|---|
| uplink · telemetry | `msh/2/json/<channel>/<node>` | nafco-compatible JSON (`{portnum, payload:{raw:b64}, sender, …}`) for **PortNum 256** — so an existing MQTT → decoder → InfluxDB pipeline consumes it unchanged |
| uplink · generic | `siliqs/rx/<G>/<portnum>` | every received packet: `{from,to,portnum,channel,rssi,snr,t,data(b64)}` |
| downlink | `siliqs/tx/<G>` | publish `{to,portnum,data(b64),wantAck?,channelIndex?}` and the Gateway sends it on the mesh |

## Virtual serial cable over MQTT

The old built-in serial pipe is now a **standalone MQTT client**, `siliqs-serial-mqtt`. It
opens a local PTY (a virtual serial port) and maps it to a Gateway's `siliqs/tx|rx/<G>`
topics on **PortNum 260** — so the serial data rides the same broker as everything else and
can run on **any** host that can reach the broker.

Run a Gateway **and** the serial client on **both** hosts, `--peer` crossed:

```sh
# host A (node !81b8aaf8):
siliqs-mesh-bridge   --iface usb --port /dev/ttyACM0 --broker mybroker.lan
siliqs-serial-mqtt   --broker mybroker.lan --gateway '!81b8aaf8' --peer '!7d51bdc4' --link /tmp/meshtty --mtu 50
# host B (node !7d51bdc4):
siliqs-mesh-bridge   --iface usb --port /dev/ttyACM0 --broker mybroker.lan
siliqs-serial-mqtt   --broker mybroker.lan --gateway '!7d51bdc4' --peer '!81b8aaf8' --link /tmp/meshtty --mtu 50
```
Then point your serial software at the printed `/dev/pts/N` (or the `--link` path). Both
nodes must be on the **same Meshtastic channel**. `--gateway` is *your own* node (where your
bytes egress); the Gateway's control panel and startup banner both show it.

### Framing (it does **not** stream byte-by-byte)

| `--mode` | behaviour |
|---|---|
| `line` (default) | buffer until **Enter** (CR or LF) or `--mtu` bytes, then send the **whole line as one packet** (line ending kept). For consoles / text / type-and-Enter. |
| `stream` | binary with no line endings — batch by a `--coalesce-ms` idle gap or `--mtu`. |

## No command line — the control panel

A small **localhost web UI** to run the Gateway and watch it, for users who don't want the
CLI. It's a guided, three-step panel (**your node → your broker → run & watch**) with the
gateway data-flow diagram and a **live telemetry** side panel. In *your broker* you pick
**Built-in** (it runs the broker and shows the LAN address to point dashboards at) or
**External** (host/port + username/password/TLS/mTLS cert fields). Bilingual (EN / 繁中).
Stdlib-only server; it spawns the Gateway for you.

```sh
siliqs-mesh-bridge-web        # then open http://127.0.0.1:8765
```
It binds 127.0.0.1 by default; use `--host 0.0.0.0` to reach it on the LAN. For an
appliance, set `SMB_CONFIG_FILE=/data/config.json`: the last applied config is saved there
and **auto-started on boot** (survives reboot, no clicking).

Why: USB is far faster than LoRa, so sending one packet per byte is hopeless. Line
mode paces it to one packet per line.

### Testing with `screen`
`screen /tmp/meshtty` works, but note: it's a **raw** line (no local echo) — you won't
see your own typing; the line appears on the **other** end after Enter. That's normal.
Don't test with `cat` (PTY/SIGTTIN quirk); real serial software is fine.

## PortNums

- **256** (`PRIVATE_APP`) — the SQC485I Modbus telemetry the Gateway forwards as nafco JSON.
- **260** — the virtual serial cable (`siliqs-serial-mqtt`). An *unnamed* value in
  Meshtastic's private range (256–510) so it collides with no Meshtastic service
  (257 = `ATAK_FORWARDER`, 511 = `MAX`). The Gateway is generic, though — it bridges
  **any** PortNum both ways over `siliqs/rx/<G>/<pn>` and `siliqs/tx/<G>`.

## Roadmap

- More north-bound consumers as plain MQTT clients: a **Modbus-TCP** adapter (expose a
  remote RS485 device as a local Modbus-TCP slave), an InfluxDB writer, an HTTP webhook.
- `pip install siliqs-mesh-bridge` from a package index.

## License

**GPL-3.0-or-later** (see `LICENSE`) — it builds on the GPL-3.0 `meshtastic` Python
library.
