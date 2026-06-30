# Host-side Meshtastic bridge — for gateways / SBCs where native Python is awkward
# (e.g. Linxdot / OpenWrt, which has Docker but a painful opkg Python).
#
# Build:  docker build -t siliqs-mesh-bridge .
# Run (USB):
#   docker run --rm -it --device /dev/ttyACM0 siliqs-mesh-bridge \
#     --iface usb --port /dev/ttyACM0 --handler serial --peer '!7d51bdc4' --link /tmp/meshtty
#   (mount /tmp out with -v if other host software needs the PTY symlink)
# BLE needs host Bluetooth + dbus passthrough (harder) — prefer USB in a container.
FROM python:3.12-slim

RUN pip install --no-cache-dir meshtastic

COPY siliqs_mesh_bridge.py /app/siliqs_mesh_bridge.py
WORKDIR /app

ENTRYPOINT ["python", "siliqs_mesh_bridge.py"]
