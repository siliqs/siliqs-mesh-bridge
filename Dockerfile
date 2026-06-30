# Host-side Meshtastic bridge — for gateways / SBCs where native Python is awkward
# (e.g. Linxdot / OpenWrt, which has Docker but a painful opkg Python).
#
# Build:  docker build --network=host -t siliqs-mesh-bridge .
# Run (serial pipe):
#   docker run --rm -it --device /dev/ttyACM0 siliqs-mesh-bridge \
#     --iface usb --port /dev/ttyACM0 --handler serial --peer '!7d51bdc4' --link /tmp/meshtty
# Run (mqtt gateway, in a compose network with a `mosquitto` service):
#   ... --handler mqtt --port /dev/ttyACM0 --broker mosquitto --channel <ch>
# BLE needs host Bluetooth + dbus passthrough (harder) — prefer USB in a container.
FROM python:3.12-slim

WORKDIR /src
COPY . /src
# install the package + meshtastic + paho-mqtt (the mqtt gateway extra)
RUN pip install --no-cache-dir ".[mqtt]"

ENTRYPOINT ["siliqs-mesh-bridge"]
