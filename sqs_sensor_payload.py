"""Decoder for the Siliqs SQS-Sensor line mesh payloads (PortNum 258).

One decoder for the whole SQS line — the `ver` + `model` header bytes let a single
function serve SQS-TH-I today and future SQS-* models. Mirror of the receiver's
`08_.../src/backend/payload.py` shape (pure function + dict), kept standalone so the
gateway, the cloud, and unit tests can all import it.

Wire format (little-endian, 11 bytes) — the source of truth is the firmware header
`sqs-sensor-mesh-fw/src/modules/SensorTHModule.h`; this is a shipped CONTRACT and the
`ver` byte gates any change:

  [0] ver   u8   payload format version (=1)
  [1] model u8   product id (1 = SQS-TH-I)
  [2] temp  i16  temperature       x100 degC
  [4] rh    u16  relative humidity x100 %RH
  [6] vbat  u16  battery millivolts (0 = not measured yet / no battery)
  [8] soc   u8   state of charge % (0..100); 0xFF = unknown / no battery
  [9] flags u8   bit0 sensor-nack, bit1 heater-ran, bit2 on-usb, bit3 charging,
                 bit4 no-battery
 [10] boot  u8   reboot/wake counter (wraps)
"""
from __future__ import annotations

import struct

SENSOR_PORTNUM = 258          # unnamed private port (256=PRIVATE_APP, 257=ATAK_FORWARDER)
SQS_PAYLOAD_LEN = 11
SQS_PAYLOAD_VER = 1

MODEL_NAMES = {1: "SQS-TH-I"}

# (json key, bit mask) — order matches SensorTHModule.h
_FLAG_BITS = (
    ("sensor_nack", 0x01),
    ("heater_ran", 0x02),
    ("on_usb", 0x04),
    ("charging", 0x08),
    ("no_battery", 0x10),
)

_STRUCT = struct.Struct("<BBhHHBBB")  # ver B, model B, temp h, rh H, vbat H, soc B, flags B, boot B


class SqsPayloadError(ValueError):
    """Raised when the bytes are not a valid SQS-Sensor payload."""


def decode_sqs_th(raw: bytes) -> dict:
    """Decode an 11-byte SQS-Sensor payload into engineering values.

    Tolerant of trailing bytes (forward-compat: a future `ver` may append fields);
    raises SqsPayloadError only if too short to hold v1.
    """
    if len(raw) < SQS_PAYLOAD_LEN:
        raise SqsPayloadError(f"payload too short: {len(raw)} < {SQS_PAYLOAD_LEN}")
    ver, model, temp, rh, vbat, soc, flags, boot = _STRUCT.unpack_from(raw, 0)
    return {
        "ver": ver,
        "model_id": model,
        "model": MODEL_NAMES.get(model, f"unknown-{model}"),
        "temp_c": round(temp / 100.0, 2),
        "rh_pct": round(rh / 100.0, 2),
        "vbat_mv": vbat,
        "soc_pct": None if soc == 0xFF else soc,
        "flags": {name: bool(flags & bit) for name, bit in _FLAG_BITS},
        "flags_raw": flags,
        "boot": boot,
    }
