"""Decoder for the Siliqs SQC485I line mesh payloads (PortNum 256).

Companion to `sqs_sensor_payload.py`, and standalone for the same reason: the
gateway, the cloud and unit tests all import this one implementation rather than
each growing their own.

Unlike the SQS payload, which is a fixed struct, an SQC485I uplink is a RAW
FORWARD — the concatenated Modbus replies, with no lengths and no separators:

    [slave][function][data …]  [slave][function][data …]  …

The only thing that says where one poll ends and the next begins is the node's
own poll plan, so `decode_raw_forward()` needs it. Ask the node for it with
'SQG?' and hand the reply to `parse_config_blob()`.

Every poll contributes exactly 2 + 2*reg_count bytes whether it succeeded or
not. A failed read emits a same-length ERROR FRAME rather than being dropped,
because dropping it would shift every following byte:

    [slave] [function|0x80] [sq_err] [sq_err …]

with one exception, literally: when sq_err is 3 (the slave answered and refused)
byte 3 carries ITS Modbus exception code instead of another copy of ours. 0x02
"illegal data address" means the poll plan names a register that is not there;
0x06 "device busy" means the plan is right and the device is not.

The source of truth for all of the above is the firmware:
`sqc485iv2-mesh-fw/src/siliqs/firmware_core/{poll,modbus,config}.{c,h}`.
"""

from __future__ import annotations

import struct

MODBUS_PORTNUM = 256  # PRIVATE_APP — where SQC485I telemetry arrives

# Our own error codes, padded into a failed poll's frame (firmware_core/modbus.h).
MB_ERR = {
    0: "ok",
    1: "timeout",
    2: "crc",
    3: "exception",
    4: "short",
    5: "mismatch",
}
MB_ERR_EXCEPTION = 3

# The slave's reason, byte 3 of a refusal frame. MODBUS Application Protocol
# V1.1b3 §7. The first group means the request is wrong, the rest mean the device
# is — which is what tells a technician where to look.
MB_EXCEPTION = {
    0x01: "illegal function",
    0x02: "illegal data address",
    0x03: "illegal data value",
    0x04: "server device failure",
    0x05: "acknowledge",
    0x06: "server device busy",
    0x08: "memory parity error",
    0x0A: "gateway path unavailable",
    0x0B: "gateway target failed to respond",
}

# Config blob (firmware_core/config.h). Only what is needed to slice a payload.
BLOB_HEADER_LEN = 37
BLOB_POLL_LEN = 6
BLOB_VERSIONS = (2, 3, 4)

PARITY_NAMES = {0: "none", 1: "even", 2: "odd"}


class Sqc485iPayloadError(ValueError):
    """Raised when the bytes are not a valid SQC485I payload or config blob."""


def _crc16(data: bytes) -> int:
    """Compute the standard Modbus RTU CRC16 (poly 0xA001, init 0xFFFF)."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def parse_config_blob(blob: bytes) -> dict:
    """Decode a node's config blob — the reply to 'SQG?', minus its 3-byte marker.

    Returns the link settings and the poll plan. The plan is what makes a raw
    forward decodable; the rest is useful for showing what the node is set to.
    """
    if len(blob) < BLOB_HEADER_LEN + 2 or blob[0:2] != b"SQ":
        raise Sqc485iPayloadError(f"not a config blob: {blob[:4].hex()}")

    version = blob[2]
    if version not in BLOB_VERSIONS:
        raise Sqc485iPayloadError(f"unsupported blob version {version}")

    poll_count = blob[36]
    extra = (5 if version >= 3 else 0) + (6 if version >= 4 else 0)
    need = BLOB_HEADER_LEN + poll_count * BLOB_POLL_LEN + extra + 2
    if len(blob) != need:
        raise Sqc485iPayloadError(f"blob length {len(blob)} != expected {need}")
    if struct.unpack_from("<H", blob, need - 2)[0] != _crc16(blob[: need - 2]):
        raise Sqc485iPayloadError("blob CRC mismatch")

    flags = blob[3]
    parity = blob[24]
    plan = {
        "blob_version": version,
        "device_name": blob[4:20].rstrip(b"\x00").decode("ascii", "replace"),
        "rs485_enabled": not flags & 0x01,
        "tunnel_enabled": bool(flags & 0x02),
        "confirmed": bool(flags & 0x04),
        "baud": struct.unpack_from("<I", blob, 20)[0],
        "parity": PARITY_NAMES.get(parity, parity),
        "stop_bits": blob[25],
        "response_timeout_ms": struct.unpack_from("<H", blob, 26)[0],
        "retries": blob[28],
        "poll_gap_ms": struct.unpack_from("<H", blob, 29)[0],
        "uplink_interval_s": struct.unpack_from("<I", blob, 31)[0],
        "deep_sleep": bool(blob[35]),
        "polls": [],
    }
    for i in range(poll_count):
        slave, function, reg_start, reg_count = struct.unpack_from("<BBHH", blob, BLOB_HEADER_LEN + i * BLOB_POLL_LEN)
        plan["polls"].append(
            {"slave": slave, "function": function, "reg_start": reg_start, "reg_count": reg_count}
        )
    return plan


def payload_len_for(polls: list[dict]) -> int:
    """Bytes a plan produces per uplink — the length a decoder should expect."""
    return sum(2 + 2 * p["reg_count"] for p in polls)


def decode_raw_forward(raw: bytes, polls: list[dict]) -> dict:
    """Slice a raw-forward payload using the node's poll plan.

    `polls` is `parse_config_blob(...)["polls"]`. Each entry yields one reading:
    the register values on success, or why the read failed. A short payload does
    not raise — the node truncates on a poll boundary when a plan does not fit a
    packet, so the polls that did arrive are still worth reporting.
    """
    readings = []
    at = 0
    for index, poll in enumerate(polls):
        width = 2 + 2 * poll["reg_count"]
        chunk = raw[at : at + width]
        at += width

        entry = {
            "index": index,
            "slave": poll["slave"],
            "function": poll["function"],
            "reg_start": poll["reg_start"],
            "reg_count": poll["reg_count"],
        }

        if len(chunk) < width:
            # The node stopped here: the plan is wider than one mesh packet.
            entry.update({"ok": False, "error": "missing", "detail": "payload ended before this poll"})
            readings.append(entry)
            continue

        function, data = chunk[1], chunk[2:]
        if not function & 0x80:
            entry.update(
                {
                    "ok": True,
                    "raw": data.hex(),
                    "registers": [int.from_bytes(data[i : i + 2], "big") for i in range(0, len(data), 2)],
                }
            )
            readings.append(entry)
            continue

        code = data[0] if data else 0xFF
        entry.update({"ok": False, "error": MB_ERR.get(code, f"unknown({code})"), "error_code": code})
        if code == MB_ERR_EXCEPTION and len(data) > 1:
            # The slave refused and said why — the one case where the padding
            # carries information instead of repeating itself.
            entry["exception_code"] = data[1]
            entry["exception"] = MB_EXCEPTION.get(data[1], f"unknown-{data[1]:#04x}")
        readings.append(entry)

    return {
        "readings": readings,
        "ok_count": sum(1 for r in readings if r["ok"]),
        "error_count": sum(1 for r in readings if not r["ok"]),
        "bytes": len(raw),
        "expected_bytes": payload_len_for(polls),
    }
