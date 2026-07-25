"""Tests for the SQC485I (PortNum 256) raw-forward payload decoder."""

import os
import struct
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqc485i_payload import (  # noqa: E402
    Sqc485iPayloadError,
    decode_raw_forward,
    parse_config_blob,
    payload_len_for,
)


def _poll(slave=1, function=3, reg_start=0, reg_count=2):
    return {"slave": slave, "function": function, "reg_start": reg_start, "reg_count": reg_count}


def _ok_frame(slave, function, values):
    """A successful raw-forward frame: [slave][function][data]."""
    body = bytes([slave, function])
    for v in values:
        body += v.to_bytes(2, "big")
    return body


def _err_frame(slave, function, err, reg_count, exception_code=None):
    """A failed poll's fixed-length error frame."""
    frame = bytearray([slave, function | 0x80] + [err] * (2 * reg_count))
    if exception_code is not None:
        frame[3] = exception_code
    return bytes(frame)


def _crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def _blob(version=4, name="unit-test", polls=((1, 3, 0, 2),), flags=0, baud=9600, parity=0):
    """Build a config blob the way the firmware does (config.h layout)."""
    body = bytearray(b"SQ" + bytes([version, flags]))
    body += name.encode()[:16].ljust(16, b"\x00")
    body += struct.pack("<I", baud) + bytes([parity, 1])
    body += struct.pack("<H", 1000) + bytes([3]) + struct.pack("<H", 20)
    body += struct.pack("<I", 60) + bytes([1, len(polls)])
    for slave, function, reg_start, reg_count in polls:
        body += bytes([slave, function]) + struct.pack("<HH", reg_start, reg_count)
    if version >= 3:
        body += struct.pack("<I", 0) + bytes([0])
    if version >= 4:
        body += struct.pack("<I", 0) + struct.pack("<H", 20)
    body += struct.pack("<H", _crc16(bytes(body)))
    return bytes(body)


# ── the poll plan ────────────────────────────────────────────────────────────


def test_config_blob_yields_the_poll_plan():
    plan = parse_config_blob(_blob(polls=((7, 4, 100, 3), (2, 3, 40, 1)), baud=19200, parity=2))
    assert plan["blob_version"] == 4
    assert plan["device_name"] == "unit-test"
    assert plan["baud"] == 19200
    assert plan["parity"] == "odd"
    assert plan["polls"] == [
        {"slave": 7, "function": 4, "reg_start": 100, "reg_count": 3},
        {"slave": 2, "function": 3, "reg_start": 40, "reg_count": 1},
    ]


def test_config_blob_accepts_older_versions():
    """A configurator in the field still speaks v2; its blobs must still parse."""
    for version in (2, 3, 4):
        plan = parse_config_blob(_blob(version=version))
        assert plan["blob_version"] == version
        assert len(plan["polls"]) == 1


def test_config_blob_flags():
    plan = parse_config_blob(_blob(flags=0x01 | 0x04))
    assert plan["rs485_enabled"] is False
    assert plan["confirmed"] is True
    assert plan["tunnel_enabled"] is False


@pytest.mark.parametrize(
    "mangle, why",
    [
        (lambda b: b"XQ" + b[2:], "bad magic"),
        (lambda b: b[:2] + bytes([9]) + b[3:], "unknown version"),
        (lambda b: b[:-1] + bytes([b[-1] ^ 0xFF]), "bad crc"),
        (lambda b: b[:-1], "truncated"),
        (lambda b: b[:10], "far too short"),
    ],
)
def test_config_blob_rejects_malformed_input(mangle, why):
    with pytest.raises(Sqc485iPayloadError):
        parse_config_blob(mangle(_blob()))


# ── the payload ──────────────────────────────────────────────────────────────


def test_decodes_successful_polls():
    polls = [_poll(1, 3, 0, 2), _poll(2, 4, 10, 1)]
    raw = _ok_frame(1, 3, [0xAA55, 0x1234]) + _ok_frame(2, 4, [0xBEEF])

    out = decode_raw_forward(raw, polls)
    assert out["ok_count"] == 2 and out["error_count"] == 0
    assert out["readings"][0]["registers"] == [0xAA55, 0x1234]
    assert out["readings"][0]["raw"] == "aa551234"
    assert out["readings"][1]["registers"] == [0xBEEF]


def test_payload_length_is_predicted_by_the_plan():
    polls = [_poll(reg_count=2), _poll(reg_count=1), _poll(reg_count=16)]
    assert payload_len_for(polls) == 6 + 4 + 34


def test_error_frame_keeps_later_polls_aligned():
    """The reason error frames are fixed-length: a failure must not shift the rest."""
    polls = [_poll(1, 3, 0, 1), _poll(2, 3, 10, 2), _poll(3, 3, 20, 1)]
    raw = _ok_frame(1, 3, [0xAAAA]) + _err_frame(2, 3, 1, 2) + _ok_frame(3, 3, [0xCCCC])

    out = decode_raw_forward(raw, polls)
    assert [r["ok"] for r in out["readings"]] == [True, False, True]
    assert out["readings"][1]["error"] == "timeout"
    assert out["readings"][2]["registers"] == [0xCCCC]  # still at the right offset


@pytest.mark.parametrize(
    "code, name",
    [(1, "timeout"), (2, "crc"), (4, "short"), (5, "mismatch")],
)
def test_error_codes_are_named(code, name):
    out = decode_raw_forward(_err_frame(1, 3, code, 2), [_poll(reg_count=2)])
    reading = out["readings"][0]
    assert reading["ok"] is False
    assert reading["error"] == name
    assert reading["error_code"] == code
    assert "exception_code" not in reading  # only refusals carry one


def test_refusal_reports_the_slaves_own_reason():
    """The distinction the whole error frame exists to carry: 0x02 means fix the
    poll plan, 0x06 means look at the device."""
    out = decode_raw_forward(_err_frame(1, 3, 3, 2, exception_code=0x02), [_poll(reg_count=2)])
    reading = out["readings"][0]
    assert reading["error"] == "exception"
    assert reading["exception_code"] == 0x02
    assert reading["exception"] == "illegal data address"

    out = decode_raw_forward(_err_frame(1, 3, 3, 1, exception_code=0x06), [_poll(reg_count=1)])
    assert out["readings"][0]["exception"] == "server device busy"


def test_unknown_exception_code_is_reported_not_dropped():
    out = decode_raw_forward(_err_frame(1, 3, 3, 2, exception_code=0x7F), [_poll(reg_count=2)])
    assert out["readings"][0]["exception_code"] == 0x7F
    assert "unknown" in out["readings"][0]["exception"]


def test_truncated_payload_reports_the_missing_polls():
    """A plan wider than one mesh packet is truncated on a poll boundary by the
    node; the polls that did arrive are still worth reporting."""
    polls = [_poll(1, 3, 0, 2), _poll(2, 3, 10, 2)]
    raw = _ok_frame(1, 3, [0x1111, 0x2222])  # only the first poll made it

    out = decode_raw_forward(raw, polls)
    assert out["ok_count"] == 1 and out["error_count"] == 1
    assert out["readings"][1]["error"] == "missing"
    assert out["bytes"] == 6 and out["expected_bytes"] == 12


def test_empty_plan_decodes_to_nothing():
    out = decode_raw_forward(b"", [])
    assert out["readings"] == [] and out["ok_count"] == 0


def test_round_trip_from_a_config_blob():
    """The path the gateway actually takes: ask the node for its plan, then use
    that plan to slice the node's uplinks."""
    plan = parse_config_blob(_blob(polls=((1, 3, 0, 2), (9, 3, 0, 1))))
    raw = _ok_frame(1, 3, [0x0102, 0x0304]) + _err_frame(9, 3, 3, 1, exception_code=0x02)

    out = decode_raw_forward(raw, plan["polls"])
    assert out["bytes"] == out["expected_bytes"]
    assert out["readings"][0]["registers"] == [0x0102, 0x0304]
    assert out["readings"][1]["exception"] == "illegal data address"
