"""Tests for the SQS-Sensor (PortNum 258) payload decoder."""
import os
import struct
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqs_sensor_payload import SQS_PAYLOAD_LEN, SqsPayloadError, decode_sqs_th  # noqa: E402


def _pack(ver=1, model=1, temp=2657, rh=5470, vbat=0, soc=0xFF, flags=0x00, boot=1):
    """Build an 11-byte payload; temp/rh are the on-wire x100 integers."""
    return struct.pack("<BBhHHBBB", ver, model, temp, rh, vbat, soc, flags, boot)


def test_nominal_reading():
    d = decode_sqs_th(_pack(temp=2657, rh=5470))
    assert d["ver"] == 1
    assert d["model"] == "SQS-TH-I"
    assert d["temp_c"] == 26.57
    assert d["rh_pct"] == 54.7
    assert d["soc_pct"] is None            # 0xFF -> unknown
    assert d["boot"] == 1
    assert d["flags"] == {
        "sensor_nack": False, "heater_ran": False,
        "on_usb": False, "charging": False, "no_battery": False,
    }


def test_negative_temperature():
    d = decode_sqs_th(_pack(temp=-1234))   # -12.34 degC
    assert d["temp_c"] == -12.34


def test_flags_and_battery():
    d = decode_sqs_th(_pack(vbat=3700, soc=87, flags=0x02 | 0x08))  # heater + charging
    assert d["vbat_mv"] == 3700
    assert d["soc_pct"] == 87
    assert d["flags"]["heater_ran"] is True
    assert d["flags"]["charging"] is True
    assert d["flags"]["sensor_nack"] is False


def test_sensor_nack_flag():
    d = decode_sqs_th(_pack(temp=0, rh=0, flags=0x01))
    assert d["flags"]["sensor_nack"] is True


def test_unknown_model_passthrough():
    d = decode_sqs_th(_pack(model=99))
    assert d["model_id"] == 99
    assert d["model"].startswith("unknown")


def test_trailing_bytes_tolerated():
    d = decode_sqs_th(_pack() + b"\xde\xad")   # forward-compat: extra bytes ignored
    assert d["temp_c"] == 26.57


def test_too_short_raises():
    with pytest.raises(SqsPayloadError):
        decode_sqs_th(_pack()[: SQS_PAYLOAD_LEN - 1])
