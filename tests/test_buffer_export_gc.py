"""Tests that buffer exports are released once pymseed no longer needs them.

pymseed releases each ``ffi.from_buffer()`` export explicitly, rather than
leaving it to the garbage collector, which avoids a crash on CPython before
3.12.7 (python/cpython#77894).

CPython refuses to resize a ``bytearray`` or ``array.array`` while an export of
it is outstanding, which is how a live export is detected here.
"""

import array
import gc
import os
import sys

import pytest

from pymseed import MS3Record, MS3RecordValidator, MS3TraceList
from pymseed.msrecord_validator import _BufferSource

pytestmark = pytest.mark.skipif(
    sys.implementation.name != "cpython",
    reason="requires the buffer export count of CPython",
)

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")

with open(test_path3, "rb") as _f:
    _valid_bytes = _f.read()

# Offset of the second record in the test file
_SECOND_RECORD = 478


def _valid() -> bytearray:
    return bytearray(_valid_bytes)


def _corrupt() -> bytearray:
    """Test file with 40 bytes zeroed in the second record, failing its CRC."""
    data = _valid()
    data[600:640] = bytes(40)
    return data


def _raised(func, *args, **kwargs) -> Exception:
    """Return the exception raised by the call.

    The caller keeps it, and with it the traceback and every frame's locals,
    so an export is released only if the code released it explicitly rather
    than by letting its frame go.
    """
    try:
        func(*args, **kwargs)
    except Exception as e:
        return e
    raise AssertionError("no exception raised")


def _assert_exported(buf):
    with pytest.raises(BufferError):
        buf.append(0)


def _assert_released(buf):
    buf.append(0)


def test_parse_releases_on_error():
    buf = bytearray(512)
    err = _raised(MS3Record.parse, buf)
    _assert_released(buf)
    del err


def test_parse_holds_until_record_freed():
    buf = _valid()
    msr = MS3Record.parse(buf)
    _assert_exported(buf)

    del msr
    gc.collect()
    _assert_released(buf)


def test_parse_into_releases_previous_buffer():
    first, second = _valid(), _valid()
    msr = MS3Record()

    msr.parse_into(first)
    _assert_exported(first)

    msr.parse_into(second)
    _assert_released(first)
    _assert_exported(second)

    del msr
    gc.collect()
    _assert_released(second)


def test_from_buffer_holds_until_records_freed():
    buf = _valid()
    records = list(MS3Record.from_buffer(buf))
    _assert_exported(buf)

    del records
    gc.collect()
    _assert_released(buf)


def test_with_datasamples_releases_on_exit():
    msr = MS3Record()
    samples = array.array("i", [1, 2, 3, 4])

    with msr.with_datasamples(samples, "i"):
        _assert_exported(samples)
    _assert_released(samples)

    samples = array.array("i", [1, 2, 3, 4])

    def failing_block():
        with msr.with_datasamples(samples, "i"):
            raise ValueError("error in block")

    err = _raised(failing_block)
    _assert_released(samples)
    del err


def test_tracelist_add_buffer_releases():
    buf = _valid()
    traces = MS3TraceList.from_buffer(buf, unpack_data=True)
    _assert_released(buf)
    traces.close()

    buf = _corrupt()
    err = _raised(MS3TraceList.from_buffer, buf, unpack_data=True)
    _assert_released(buf)
    del err


def test_tracelist_record_list_holds_until_close():
    buf = _valid()
    traces = MS3TraceList.from_buffer(buf, record_list=True)
    _assert_exported(buf)

    traces.close()
    _assert_released(buf)


def test_unpack_recordlist_releases_output_buffer():
    traces = MS3TraceList.from_file(test_path3, unpack_data=False, record_list=True)
    segment = traces[0][0]

    too_small = bytearray(1)
    err = _raised(segment.unpack_recordlist, too_small)
    _assert_released(too_small)
    del err

    output = bytearray(segment.samplecnt * segment.sample_size_type[0])
    segment.unpack_recordlist(output)
    _assert_released(output)

    traces.close()


def test_validator_releases_buffer():
    buf = _corrupt()
    errors, _traces = MS3RecordValidator.from_buffer(buf).validate()
    assert errors
    _assert_released(buf)


def test_buffer_source_close_releases():
    buf = _valid()
    records = iter(_BufferSource(buf))
    next(records)
    _assert_exported(buf)

    records.close()
    _assert_released(buf)
