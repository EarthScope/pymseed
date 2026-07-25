import gc
import os

import pytest

from pymseed import DataEncoding, MiniSEEDError, MS3Record, SubSecond, TimeFormat
from pymseed.clib import clibmseed

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")
test_path2 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")


def test_msrecord_read_buffer_details():
    # Read data from test file into a buffer and parse the first record
    with open(test_path3, "rb") as fp:
        buffer = bytearray(fp.read())

    msr = MS3Record.parse(buffer, unpack_data=True)

    assert msr.reclen == 478
    assert msr.swapflag == 2
    assert msr.swapflag_dict() == {
        "header_swapped": False,
        "payload_swapped": True,
    }
    assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert msr.formatversion == 3
    assert msr.flags == 4
    assert msr.flags_dict() == {"clock_locked": True}
    assert msr.starttime == 1267253400019539000
    assert msr.starttime_seconds == 1267253400.019539
    assert msr.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_Z) == "2010-02-27T06:50:00.019539Z"
    assert (
        msr.starttime_str(timeformat=TimeFormat.SEEDORDINAL, subsecond=SubSecond.NONE)
        == "2010,058,06:50:00"
    )
    assert msr.samprate == 20.0
    assert msr.samprate_raw == 20.0
    assert msr.encoding == DataEncoding.STEIM2
    assert msr.encoding_str() == "STEIM-2 integer compression"
    assert msr.pubversion == 4
    assert msr.samplecnt == 296
    assert msr.crc == 0x4DFE46ED
    assert msr.extralength == 33
    assert msr.datalength == 384
    assert msr.extra == '{"FDSN":{"Time":{"Quality":100}}}'
    assert msr.numsamples == 296
    assert msr.sampletype == "i"
    assert msr.endtime == 1267253414769539000
    assert msr.endtime_seconds == 1267253414.769539

    # Data sample array tests
    data = msr.datasamples

    # Check first 6 samples
    assert data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

    # Check last 6 samples
    assert data[-6:].tolist() == [-508722, -508764, -508809, -508866, -508927, -508986]


def test_msrecord_numpy():
    np = pytest.importorskip("numpy")

    with open(test_path3, "rb") as fp:
        buffer = bytearray(fp.read())

    msr = MS3Record.parse(buffer, unpack_data=True)

    # Data sample array tests
    data = msr.np_datasamples

    # Check first 6 samples
    assert np.all(data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331])

    # Check last 6 samples
    assert np.all(data[-6:].tolist() == [-508722, -508764, -508809, -508866, -508927, -508986])


def test_msrecord_read_buffer_summary():
    # Read data from test file into a buffer
    with open(test_path2, "rb") as fp:
        buffer = bytearray(fp.read())

    record_count = 0
    sample_count = 0

    for msr in MS3Record.from_buffer(buffer):
        record_count += 1
        sample_count += msr.samplecnt

    assert record_count == 1141
    assert sample_count == 252000


def test_record_survives_temporary_buffer_generator():
    """A record must not read freed memory when its generator is a temporary.

    from_buffer() reuses one struct for the whole iteration and used to free it
    when the generator was collected, leaving an escaped record pointing at
    released memory.
    """
    with open(test_path3, "rb") as f:
        data = f.read()

    msr = next(MS3Record.from_buffer(data, unpack_data=True))
    gc.collect()
    _churn = [bytearray(4096) for _ in range(3000)]

    assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert msr.numsamples == msr.samplecnt
    assert msr.datasamples[0] == -502916
    assert msr.record[:2] == b"MS"


def test_from_buffer_reports_truncated_final_record():
    """A buffer ending mid-record must not read as a clean end of data."""
    with open(test_path3, "rb") as fp:
        data = fp.read()

    with pytest.raises(MiniSEEDError, match="100 more bytes needed") as excinfo:
        list(MS3Record.from_buffer(data[:-100]))

    assert excinfo.value.status_code == clibmseed.MS_ENDOFFILE

    # The records before the truncation are still yielded
    records = []
    with pytest.raises(MiniSEEDError):
        for msr in MS3Record.from_buffer(data[:-100]):
            records.append(msr.sourceid)
    assert len(records) == 1140


def test_from_buffer_reports_trailing_bytes():
    """Bytes too few for any record must be reported, not dropped."""
    with open(test_path3, "rb") as fp:
        data = fp.read()

    with pytest.raises(MiniSEEDError, match="2 unparsed bytes"):
        list(MS3Record.from_buffer(data + b"xx"))

    # An empty buffer is a clean, empty result
    assert list(MS3Record.from_buffer(b"")) == []


def test_from_buffer_sizes_final_v2_record_without_blockette_1000():
    """The buffer is the end of the data, which sizes a v2 record lacking B1000.

    Records followed by another are sized by scanning for the next header; only
    the final one needs the end-of-data length implied by the buffer.
    """
    with open(test_path2, "rb") as fp:
        data = bytearray(fp.read())

    # Drop the first blockette offset from each 512-byte record
    for offset in range(0, len(data), 512):
        data[offset + 46 : offset + 48] = b"\x00\x00"

    assert sum(1 for _ in MS3Record.from_buffer(bytes(data))) == 1141


def test_from_buffer_reports_data_too_short_as_not_seed():
    """Too little data for any record reads as not miniSEED, as from_file() does."""
    with pytest.raises(MiniSEEDError, match="No miniSEED data detected") as excinfo:
        list(MS3Record.from_buffer(b"hello"))

    assert excinfo.value.status_code == clibmseed.MS_NOTSEED
