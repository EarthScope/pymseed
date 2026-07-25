import gc
import io
import os

import pytest

from pymseed import MiniSEEDError, MS3Record
from pymseed.clib import clibmseed

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")
test_path2 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")


def _read(path):
    with open(path, "rb") as f:
        return f.read()


def test_from_filelike_empty():
    records = list(MS3Record.from_filelike(io.BytesIO(b"")))
    assert records == []


def test_from_filelike_equivalence_v3():
    data = _read(test_path3)
    buffer_counts = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    filelike_counts = [
        (msr.sourceid, msr.samplecnt) for msr in MS3Record.from_filelike(io.BytesIO(data))
    ]
    assert filelike_counts == buffer_counts


def test_from_filelike_equivalence_v2():
    data = _read(test_path2)
    record_count = 0
    sample_count = 0
    for msr in MS3Record.from_filelike(io.BytesIO(data)):
        record_count += 1
        sample_count += msr.samplecnt
    assert record_count == 1141
    assert sample_count == 252000


def test_from_filelike_small_chunk_size():
    data = _read(test_path3)
    expected = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    actual = [
        (msr.sourceid, msr.samplecnt)
        for msr in MS3Record.from_filelike(io.BytesIO(data), chunk_size=64)
    ]
    assert actual == expected


def test_from_filelike_single_byte_chunks():
    data = _read(test_path3)
    expected_count = sum(1 for _ in MS3Record.from_buffer(data))
    actual_count = sum(1 for _ in MS3Record.from_filelike(io.BytesIO(data), chunk_size=1))
    assert actual_count == expected_count


def test_from_filelike_unpack_data():
    data = _read(test_path3)

    buf_samples = []
    for msr in MS3Record.from_buffer(data, unpack_data=True):
        if msr.numsamples > 0:
            buf_samples.append(list(msr.datasamples))

    fl_samples = []
    for msr in MS3Record.from_filelike(io.BytesIO(data), unpack_data=True):
        if msr.numsamples > 0:
            fl_samples.append(list(msr.datasamples))

    assert fl_samples == buf_samples


def test_from_filelike_first_record_details():
    data = _read(test_path3)
    msr_buf = MS3Record.parse(data)

    # Keep the generator alive while accessing record fields (the C struct is
    # owned by the generator and freed when it is closed/GC'd)
    gen = MS3Record.from_filelike(io.BytesIO(data))
    msr_fl = next(gen)

    assert msr_fl.sourceid == msr_buf.sourceid
    assert msr_fl.reclen == msr_buf.reclen
    assert msr_fl.starttime == msr_buf.starttime
    assert msr_fl.samplecnt == msr_buf.samplecnt
    assert msr_fl.samprate == msr_buf.samprate
    assert msr_fl.encoding == msr_buf.encoding

    gen.close()


# iter_records dispatch tests


def test_iter_records_from_path():
    data = _read(test_path3)
    expected = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    actual = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.iter_records(test_path3)]
    assert actual == expected


def test_iter_records_from_pathlike():
    import pathlib

    data = _read(test_path3)
    expected = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    actual = [
        (msr.sourceid, msr.samplecnt) for msr in MS3Record.iter_records(pathlib.Path(test_path3))
    ]
    assert actual == expected


def test_iter_records_from_filelike():
    data = _read(test_path3)
    expected = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    actual = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.iter_records(io.BytesIO(data))]
    assert actual == expected


def test_iter_records_from_buffer():
    data = _read(test_path3)
    expected = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)]
    actual = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.iter_records(data)]
    assert actual == expected


def test_iter_records_kwargs_forwarded():
    data = _read(test_path3)
    samples_buf = [
        list(msr.datasamples)
        for msr in MS3Record.from_buffer(data, unpack_data=True)
        if msr.numsamples > 0
    ]
    samples_fl = [
        list(msr.datasamples)
        for msr in MS3Record.iter_records(io.BytesIO(data), unpack_data=True)
        if msr.numsamples > 0
    ]
    assert samples_fl == samples_buf


def test_record_survives_temporary_filelike_generator():
    """A record must not read freed memory when its generator is a temporary.

    The decoded samples are libmseed-owned and stay valid; the raw record bytes
    live in the generator's sliding buffer and are not retained, so only the
    struct-level access is checked here.
    """
    msr = next(MS3Record.from_filelike(io.BytesIO(_read(test_path3)), unpack_data=True))
    gc.collect()
    _churn = [bytearray(4096) for _ in range(3000)]

    assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert msr.numsamples == msr.samplecnt
    assert msr.datasamples[0] == -502916


def test_from_filelike_reports_truncated_final_record():
    """A stream ending mid-record must not read as a clean end of data."""
    data = _read(test_path3)

    records = []
    with pytest.raises(MiniSEEDError, match="100 more bytes needed") as excinfo:
        for msr in MS3Record.from_filelike(io.BytesIO(data[:-100])):
            records.append(msr.sourceid)

    assert excinfo.value.status_code == clibmseed.MS_ENDOFFILE
    assert len(records) == 1140


def test_from_filelike_reports_trailing_bytes():
    """Bytes too few for any record must be reported, not dropped."""
    data = _read(test_path3)

    with pytest.raises(MiniSEEDError, match="2 unparsed bytes"):
        list(MS3Record.from_filelike(io.BytesIO(data + b"xx")))


def test_from_filelike_sizes_final_v2_record_without_blockette_1000():
    """An exhausted stream sizes a trailing v2 record lacking Blockette 1000."""
    data = bytearray(_read(test_path2))

    # Drop the first blockette offset from each 512-byte record
    for offset in range(0, len(data), 512):
        data[offset + 46 : offset + 48] = b"\x00\x00"

    assert sum(1 for _ in MS3Record.from_filelike(io.BytesIO(bytes(data)))) == 1141


def test_from_filelike_reports_data_too_short_as_not_seed():
    """Too little data for any record reads as not miniSEED, as from_file() does."""
    with pytest.raises(MiniSEEDError, match="No miniSEED data detected") as excinfo:
        list(MS3Record.from_filelike(io.BytesIO(b"hello")))

    assert excinfo.value.status_code == clibmseed.MS_NOTSEED
