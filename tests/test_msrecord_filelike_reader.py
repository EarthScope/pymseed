import io
import os

import pytest

from pymseed import MS3Record

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
    buffer_counts = [
        (msr.sourceid, msr.samplecnt) for msr in MS3Record.from_buffer(data)
    ]
    filelike_counts = [
        (msr.sourceid, msr.samplecnt)
        for msr in MS3Record.from_filelike(io.BytesIO(data))
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
    actual_count = sum(
        1 for _ in MS3Record.from_filelike(io.BytesIO(data), chunk_size=1)
    )
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
    actual = [(msr.sourceid, msr.samplecnt) for msr in MS3Record.iter_records(pathlib.Path(test_path3))]
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
    samples_buf = [list(msr.datasamples) for msr in MS3Record.from_buffer(data, unpack_data=True) if msr.numsamples > 0]
    samples_fl = [list(msr.datasamples) for msr in MS3Record.iter_records(io.BytesIO(data), unpack_data=True) if msr.numsamples > 0]
    assert samples_fl == samples_buf
