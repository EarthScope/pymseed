import gc
import io
import os
import sys
import types
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest

import pymseed.msrecord_batch
from pymseed import MS3Record, MS3RecordBatchReader
from pymseed.clib import clibmseed
from pymseed.exceptions import MiniSEEDError

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")
test_path2 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")
test_60sec = os.path.join(test_dir, "data", "testdata-60sec-period.mseed3")


def read(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


# Arrow export creates PyCapsules with CPython's C API
cpython_only = pytest.mark.skipif(
    sys.implementation.name != "cpython", reason="Arrow export requires CPython"
)

# miniSEED 3 and 2 records of three channels, then 60-second period records
MIXED = read(test_path3) + read(test_path2) + read(test_60sec)


def expected_records(data: bytes) -> list[tuple]:
    return [
        (
            r.sourceid,
            r.starttime,
            r.endtime,
            r.samprate,
            r.samplecnt,
            r.encoding,
            r.pubversion,
            r.formatversion,
            r.record,
        )
        for r in MS3Record.from_buffer(data)
    ]


def batch_records(batches) -> list[tuple]:
    return [
        (
            batch.sourceid(i),
            batch.starttimes[i],
            batch.endtimes[i],
            batch.samprates[i],
            batch.samplecnts[i],
            batch.encodings[i],
            batch.pubversions[i],
            batch.formatversions[i],
            batch.record(i),
        )
        for batch in batches
        for i in range(len(batch))
    ]


@pytest.mark.parametrize(
    "read_batches",
    [
        pytest.param(lambda data: MS3RecordBatchReader.from_buffer(data), id="buffer"),
        pytest.param(
            lambda data: MS3RecordBatchReader.from_buffer(bytearray(data)), id="bytearray"
        ),
        pytest.param(
            lambda data: MS3RecordBatchReader.from_filelike(io.BytesIO(data)), id="filelike"
        ),
        # Records split across blocks
        pytest.param(
            lambda data: MS3RecordBatchReader.from_filelike(io.BytesIO(data), block_size=1000),
            id="filelike-small-blocks",
        ),
        # Records larger than a block
        pytest.param(
            lambda data: MS3RecordBatchReader.from_filelike(io.BytesIO(data), block_size=100),
            id="filelike-tiny-blocks",
        ),
        pytest.param(
            lambda data: MS3RecordBatchReader.from_buffer(data, batch_size=7), id="small-batches"
        ),
    ],
)
def test_batches_hold_the_same_records_as_ms3record(read_batches):
    expected = expected_records(MIXED)
    assert len({r[0] for r in expected}) > 1
    assert {r[7] for r in expected} == {2, 3}

    assert batch_records(read_batches(MIXED)) == expected


def test_from_file(tmp_path):
    path = tmp_path / "mixed.mseed"
    path.write_bytes(MIXED)

    assert batch_records(MS3RecordBatchReader.from_file(path, block_size=4096)) == expected_records(
        MIXED
    )


def test_batch_size_bounds_batches():
    sizes = [len(b) for b in MS3RecordBatchReader.from_buffer(MIXED, batch_size=10)]

    assert sum(sizes) == len(expected_records(MIXED))
    assert max(sizes) == 10


def test_batch_ends_when_its_source_id_table_is_full(monkeypatch):
    monkeypatch.setattr(pymseed.msrecord_batch, "DEFAULT_MAX_SOURCEIDS", 1)

    batches = list(MS3RecordBatchReader.from_buffer(MIXED))

    assert all(len(b.sourceids) == 1 for b in batches)
    assert batch_records(batches) == expected_records(MIXED)


def test_records_and_offsets_cover_each_batch():
    for batch in MS3RecordBatchReader.from_buffer(MIXED, batch_size=10):
        offsets = batch.record_offsets
        assert len(offsets) == len(batch) + 1
        assert offsets[0] == 0
        assert len(batch.records) == offsets[-1]
        assert bytes(batch.records[offsets[1] : offsets[2]]) == batch.record(1)


def test_columns_outlive_their_batch():
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))
    expected = list(batch.starttimes)
    starttimes = batch.starttimes
    del batch
    gc.collect()

    assert list(starttimes) == expected


def test_columns_as_numpy_arrays():
    np = pytest.importorskip("numpy")
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    starttimes = np.frombuffer(batch.starttimes, dtype=np.int64)

    assert starttimes.tolist() == list(batch.starttimes)


@cpython_only
def test_to_arrow():
    pa = pytest.importorskip("pyarrow")
    expected = expected_records(MIXED)

    batches = [b.to_arrow() for b in MS3RecordBatchReader.from_buffer(MIXED, batch_size=100)]
    gc.collect()  # the Arrow arrays must keep the C arrays and buffer alive
    table = pa.Table.from_batches(batches)

    assert table.schema.names == [
        "sourceid",
        "starttime",
        "endtime",
        "samprate",
        "samplecnt",
        "encoding",
        "pubversion",
        "formatversion",
        "record",
    ]
    assert table["sourceid"].type == pa.dictionary(pa.int32(), pa.string())
    assert table["starttime"].type == pa.timestamp("ns", tz="UTC")
    rows = list(
        zip(
            table["sourceid"].cast(pa.string()).to_pylist(),
            table["starttime"].cast(pa.int64()).to_pylist(),
            table["endtime"].cast(pa.int64()).to_pylist(),
            table["samprate"].to_pylist(),
            table["samplecnt"].to_pylist(),
            table["encoding"].to_pylist(),
            table["pubversion"].to_pylist(),
            table["formatversion"].to_pylist(),
            table["record"].to_pylist(),
            strict=True,
        )
    )
    assert rows == expected


@cpython_only
def test_to_arrow_does_not_copy_records():
    pa = pytest.importorskip("pyarrow")
    data = pa.py_buffer(MIXED)
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    record_data = batch.to_arrow()["record"].buffers()[2]

    assert record_data.address == data.address


def test_empty_input_yields_nothing():
    assert list(MS3RecordBatchReader.from_buffer(b"")) == []
    assert list(MS3RecordBatchReader.from_filelike(io.BytesIO(b""))) == []


@pytest.mark.parametrize("block_size", [100, 1000, 1 << 20])
def test_truncated_input_raises(block_size):
    data = read(test_path3)[:-10]

    with pytest.raises(MiniSEEDError) as excinfo:
        list(MS3RecordBatchReader.from_filelike(io.BytesIO(data), block_size=block_size))

    assert excinfo.value.status_code == clibmseed.MS_ENDOFFILE


def test_input_too_short_for_a_record_is_not_miniseed():
    with pytest.raises(MiniSEEDError) as excinfo:
        list(MS3RecordBatchReader.from_buffer(b"too short"))

    assert excinfo.value.status_code == clibmseed.MS_NOTSEED


def test_data_that_is_not_miniseed_raises():
    with pytest.raises(MiniSEEDError) as excinfo:
        list(MS3RecordBatchReader.from_buffer(b"x" * 4096))

    assert excinfo.value.status_code == clibmseed.MS_NOTSEED


def test_invalid_crc_raises_unless_not_validated():
    data = bytearray(read(test_path3))
    data[100] ^= 0xFF  # inside the first record's data payload

    with pytest.raises(MiniSEEDError) as excinfo:
        list(MS3RecordBatchReader.from_buffer(data))
    assert excinfo.value.status_code == clibmseed.MS_INVALIDCRC

    assert len(batch_records(MS3RecordBatchReader.from_buffer(data, validate_crc=False))) == len(
        expected_records(read(test_path3))
    )


def test_invalid_arguments():
    with pytest.raises(ValueError):
        list(MS3RecordBatchReader.from_buffer(MIXED, batch_size=0))
    with pytest.raises(ValueError):
        list(MS3RecordBatchReader.from_filelike(io.BytesIO(MIXED), block_size=0))


# Arrow PyCapsule interface


def live_exports() -> tuple[int, int]:
    """Batch arrays and streams still held by Arrow consumers."""
    gc.collect()
    return len(pymseed.msrecord_batch._exports), len(pymseed.msrecord_batch._streams)


@cpython_only
def test_capsules_follow_the_arrow_pycapsule_interface():
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    schema, array = batch.__arrow_c_array__()

    assert type(schema).__name__ == "PyCapsule"
    assert type(array).__name__ == "PyCapsule"
    assert "arrow_schema" in repr(schema)
    assert "arrow_array" in repr(array)
    assert "arrow_array_stream" in repr(batch.__arrow_c_stream__())


@cpython_only
def test_unconsumed_capsules_release_the_batch():
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    capsules = (batch.__arrow_c_array__(), batch.__arrow_c_stream__())
    assert live_exports() == (1, 1)
    del capsules

    assert live_exports() == (0, 0)


@cpython_only
def test_arrow_arrays_keep_their_batch_alive():
    pa = pytest.importorskip("pyarrow")
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))
    expected = batch.record(0)
    ref = weakref.ref(batch)

    arrow = pa.record_batch(batch)
    del batch
    gc.collect()
    assert ref() is not None
    assert arrow["record"][0].as_py() == expected

    del arrow
    gc.collect()
    assert ref() is None
    assert live_exports() == (0, 0)


@cpython_only
def test_arrow_schema():
    pytest.importorskip("pyarrow")
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    schema = batch.to_arrow().schema

    assert schema == MS3RecordBatchReader.from_buffer(MIXED).to_arrow().schema
    assert not any(field.nullable for field in schema)
    # Data, not schema-like: the schema comes with the array and stream exports
    assert not hasattr(batch, "__arrow_c_schema__")
    assert not hasattr(MS3RecordBatchReader.from_buffer(MIXED), "__arrow_c_schema__")


@cpython_only
def test_arrays_released_on_other_threads():
    pytest.importorskip("pyarrow")
    arrays = [b.to_arrow() for b in MS3RecordBatchReader.from_buffer(MIXED, batch_size=10)]
    assert live_exports()[0] == len(arrays)

    def release(i):
        arrays[i] = None
        gc.collect()

    with ThreadPoolExecutor(4) as pool:
        list(pool.map(release, range(len(arrays))))

    assert live_exports() == (0, 0)


@cpython_only
def test_reader_as_arrow_stream():
    pa = pytest.importorskip("pyarrow")
    expected = expected_records(MIXED)

    reader = MS3RecordBatchReader.from_filelike(io.BytesIO(MIXED), block_size=1000, batch_size=50)
    table = pa.RecordBatchReader.from_stream(reader).read_all()

    assert table.num_rows == len(expected)
    assert table["record"].to_pylist() == [r[-1] for r in expected]
    assert table["sourceid"].cast(pa.string()).to_pylist() == [r[0] for r in expected]
    assert live_exports() == (len(table.to_batches()), 0)
    del table
    assert live_exports() == (0, 0)


@cpython_only
def test_stream_continues_where_iteration_left_off():
    pytest.importorskip("pyarrow")
    reader = MS3RecordBatchReader.from_buffer(MIXED, batch_size=10)
    first = next(reader)

    rest = reader.to_arrow().read_all()

    assert len(first) + rest.num_rows == len(expected_records(MIXED))


@cpython_only
def test_stream_error_reaches_the_consumer():
    pa = pytest.importorskip("pyarrow")
    reader = MS3RecordBatchReader.from_filelike(io.BytesIO(MIXED[:-10]), block_size=1000)

    with pytest.raises(OSError, match="MiniSEEDError: Stream ends with"):
        pa.RecordBatchReader.from_stream(reader).read_all()

    assert live_exports() == (0, 0)


def test_from_file_reader_closes_its_file(tmp_path):
    path = tmp_path / "mixed.mseed"
    path.write_bytes(MIXED)

    with MS3RecordBatchReader.from_file(path, block_size=1000) as reader:
        next(reader)

    with pytest.raises(StopIteration):
        next(reader)


@cpython_only
def test_polars_reads_batches_and_streams():
    pl = pytest.importorskip("polars")
    n = len(expected_records(MIXED))

    assert pl.DataFrame(next(MS3RecordBatchReader.from_buffer(MIXED))).height > 0
    assert pl.DataFrame(MS3RecordBatchReader.from_buffer(MIXED, batch_size=50)).height == n


@cpython_only
def test_duckdb_reads_streams():
    duckdb = pytest.importorskip("duckdb")
    pytest.importorskip("pytz")  # needed by DuckDB for timestamps with time zones
    reader = MS3RecordBatchReader.from_buffer(MIXED, batch_size=50)  # noqa: F841 (read by name in the query)

    rows = duckdb.sql("select sourceid::varchar, count(*) from reader group by 1").fetchall()

    expected = expected_records(MIXED)
    assert dict(rows) == {s: sum(r[0] == s for r in expected) for s in {r[0] for r in expected}}


def test_to_arrow_without_pyarrow_explains_what_to_install(monkeypatch):
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))
    monkeypatch.setitem(sys.modules, "pyarrow", None)  # import fails

    with pytest.raises(ImportError, match=r"requires pyarrow 14 or later"):
        batch.to_arrow()
    with pytest.raises(ImportError, match=r"requires pyarrow 14 or later"):
        MS3RecordBatchReader.from_buffer(MIXED).to_arrow()


def test_to_arrow_with_old_pyarrow_explains_what_to_install(monkeypatch):
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))
    monkeypatch.setitem(sys.modules, "pyarrow", types.SimpleNamespace(__version__="13.0.0"))

    with pytest.raises(ImportError, match=r"found 13\.0\.0"):
        batch.to_arrow()


@pytest.mark.skipif(sys.implementation.name == "cpython", reason="CPython supports Arrow export")
def test_arrow_export_outside_cpython_raises():
    batch = next(MS3RecordBatchReader.from_buffer(MIXED))

    with pytest.raises(NotImplementedError, match="requires CPython"):
        batch.__arrow_c_array__()
    with pytest.raises(NotImplementedError, match="requires CPython"):
        MS3RecordBatchReader.from_buffer(MIXED).__arrow_c_stream__()
