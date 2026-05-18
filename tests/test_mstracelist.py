import io
import math
import os
import warnings

import pytest

from pymseed import MiniSEEDError, MS3TraceList, sample_time, timestr2nstime

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")


def test_tracelist_read():
    # Read test data from test file into a trace list
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Fetch first traceID
    traceid = traces[0]

    assert traceid.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert traceid.pubversion == 4
    assert traceid.earliest == 1267253400019539000
    assert traceid.earliest_seconds == 1267253400.019539
    assert traceid.latest == 1267257599969538000
    assert traceid.latest_seconds == 1267257599.969538

    # Fetch first trace segment
    segment = traceid[0]

    assert segment.starttime == 1267253400019539000
    assert segment.starttime_seconds == 1267253400.019539
    assert segment.endtime == 1267257599969538000
    assert segment.endtime_seconds == 1267257599.969538
    assert segment.samprate == 20.0
    assert segment.samplecnt == 84000
    assert segment.numsamples == 84000
    assert segment.sampletype == "i"
    assert segment.sampletype == "i"

    # Data sample array tests
    data = segment.datasamples

    # Check first 6 samples
    assert data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

    # Check last 6 samples
    assert data[-6:].tolist() == [-929184, -928936, -928632, -928248, -927779, -927206]

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert foundid.pubversion == 4
    assert foundid.earliest == 1267253400019539000
    assert foundid.earliest_seconds == 1267253400.019539
    assert foundid.latest == 1267257599969538000
    assert foundid.latest_seconds == 1267257599.969538

    foundseg = foundid[0]

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]


def test_tracelist_read_buffer():
    # Read test data from test file into a buffer
    with open(test_path3, "rb") as fp:
        buffer = fp.read()

    # Read miniSEED data from buffer into a trace list
    traces = MS3TraceList.from_buffer(buffer, unpack_data=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Fetch first traceID
    traceid = traces[0]

    assert traceid.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert traceid.pubversion == 4
    assert traceid.earliest == 1267253400019539000
    assert traceid.earliest_seconds == 1267253400.019539
    assert traceid.latest == 1267257599969538000
    assert traceid.latest_seconds == 1267257599.969538

    # Fetch first trace segment
    segment = traceid[0]

    assert segment.starttime == 1267253400019539000
    assert segment.starttime_seconds == 1267253400.019539
    assert segment.endtime == 1267257599969538000
    assert segment.endtime_seconds == 1267257599.969538
    assert segment.samprate == 20.0
    assert segment.samplecnt == 84000
    assert segment.numsamples == 84000
    assert segment.sampletype == "i"
    assert segment.sampletype == "i"

    # Data sample array tests
    data = segment.datasamples

    # Check first 6 samples
    assert data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

    # Check last 6 samples
    assert data[-6:].tolist() == [-929184, -928936, -928632, -928248, -927779, -927206]

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert foundid.pubversion == 4
    assert foundid.earliest == 1267253400019539000
    assert foundid.earliest_seconds == 1267253400.019539
    assert foundid.latest == 1267257599969538000
    assert foundid.latest_seconds == 1267257599.969538

    foundseg = foundid[0]

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]


class _PackFreeTracker:
    """Wraps the cffi lib namespace, counting calls to mstl3_pack_free."""

    def __init__(self, real):
        self._real = real
        self.pack_free_count = 0

    def __getattr__(self, name):
        attr = getattr(self._real, name)
        if name == "mstl3_pack_free":
            tracker = self

            def wrapped(*args, **kwargs):
                tracker.pack_free_count += 1
                return attr(*args, **kwargs)

            return wrapped
        return attr


def _install_pack_free_tracker(monkeypatch) -> _PackFreeTracker:
    """Replace mstracelist.clibmseed with a tracker; return it for assertions."""
    from pymseed import mstracelist

    tracker = _PackFreeTracker(mstracelist.clibmseed)
    monkeypatch.setattr(mstracelist, "clibmseed", tracker)
    return tracker


def test_tracelist_generate_frees_packer_on_full_consumption(monkeypatch):
    """Baseline: exhausting the generator frees the packer exactly once."""
    tracker = _install_pack_free_tracker(monkeypatch)
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)
    for _ in traces.generate():
        pass
    assert tracker.pack_free_count == 1


def test_tracelist_generate_frees_packer_on_early_break(monkeypatch):
    """Breaking out of the generator after one record still frees the packer."""
    tracker = _install_pack_free_tracker(monkeypatch)
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)
    gen = traces.generate()
    next(gen)  # take exactly one record
    gen.close()  # simulates `break` / falling out of the for-loop
    assert tracker.pack_free_count == 1


def test_tracelist_generate_frees_packer_on_consumer_exception(monkeypatch):
    """An exception raised by the consumer mid-iteration still frees the packer."""
    tracker = _install_pack_free_tracker(monkeypatch)
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)

    class ConsumerError(Exception):
        pass

    with pytest.raises(ConsumerError):
        for i, _ in enumerate(traces.generate()):
            if i == 1:
                raise ConsumerError("simulated consumer failure")

    assert tracker.pack_free_count == 1


def test_tracelist_time_str_sentinels():
    """starttime_str/endtime_str on MS3TraceSeg and earliest_str/latest_str
    on MS3TraceID must return the libmseed sentinel strings ``"ERROR"`` and
    ``"UNSET"`` instead of falling through to ``nstime2timestr``, which would
    now raise. Mirrors MS3Record.starttime_str()'s contract."""
    from pymseed.clib import clibmseed

    traces = MS3TraceList.from_file(test_path3)
    traceid = next(iter(traces))
    seg = traceid[0]

    # Sanity: a real record yields a real ISO string for all four methods.
    assert seg.starttime_str().endswith("Z")
    assert seg.endtime_str().endswith("Z")
    assert traceid.earliest_str().endswith("Z")
    assert traceid.latest_str().endswith("Z")

    # Force sentinel values by writing into the underlying C structs.
    seg._seg.starttime = clibmseed.NSTUNSET
    seg._seg.endtime = clibmseed.NSTERROR
    traceid._id.earliest = clibmseed.NSTUNSET
    traceid._id.latest = clibmseed.NSTERROR

    assert seg.starttime_str() == "UNSET"
    assert seg.endtime_str() == "ERROR"
    assert traceid.earliest_str() == "UNSET"
    assert traceid.latest_str() == "ERROR"


def test_tracelist_sampletype_returns_none_when_unset():
    """MS3TraceSeg.sampletype must return None when the underlying C struct
    has no sample-type byte set (zero byte). The old truthiness check
    ``if self._seg.sampletype:`` was always True because CFFI char fields
    surface as a 1-byte bytes object and ``bool(b'\\x00')`` is True, so the
    None branch was unreachable and callers got the literal '\\x00' character
    instead of None."""
    # unpack_data=False leaves the segment's sampletype byte as 0 because no
    # decoding has happened yet.
    traces = MS3TraceList.from_file(test_path3, unpack_data=False)
    seg = next(iter(traces))[0]
    assert seg._seg.sampletype == b"\x00"  # contract sanity on the C side
    assert seg.sampletype is None

    # Once decoded, the property surfaces the actual ASCII code.
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)
    seg = next(iter(traces))[0]
    assert seg.sampletype == "i"


def test_tracelist_add_data_rejects_ambiguous_time_arguments():
    """add_data() documents the three start_time_* parameters as mutually
    exclusive; previously the implementation just let start_time_str win
    silently when multiple were passed. Enforce the exclusivity and keep
    the existing 'none-passed' error too."""
    traces = MS3TraceList()
    common = dict(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=[1, 2, 3],
        sample_type="i",
        sample_rate=20.0,
    )

    # None passed: pre-existing contract preserved.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(**common)

    # Two passed: previously silently accepted (string won), now rejected.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(
            **common,
            start_time_str="2023-01-01T00:00:00.000Z",
            start_time=1672531200_000000000,
        )

    # All three passed: also rejected.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(
            **common,
            start_time_str="2023-01-01T00:00:00.000Z",
            start_time=1672531200_000000000,
            start_time_seconds=1672531200.0,
        )

    # Exactly one passed: each form still works.
    for tkw in (
        {"start_time_str": "2023-01-01T00:00:00.000Z"},
        {"start_time": 1672531200_000000000},
        {"start_time_seconds": 1672531200.0},
    ):
        MS3TraceList().add_data(**common, **tkw)


def test_tracelist_unpack_recordlist_rejects_non_buffer():
    """unpack_recordlist() must raise ValueError on objects that don't expose
    the buffer protocol, not bubble up CFFI's TypeError. Also exercises an
    array.array (no .nbytes, has .itemsize) to keep the fallback branch
    covered."""
    import array

    traces = MS3TraceList.from_file(test_path3, record_list=True)
    traceid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")
    seg = traceid[0]

    class NotABuffer:
        pass

    with pytest.raises(ValueError, match="buffer protocol"):
        seg.unpack_recordlist(buffer=NotABuffer())

    # array.array has .itemsize but no .nbytes; the itemsize fallback must
    # compute the correct byte size so libmseed sees a buffer large enough
    # to hold all samples.
    arr = array.array("i", [0] * seg.samplecnt)
    count = seg.unpack_recordlist(buffer=arr)
    assert count == seg.samplecnt
    assert any(v != 0 for v in arr)  # buffer was actually written into


def test_tracelist_generate_removed_packed_deprecated_alias():
    """`generate(removed_packed=...)` is a typo'd alias for `remove_packed`.
    Keep accepting it for backward compatibility but emit DeprecationWarning
    eagerly and produce the same output as the canonical spelling."""

    def _populated_list() -> MS3TraceList:
        traces = MS3TraceList()
        traces.add_data(
            "FDSN:XX_STA__H_H_Z",
            [1, 2, 3, 4, 5],
            "i",
            100.0,
            start_time_str="2023-01-01T00:00:00.000Z",
        )
        return traces

    # Canonical spelling: silent.
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        records_new = list(_populated_list().generate(remove_packed=True))

    # Deprecated spelling: DeprecationWarning raised eagerly at the
    # generate() call (not lazily on first iteration), and the resulting
    # records are byte-identical to the canonical spelling.
    with pytest.warns(DeprecationWarning, match="removed_packed"):
        gen = _populated_list().generate(removed_packed=True)
    records_old = list(gen)
    assert records_old == records_new


def test_tracelist_add_file_does_not_retain_filename_buffer_without_record_list():
    """Without record_list=True no MS3RecordPtr entries reference the C
    filename buffer, so add_file() must not pin it on the trace list. The
    record_list=True path still needs to retain it because libmseed stores
    the pointer in MS3RecordPtr entries for later use (e.g. by
    unpack_recordlist())."""
    traces = MS3TraceList()
    assert len(traces._c_file_names) == 0

    # Re-using one MS3TraceList across many add_file() calls must not grow
    # the retained-buffer list when record_list=False.
    for _ in range(5):
        traces.add_file(test_path3)
    assert len(traces._c_file_names) == 0

    # With record_list=True each call must retain its filename buffer so the
    # libmseed-stored pointer remains valid for the lifetime of the records.
    traces_rl = MS3TraceList()
    traces_rl.add_file(test_path3, record_list=True)
    traces_rl.add_file(test_path3, record_list=True)
    assert len(traces_rl._c_file_names) == 2

    # And the retained pointer is still readable through the record list,
    # confirming the buffer wasn't prematurely freed.
    traceid = next(iter(traces_rl))
    seg = traceid[0]
    first_ptr = next(iter(seg.recordlist))
    assert first_ptr.filename is not None
    assert first_ptr.filename.endswith(os.path.basename(test_path3))


def test_tracelist_has_same_data_short_circuits_on_sampletype():
    """has_same_data() must consult sampletype before the byte-level
    memoryview comparison. memoryview equality is value-based across formats
    (e.g. ``memoryview(b"abc") == memoryview(array('i', [97,98,99]))`` is
    ``True``), so a text segment whose bytes happen to align with an int
    segment's samples would otherwise be reported as equivalent. Also pins
    that datasamples is not consulted at all when scalar metadata disagrees.
    """
    from unittest.mock import PropertyMock, patch

    from pymseed.mstracelist import MS3TraceSeg

    traces = MS3TraceList.from_file(test_path3, unpack_data=True)
    traceid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")
    seg = traceid[0]

    assert seg.has_same_data(seg) is True

    with (
        patch.object(MS3TraceSeg, "sampletype", new_callable=PropertyMock) as mock_sampletype,
        patch.object(MS3TraceSeg, "datasamples", new_callable=PropertyMock) as mock_datasamples,
    ):
        mock_sampletype.side_effect = ["i", "t"]
        assert seg.has_same_data(seg) is False
        assert mock_datasamples.call_count == 0


def test_tracelist_sample_size_type_requires_record_list():
    """sample_size_type needs record_list=True; absent it, raises a clear ValueError."""
    # Default construction does not retain a record list.
    traces = MS3TraceList.from_file(test_path3, unpack_data=False)
    traceid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")
    seg = traceid[0]

    assert seg.recordlist is None  # contract sanity check

    with pytest.raises(ValueError, match="No record list available"):
        _ = seg.sample_size_type


def test_tracelist_read_recordlist():
    traces = MS3TraceList(test_path3, unpack_data=False, record_list=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Search for a specific trace ID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    foundseg = foundid[0]

    assert foundseg.numsamples == 0

    # Get sample size and type from first trace ID
    (sample_size, sample_type) = foundseg.sample_size_type

    assert sample_size == 4
    assert sample_type == "i"

    # Unpack data samples using in-place buffer
    foundseg.unpack_recordlist()

    assert foundseg.numsamples == 84000

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]

    # Traverse the record list counting records and samples
    record_count = 0
    sample_count = 0
    for record_ptr in foundseg.recordlist.records():
        record_count += 1
        sample_count += record_ptr.record.samplecnt

    assert record_count == 386
    assert sample_count == 84000


def test_tracelist_slicing():
    traces = MS3TraceList(test_path3, unpack_data=True)

    assert len(traces) == 3

    # Test slicing (trace has 1 segment, so test valid slices)
    traceid = traces[0]

    assert len(traceid[0:1]) == 1
    assert traceid[0:1][0].starttime == 1267253400019539000

    # Test empty slice
    assert len(traceid[1:3]) == 0  # No segments at indices 1-2

    # Test full slice
    assert len(traceid[:]) == 1
    assert len(traceid) == 1

    # Test slicing (trace has 1 segment, so test valid slices)

    assert len(traces[0:1]) == 1
    assert len(traces[0:1][0]) == 1


def test_tracelist_numpy():
    np = pytest.importorskip("numpy")

    traces = MS3TraceList(test_path3, record_list=True)

    # Fetch first traceID
    traceid = traces[0]

    # Fetch first trace segment
    segment = traceid[0]

    # Unpack data samples from record list before accessing numpy data
    segment.unpack_recordlist()

    # Data sample array tests
    np_data = segment.np_datasamples

    assert np_data.dtype == np.int32

    assert np_data.shape == (84000,)

    # Check first 6 samples
    assert np.all(np_data[0:6] == [-502916, -502808, -502691, -502567, -502433, -502331])

    # Check last 6 samples
    assert np.all(np_data[-6:] == [-929184, -928936, -928632, -928248, -927779, -927206])

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    foundseg = foundid[0]

    # Unpack data samples from record list before accessing numpy data
    foundseg.unpack_recordlist()

    # Check first 6 samples
    assert np.all(
        foundseg.np_datasamples[0:6].tolist()
        == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]
    )

    # Check last 6 samples
    assert np.all(
        foundseg.np_datasamples[-6:].tolist()
        == [
            -165263,
            -162103,
            -159002,
            -155907,
            -152810,
            -149774,
        ]
    )


def test_tracelist_numpy_arrayfrom_recordlist():
    np = pytest.importorskip("numpy")

    with pytest.raises(ValueError):
        # Must specify record_list=True
        traces = MS3TraceList(test_path3)
        traceid = traces[0]
        segment = traceid[0]
        np_data = segment.create_numpy_array_from_recordlist()

    traces = MS3TraceList(test_path3, record_list=True)

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    foundseg = foundid[0]

    np_data = foundseg.create_numpy_array_from_recordlist()

    assert np_data.dtype == np.int32

    assert np_data.shape == (84000,)

    # Check first 6 samples
    assert np.all(
        np_data[0:6].tolist()
        == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]
    )

    # Check last 6 samples
    assert np.all(
        np_data[-6:].tolist()
        == [
            -165263,
            -162103,
            -159002,
            -155907,
            -152810,
            -149774,
        ]
    )


# A sine wave generator
def sine_generator(start_degree=0, yield_count=100, total=1000):
    """A generator returning a continuing sequence of sine values."""
    generated = 0
    while generated < total:
        chunk_size = min(yield_count, total - generated)

        # Yield a list of continuing sine values
        yield [
            int(math.sin(math.radians(x)) * 500)
            for x in range(start_degree, start_degree + chunk_size)
        ]

        start_degree += chunk_size
        generated += chunk_size


# A global record buffer
record_buffer = bytearray()


def record_handler(record, handler_data):
    """A callback function for MSTraceList.set_record_handler()
    Adds the record to a global buffer for testing
    """
    global record_buffer
    record_buffer.extend(bytes(record))


test_pack3 = os.path.join(test_dir, "data", "packtest_sine2000.mseed3")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_mstracelist_pack():
    # Create a new MSTraceList object
    traces = MS3TraceList()

    total_samples = 0
    total_records = 0
    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    format_version = 3
    max_record_length = 512

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

        (packed_samples, packed_records) = traces.pack(
            record_handler,
            flush_data=False,
            format_version=format_version,
            max_record_length=max_record_length,
        )

        total_samples += packed_samples
        total_records += packed_records

    (packed_samples, packed_records) = traces.pack(
        record_handler,
        format_version=format_version,
        max_record_length=max_record_length,
    )

    total_samples += packed_samples
    total_records += packed_records

    assert total_samples == 2000
    assert total_records == 5

    with open(test_pack3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3


def test_mstracelist_generate_rollingbuffer():
    """Test creation of miniSEED v3 records from a trace list using a rolling buffer.

    The rolling buffer usage removes packed data from the trace list after each
    pack, data is then added and packed in later calls.  After the final pack to
    flush any remaining data, the trace list is empty.
    """
    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    format_version = 3
    max_record_length = 512

    # Test creation of a miniSEED v3 records
    record_buffer = b""
    record_count = 0

    # Mimic generating miniSEED records from a continuous data stream by adding
    # data to the trace list and generating filled records
    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

        # Generate filled records during regular data flow
        for record in traces.generate(
            max_record_length=max_record_length,
            format_version=format_version,
            flush_data=False,
            flush_idle_seconds=10,
            remove_packed=True,
        ):
            record_buffer += record
            record_count += 1

    # Final record creation to flush any remaining data
    for record in traces.generate(
        max_record_length=max_record_length,
        format_version=format_version,
        flush_data=True,
        remove_packed=True,
    ):
        record_buffer += record
        record_count += 1

    assert record_count == 5
    assert len(record_buffer) == 2471

    with open(test_pack3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3

    assert len(traces) == 0  # Trace list should be empty after final pack


test_pack3_x3 = os.path.join(test_dir, "data", "packtest_sine500x3.mseed3")
test_pack2_x3 = os.path.join(test_dir, "data", "packtest_sine500x3.mseed2")


def test_mstracelist_generate():
    """Test creation of miniSEED v3 and v2 records from a trace list.

    The same trace list is used for both versions to ensure that the data is
    maintained in the trace list after packing.
    """

    # A sine wave of 500 samples
    sine_500 = [int(math.sin(math.radians(x)) * 500) for x in range(0, 500)]

    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    max_record_length = 512

    # Add 3 traces to the list
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_1",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_2",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_3",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )

    # Test creation of a miniSEED v3 records
    record_buffer = b""
    record_count = 0

    for record in traces.generate(max_record_length=max_record_length, format_version=3):
        record_buffer += record
        record_count += 1

    assert record_count == 6
    assert len(record_buffer) == 2082

    with open(test_pack3_x3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3

    # Test creation of a miniSEED v2 records
    record_buffer = b""
    record_count = 0

    for record in traces.generate(max_record_length=max_record_length, format_version=2):
        record_buffer += record
        record_count += 1

    assert record_count == 6
    assert len(record_buffer) == 3072

    with open(test_pack2_x3, "rb") as f:
        data_v2 = f.read()
        assert record_buffer == data_v2

    assert len(traces) == 3  # Traces should remain in the list
    assert traces[0][0].numsamples == 500
    assert traces[1][0].numsamples == 500
    assert traces[2][0].numsamples == 500


test_pack2 = os.path.join(test_dir, "data", "packtest_sine2000.mseed2")


def test_mstracelist_to_file(tmp_path):
    """Test MS3TraceList.to_file() method using pytest's tmp_path fixture."""
    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

    # Use pytest's tmp_path fixture to create a temporary file
    temp_file = tmp_path / "test_output.mseed3"

    # Write using to_file method
    records_written = traces.to_file(
        str(temp_file), overwrite=True, format_version=2, max_record_length=512
    )

    # Verify number of records written
    assert records_written == 5

    # Verify file was created and has content
    assert temp_file.exists()
    assert temp_file.stat().st_size > 0

    # Compare created file to reference file
    with open(test_pack2, "rb") as f:
        reference_data = f.read()
        with open(temp_file, "rb") as f:
            test_data = f.read()
            assert reference_data == test_data


def test_mstracelist_nosuchfile():
    with pytest.raises(MiniSEEDError):
        traces = MS3TraceList("NOSUCHFILE")


# ---------------------------------------------------------------------------
# Selection filter tests (sourceid / starttime / endtime)
# ---------------------------------------------------------------------------


def test_tracelist_file_sourceid_exact():
    """Exact source ID filter returns only that channel."""
    traces = MS3TraceList.from_file(test_path3, sourceid="FDSN:IU_COLA_00_B_H_Z")
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"


def test_tracelist_file_sourceid_glob():
    """Glob source ID filter (trailing wildcard) returns matching channels."""
    traces = MS3TraceList.from_file(test_path3, sourceid="FDSN:IU_COLA_00_B_H_*")
    assert len(traces) == 3


def test_tracelist_file_sourceid_no_match():
    """Non-matching source ID filter returns empty trace list."""
    traces = MS3TraceList.from_file(test_path3, sourceid="FDSN:XX_NONE_*")
    assert len(traces) == 0


def test_tracelist_file_time_window():
    """Time-window filter returns fewer samples than unfiltered read."""
    traces_full = MS3TraceList.from_file(test_path3)
    traces_windowed = MS3TraceList.from_file(
        test_path3,
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces_windowed) == 3
    # Each windowed segment must have fewer samples than the full segment
    for tid_full, tid_windowed in zip(traces_full, traces_windowed, strict=True):
        assert tid_windowed[0].samplecnt < tid_full[0].samplecnt


def test_tracelist_file_sourceid_and_time_window():
    """Combined source ID and time-window filter."""
    traces = MS3TraceList.from_file(
        test_path3,
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert traces[0][0].samplecnt == 36080


def test_tracelist_file_invalid_starttime():
    """Invalid starttime string raises ValueError."""
    with pytest.raises(ValueError):
        MS3TraceList.from_file(test_path3, starttime="not-a-time")


def test_tracelist_file_invalid_endtime():
    """Invalid endtime string raises ValueError."""
    with pytest.raises(ValueError):
        MS3TraceList.from_file(test_path3, endtime="not-a-time")


def test_tracelist_buffer_sourceid_exact():
    """Buffer path: exact source ID filter returns only that channel."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_buffer(buf, sourceid="FDSN:IU_COLA_00_B_H_Z")
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"


def test_tracelist_buffer_time_window():
    """Buffer path: time-window filter returns fewer samples than unfiltered."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces_full = MS3TraceList.from_buffer(buf)
    traces_windowed = MS3TraceList.from_buffer(
        buf,
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces_windowed) == 3
    for tid_full, tid_windowed in zip(traces_full, traces_windowed, strict=True):
        assert tid_windowed[0].samplecnt < tid_full[0].samplecnt


def test_tracelist_buffer_sourceid_and_time_window():
    """Buffer path: combined source ID and time-window filter."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_buffer(
        buf,
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert traces[0][0].samplecnt == 36080


def test_tracelist_buffer_invalid_starttime():
    """Buffer path: invalid starttime string raises ValueError."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    with pytest.raises(ValueError):
        MS3TraceList.from_buffer(buf, starttime="not-a-time")


def test_tracelist_buffer_invalid_endtime():
    """Buffer path: invalid endtime string raises ValueError."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    with pytest.raises(ValueError):
        MS3TraceList.from_buffer(buf, endtime="not-a-time")


# ---------------------------------------------------------------------------
# Filelike (streaming) tests
# ---------------------------------------------------------------------------


def test_tracelist_filelike_basic():
    """Filelike read produces the same trace list as the buffer/file paths."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), unpack_data=True)
    assert len(traces) == 3
    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]
    # Per-segment sanity check
    for tid in traces:
        seg = tid[0]
        assert seg.samplecnt == 84000
        assert seg.numsamples == 84000
        assert seg.samprate == 20.0


def test_tracelist_filelike_matches_buffer_path():
    """Streaming filelike yields the same sample counts as add_buffer."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces_buffer = MS3TraceList.from_buffer(buf)
    traces_filelike = MS3TraceList.from_filelike(io.BytesIO(buf))
    assert len(traces_buffer) == len(traces_filelike)
    for tid_b, tid_f in zip(traces_buffer, traces_filelike, strict=True):
        assert tid_b.sourceid == tid_f.sourceid
        assert tid_b[0].samplecnt == tid_f[0].samplecnt
        assert tid_b[0].starttime == tid_f[0].starttime
        assert tid_b[0].endtime == tid_f[0].endtime


def test_tracelist_filelike_sourceid_filter():
    """Filelike path: exact source ID filter returns only that channel."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), sourceid="FDSN:IU_COLA_00_B_H_Z")
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"


def test_tracelist_filelike_glob_filter():
    """Filelike path: glob source ID filter."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), sourceid="FDSN:IU_COLA_00_B_H_*")
    assert len(traces) == 3


def test_tracelist_filelike_time_window():
    """Filelike path: time-window filter narrows results."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces_full = MS3TraceList.from_filelike(io.BytesIO(buf))
    traces_windowed = MS3TraceList.from_filelike(
        io.BytesIO(buf),
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces_windowed) == 3
    for tid_full, tid_windowed in zip(traces_full, traces_windowed, strict=True):
        assert tid_windowed[0].samplecnt < tid_full[0].samplecnt


def test_tracelist_filelike_sourceid_and_time_window():
    """Filelike path: combined source ID and time-window filter matches buffer path."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(
        io.BytesIO(buf),
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime="2010-02-27T07:00:00Z",
        endtime="2010-02-27T07:30:00Z",
    )
    assert len(traces) == 1
    assert traces[0].sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert traces[0][0].samplecnt == 36080


def test_tracelist_filelike_no_match():
    """Filelike path: non-matching source ID filter returns empty trace list."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), sourceid="FDSN:XX_NONE_*")
    assert len(traces) == 0


def test_tracelist_filelike_invalid_starttime():
    """Filelike path: invalid starttime raises ValueError."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    with pytest.raises(ValueError):
        MS3TraceList.from_filelike(io.BytesIO(buf), starttime="not-a-time")


def test_tracelist_filelike_invalid_endtime():
    """Filelike path: invalid endtime raises ValueError."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    with pytest.raises(ValueError):
        MS3TraceList.from_filelike(io.BytesIO(buf), endtime="not-a-time")


def test_tracelist_filelike_record_list_metadata():
    """Filelike + record_list=True populates per-record metadata."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), record_list=True)
    assert len(traces) == 3
    for tid in traces:
        seg = tid[0]
        rl = seg.recordlist
        assert rl is not None
        # Each record's metadata should be queryable
        assert len(rl) > 0
        first = rl[0]
        assert first.record.sourceid == tid.sourceid
        assert first.record.reclen > 0


def test_tracelist_filelike_record_list_unpack_fails_cleanly():
    """Filelike + record_list=True: unpack_recordlist() raises rather than crashing."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), record_list=True)
    seg = traces[0][0]
    with pytest.raises(MiniSEEDError):
        seg.unpack_recordlist()


def test_tracelist_filelike_unpack_data_and_record_list():
    """Filelike + unpack_data=True + record_list=True yields samples and metadata."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), unpack_data=True, record_list=True)
    assert len(traces) == 3
    for tid in traces:
        seg = tid[0]
        assert seg.numsamples == 84000
        assert seg.samplecnt == 84000
        assert seg.recordlist is not None
        assert len(seg.recordlist) > 0


def test_tracelist_add_filelike_appends():
    """add_filelike() appends to an existing trace list."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList()
    traces.add_filelike(io.BytesIO(buf))
    traces.add_filelike(io.BytesIO(buf))
    # Same three sourceids, each with two duplicate segments
    assert len(traces) == 3
    for tid in traces:
        assert len(tid) == 2


def test_tracelist_filelike_small_chunk_size():
    """Tiny chunk_size still produces correct results (exercises sliding-buffer path)."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), chunk_size=128)
    assert len(traces) == 3
    for tid in traces:
        assert tid[0].samplecnt == 84000
