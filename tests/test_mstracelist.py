import array
import gc
import io
import math
import os
import time
import warnings
import weakref

import pytest

from pymseed import (
    NSTMODULUS,
    MiniSEEDError,
    MS3RecordValidator,
    MS3TraceList,
    sample_time,
    system_time,
    timestr2nstime,
)

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


def test_tracelist_segment_update_time():
    """update_time exposes the value flush_idle_seconds is measured against, so
    a rolling buffer can see how long a segment has been idle."""
    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=[1, 2, 3],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2023-01-01T00:00:00Z",
    )
    segment = traces[0][0]

    first_update = segment.update_time
    assert first_update is not None
    assert abs(system_time() - first_update) < 5 * NSTMODULUS
    assert segment.update_time_seconds == pytest.approx(first_update / NSTMODULUS)

    # The update time tracks the segment, not the data times it holds
    assert first_update > segment.endtime

    time.sleep(0.01)
    traces.add_data(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=[4, 5, 6],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2023-01-01T00:00:00.03Z",
    )
    assert traces[0][0].update_time > first_update


def test_tracelist_segment_update_time_unrecorded():
    """Segments built without an update time report None rather than reading
    whatever else a private pointer might hold."""
    with open(test_path3, "rb") as f:
        errors, traces = MS3RecordValidator.from_buffer(f.read()).validate()

    assert errors == []
    segment = traces[0][0]
    assert segment.update_time is None
    assert segment.update_time_seconds is None


def test_tracelist_read_buffer_itemsize_views():
    """len() on a buffer-protocol object with itemsize > 1 is the element count,
    not the byte count. Passing it to libmseed as a byte length made add_buffer()
    read only 1/itemsize of the data and silently return a short trace list."""
    with open(test_path3, "rb") as fp:
        data = fp.read()

    assert len(data) % 2 == 0, "test data must divide evenly into 2-byte items"

    reference = MS3TraceList.from_buffer(data)
    expected = (len(reference), sum(seg.samplecnt for tid in reference for seg in tid))
    assert expected[0] == 3

    views: list[tuple[str, object]] = [
        ("memoryview", memoryview(data)),
        ("memoryview.cast('H')", memoryview(data).cast("H")),
        ("array('H')", array.array("H", data)),
    ]

    np = pytest.importorskip("numpy", reason="numpy views are the common case")
    views.append(("numpy int16", np.frombuffer(data, dtype=np.int16)))

    for label, view in views:
        traces = MS3TraceList.from_buffer(view)
        got = (len(traces), sum(seg.samplecnt for tid in traces for seg in tid))
        assert got == expected, f"{label}: read {got}, expected {expected}"


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

    # Bind the generator and close it explicitly in finally so cleanup is
    # deterministic across CPython (refcount-driven finalization) and PyPy
    # (tracing GC; the unnamed generator would not finalize until the next GC).
    gen = traces.generate()
    try:
        with pytest.raises(ConsumerError):
            for i, _ in enumerate(gen):
                if i == 1:
                    raise ConsumerError("simulated consumer failure")
    finally:
        gen.close()

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


def test_tracelist_add_data_rejects_multidimensional():
    """add_data() shares MS3Record.with_datasamples(), so a multi-dimensional
    array must be rejected here too rather than adding only the first row."""
    np = pytest.importorskip("numpy")

    traces = MS3TraceList()
    common = {
        "sourceid": "FDSN:XX_STA__B_H_Z",
        "sample_type": "i",
        "sample_rate": 20.0,
        "starttime_str": "2023-01-01T00:00:00.000Z",
    }

    with pytest.raises(ValueError, match="one-dimensional"):
        traces.add_data(data_samples=np.arange(12, dtype=np.int32).reshape(3, 4), **common)

    traces.add_data(data_samples=np.arange(12, dtype=np.int32), **common)
    assert sum(seg.samplecnt for tid in traces for seg in tid) == 12


def test_tracelist_add_data_rejects_ambiguous_time_arguments():
    """add_data() documents the three starttime_* parameters as mutually
    exclusive; previously the implementation just let starttime_str win
    silently when multiple were passed. Enforce the exclusivity and keep
    the existing 'none-passed' error too."""
    traces = MS3TraceList()
    common = {
        "sourceid": "FDSN:XX_STA__B_H_Z",
        "data_samples": [1, 2, 3],
        "sample_type": "i",
        "sample_rate": 20.0,
    }

    # None passed: pre-existing contract preserved.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(**common)

    # Two passed: previously silently accepted (string won), now rejected.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(
            **common,
            starttime_str="2023-01-01T00:00:00.000Z",
            starttime=1672531200_000000000,
        )

    # All three passed: also rejected.
    with pytest.raises(ValueError, match="exactly one of"):
        traces.add_data(
            **common,
            starttime_str="2023-01-01T00:00:00.000Z",
            starttime=1672531200_000000000,
            starttime_seconds=1672531200.0,
        )

    # A canonical name plus its own deprecated alias is ambiguous, not a
    # silent override.
    for canonical, alias in (
        ({"starttime_str": "2023-01-01T00:00:00.000Z"}, {"start_time_str": "2023-01-02T00:00:00Z"}),
        ({"starttime": 1672531200_000000000}, {"start_time": 1672617600_000000000}),
        ({"starttime_seconds": 1672531200.0}, {"start_time_seconds": 1672617600.0}),
    ):
        with pytest.raises(ValueError, match="exactly one of"):
            MS3TraceList().add_data(**common, **canonical, **alias)

    # Mixing a canonical name with a *different* slot's alias is also rejected.
    with pytest.raises(ValueError, match="exactly one of"):
        MS3TraceList().add_data(
            **common,
            starttime_str="2023-01-01T00:00:00.000Z",
            start_time=1672531200_000000000,
        )

    # Exactly one passed: each form still works.
    for tkw in (
        {"starttime_str": "2023-01-01T00:00:00.000Z"},
        {"starttime": 1672531200_000000000},
        {"starttime_seconds": 1672531200.0},
    ):
        MS3TraceList().add_data(**common, **tkw)


def test_tracelist_add_data_start_time_deprecated_aliases():
    """`start_time_str`/`start_time`/`start_time_seconds` are deprecated aliases
    for `starttime_str`/`starttime`/`starttime_seconds`. Keep accepting them for
    backward compatibility, emit a DeprecationWarning naming the replacement, and
    produce results identical to the canonical spelling."""
    common = {
        "sourceid": "FDSN:XX_STA__B_H_Z",
        "data_samples": [1, 2, 3],
        "sample_type": "i",
        "sample_rate": 20.0,
    }
    pairs = (
        ("starttime_str", "start_time_str", "2023-01-01T00:00:00.000Z"),
        ("starttime", "start_time", 1672531200_000000000),
        ("starttime_seconds", "start_time_seconds", 1672531200.0),
    )

    for canonical, alias, value in pairs:
        # Canonical spelling: silent.
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            new_list = MS3TraceList()
            new_list.add_data(**common, **{canonical: value})

        # Deprecated spelling: warns, and names both the alias and replacement.
        with pytest.warns(DeprecationWarning, match=f"'{alias}' is a deprecated alias") as record:
            old_list = MS3TraceList()
            old_list.add_data(**common, **{alias: value})
        assert f"use '{canonical}'" in str(record[0].message)

        # Same resulting data either way.
        new_seg = next(iter(new_list))[0]
        old_seg = next(iter(old_list))[0]
        assert new_seg.starttime == old_seg.starttime
        assert new_seg.samplecnt == old_seg.samplecnt
        assert new_seg.has_same_data(old_seg)


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


def test_tracelist_unpack_recordlist_rejects_readonly_buffer():
    """libmseed memcpys the decoded samples into the supplied buffer, so a
    read-only one must be refused. Without require_writable=True, CFFI handed
    out the address of immutable Python storage and the C code wrote straight
    into it -- corrupting a `bytes` object with no error at all."""
    traces = MS3TraceList.from_file(test_path3, record_list=True)

    def fresh_segment():
        return MS3TraceList.from_file(test_path3, record_list=True).get_traceid(
            "FDSN:IU_COLA_00_B_H_Z"
        )[0]

    nbytes = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")[0].samplecnt * 4

    immutable = bytes(nbytes)
    with pytest.raises(BufferError, match="Cannot unpack into the provided buffer"):
        fresh_segment().unpack_recordlist(buffer=immutable)

    # The bytes object must be left untouched, not written through.
    assert immutable == bytes(nbytes)

    with pytest.raises(BufferError, match="Cannot unpack into the provided buffer"):
        fresh_segment().unpack_recordlist(buffer=memoryview(bytes(nbytes)))

    # A writable buffer of the same size still works.
    writable = bytearray(nbytes)
    seg = fresh_segment()
    assert seg.unpack_recordlist(buffer=writable) == seg.samplecnt
    assert any(writable)


def test_tracelist_unpack_recordlist_rejects_unwritable_numpy():
    """numpy refuses the buffer export itself, raising ValueError rather than
    BufferError; both must surface as the same error for the caller."""
    np = pytest.importorskip("numpy")

    def fresh_segment():
        return MS3TraceList.from_file(test_path3, record_list=True).get_traceid(
            "FDSN:IU_COLA_00_B_H_Z"
        )[0]

    seg = fresh_segment()

    readonly = np.zeros(seg.samplecnt, dtype=np.int32)
    readonly.setflags(write=False)
    with pytest.raises(BufferError, match="read-only"):
        fresh_segment().unpack_recordlist(buffer=readonly)

    # Writable but strided: libmseed would write a contiguous block over the
    # whole span, not every other element.
    strided = np.zeros(seg.samplecnt * 2, dtype=np.int32)[::2]
    with pytest.raises(BufferError, match="contiguous"):
        fresh_segment().unpack_recordlist(buffer=strided)


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
            starttime_str="2023-01-01T00:00:00.000Z",
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


def test_tracelist_add_file_accepts_pathlike():
    # pathlib.Path (and any os.PathLike) must be accepted by add_file/__init__,
    # not crash on .encode() like raw bytes/None used to.
    import pathlib

    p = pathlib.Path(test_path3)
    traces = MS3TraceList(file_name=p)
    assert len(traces) > 0

    traces2 = MS3TraceList()
    traces2.add_file(p)
    assert len(traces2) > 0


def test_tracelist_add_file_rejects_invalid_filename_types():
    # bytes/None/list/etc. must fail fast with TypeError, not AttributeError
    # buried inside ffi.new("char[]", ...encode()).
    traces = MS3TraceList()
    for bad in (b"some/path", None, ["a", "b"], 3.14):
        with pytest.raises(TypeError, match="file_name must be"):
            traces.add_file(bad)


def test_tracelist_to_file_rejects_invalid_filename_types(tmp_path):
    traces = MS3TraceList(file_name=test_path3)
    for bad in (b"some/path", None, ["a", "b"], 3.14):
        with pytest.raises(TypeError, match="filename must be"):
            traces.to_file(bad)


def test_tracelist_to_file_accepts_pathlike(tmp_path):
    import pathlib

    traces = MS3TraceList(file_name=test_path3, unpack_data=True)
    out = pathlib.Path(tmp_path) / "out.mseed3"
    records_written = traces.to_file(out, overwrite=True, format_version=3)
    assert records_written > 0
    assert out.exists() and out.stat().st_size > 0


def test_tracelist_add_file_does_not_retain_filename_buffer_without_record_list():
    """Without record_list=True no MS3RecordPtr entries reference the C
    filename buffer, so add_file() must not pin it on the trace list. The
    record_list=True path must retain it because libmseed stores the pointer in
    MS3RecordPtr entries for later use (e.g. by unpack_recordlist())."""
    traces = MS3TraceList()
    assert len(traces._c_file_names) == 0

    # Many add_file() calls on one trace list pin nothing when
    # record_list=False.
    for _ in range(5):
        traces.add_file(test_path3)
    assert len(traces._c_file_names) == 0

    # With record_list=True the buffer must be retained for the lifetime of the
    # records, but one per distinct path serves any number of calls.
    traces_rl = MS3TraceList()
    for _ in range(5):
        traces_rl.add_file(test_path3, record_list=True)
    assert len(traces_rl._c_file_names) == 1

    # The retained pointer is still readable through the record list.
    traceid = next(iter(traces_rl))
    seg = traceid[0]
    first_ptr = next(iter(seg.recordlist))
    assert first_ptr.filename is not None
    assert first_ptr.filename.endswith(os.path.basename(test_path3))


def test_tracelist_add_file_retains_one_buffer_per_path(tmp_path):
    """Distinct paths each need their own buffer, and a buffer shared by several
    reads must serve unpack_recordlist() for every record pointing at it."""
    copy_path = tmp_path / "copy.mseed3"
    copy_path.write_bytes(open(test_path3, "rb").read())

    traces = MS3TraceList()
    traces.add_file(test_path3, record_list=True)
    traces.add_file(copy_path, record_list=True)
    traces.add_file(test_path3, record_list=True)
    assert len(traces._c_file_names) == 2

    # Every segment unpacks, including the two sharing one filename buffer
    for traceid in traces:
        for seg in traceid:
            assert seg.unpack_recordlist() == seg.samplecnt


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


def _churn_heap():
    """Force reclamation and reuse of freed blocks so a stale pointer shows up."""
    gc.collect()
    return [bytearray(4096) for _ in range(3000)]


def test_recordlist_keeps_tracelist_alive():
    """A record list must outlive the expression that produced it.

    MS3RecordList and MS3RecordPtr point into memory owned by the MS3TraceList.
    Without a reference back to the owner, a temporary trace list was freed
    while the record list was still in use, which read freed memory and crashed.
    """
    # Ground truth with the trace list held alive
    held = MS3TraceList.from_file(test_path3, record_list=True)
    expected_count = len(held[0][0].recordlist)
    expected_sourceid = held[0][0].recordlist[0].record.sourceid
    del held
    _churn_heap()

    # Every intermediate here is a temporary
    recordlist = MS3TraceList.from_file(test_path3, record_list=True)[0][0].recordlist
    _churn_heap()

    assert len(recordlist) == expected_count
    assert recordlist.recordcnt == expected_count
    assert sum(1 for _ in recordlist) == expected_count
    assert recordlist[0].record.sourceid == expected_sourceid
    assert recordlist[-1].record.sourceid == expected_sourceid
    assert [p.fileoffset for p in recordlist[0:3]] == [0, 478, 1020]


def test_recordptr_keeps_tracelist_alive():
    """A single MS3RecordPtr must outlive the list and trace list it came from."""
    recordptr = MS3TraceList.from_file(test_path3, record_list=True)[0][0].recordlist[0]
    _churn_heap()

    assert recordptr.record.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert recordptr.fileoffset == 0
    assert recordptr.filename == test_path3
    assert "MS3RecordPtr(sourceid:" in repr(recordptr)


def test_record_from_temporary_recordlist_chain():
    """An MS3Record from a record list must keep the owning trace list alive.

    MS3RecordPtr.record wraps a struct owned by the trace list, so a record that
    escaped the whole chain used to read freed memory.
    """
    record = MS3TraceList.from_file(test_path3, record_list=True)[0][0].recordlist[0].record
    _churn_heap()

    assert record.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert record.reclen == 478
    assert record.record[:2] == b"MS"


def test_unpack_recordlist_from_temporary_tracelist():
    """A segment keeps its trace list alive, so unpacking works after the
    expression that produced it has gone away.

    This guards MS3TraceSeg's owner reference rather than reproducing a past
    failure; the segment already held one.
    """
    segment = MS3TraceList.from_file(test_path3, record_list=True)[0][0]
    _churn_heap()

    assert segment.unpack_recordlist() == segment.samplecnt
    assert segment.datasamples[0:3].tolist() == [-502916, -502808, -502691]


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
    starttime = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    format_version = 3
    max_record_length = 512

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            starttime=starttime,
        )

        starttime = sample_time(starttime, len(new_data), sample_rate)

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


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_mstracelist_pack_reraises_handler_exception():
    """A failing handler must not be reported as a successful pack()."""
    # Enough samples for many 128-byte records
    sine_500 = [int(math.sin(math.radians(x)) * 500) for x in range(0, 500)]

    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_X",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2024-01-01T00:00:00Z",
    )

    calls = []

    def _failing_handler(record, data):
        calls.append(record)
        raise OSError("no space left on device")

    with pytest.raises(OSError, match="no space left on device"):
        traces.pack(_failing_handler, max_record_length=128)

    # The records after the failure are not handed to the handler
    assert len(calls) == 1


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_mstracelist_pack_leaves_no_reference_cycle():
    """pack() must not tie the trace list into a reference cycle.

    A cycle leaves mstl3_free() waiting for the cyclic collector, which in a
    long-running rolling buffer defers every release.
    """
    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_X",
        data_samples=[1, 2, 3, 4, 5],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2024-01-01T00:00:00Z",
    )
    traces.pack(lambda record, handler_data: None)

    reference = weakref.ref(traces)

    # Reference counting alone must release it, with the cyclic collector off
    gc.collect()
    gc.disable()
    try:
        del traces
        assert reference() is None
    finally:
        gc.enable()


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_mstracelist_pack_does_not_retain_handler_data():
    """pack() must not keep the handler or handler data alive after returning.

    Handler data is typically the output file handle, which would then stay
    open for as long as the trace list lives.
    """

    class HandlerData:
        pass

    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_X",
        data_samples=[1, 2, 3, 4, 5],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2024-01-01T00:00:00Z",
    )

    handler_data = HandlerData()
    reference = weakref.ref(handler_data)

    traces.pack(lambda record, data: None, handler_data)

    del handler_data
    gc.collect()

    assert reference() is None


def test_mstracelist_generate_rollingbuffer():
    """Test creation of miniSEED v3 records from a trace list using a rolling buffer.

    The rolling buffer usage removes packed data from the trace list after each
    pack, data is then added and packed in later calls.  After the final pack to
    flush any remaining data, the trace list is empty.
    """
    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    starttime = timestr2nstime("2024-01-01T15:13:55.123456789Z")
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
            starttime=starttime,
        )

        starttime = sample_time(starttime, len(new_data), sample_rate)

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


def test_mstracelist_generate_raises_on_pack_error():
    """A packing failure must raise, not end the generator silently.

    mstl3_pack_next() returns 1 for a record, 0 when finished and a negative
    value on error; treating the error as completion would yield no records and
    let the caller write an empty file believing packing succeeded.
    """
    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=[1, 2, 3, 4, 5],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2023-01-01T00:00:00Z",
    )

    # miniSEED v2 record lengths must be a power of 2
    with pytest.raises(MiniSEEDError, match="power of 2"):
        list(traces.generate(format_version=2, max_record_length=1000))


def test_mstracelist_generate_validates_format_version_eagerly():
    """generate() must reject its arguments on the call, not on the first record.

    A generator body raising only at the first next() leaves the caller's
    try/except around the call itself unable to see the error.
    """
    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=[1, 2, 3, 4, 5],
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2023-01-01T00:00:00Z",
    )

    with pytest.raises(ValueError, match="Invalid miniSEED format version: 4"):
        traces.generate(format_version=4)

    # The supported versions still pack
    assert len(list(traces.generate(format_version=2, max_record_length=512))) == 1


def test_mstracelist_generate_flush_idle_reclaims_idle_sources():
    """A rolling buffer whose source IDs come and go relies on
    flush_idle_seconds to drain them; without it partial segments accumulate for
    the life of the trace list."""
    traces = MS3TraceList()
    for i in range(20):
        traces.add_data(
            sourceid=f"FDSN:XX_ST{i:03d}__B_H_Z",
            data_samples=[1, 2, 3, 4, 5],
            sample_type="i",
            sample_rate=100.0,
            starttime_str="2023-01-01T00:00:00Z",
        )
    assert len(traces) == 20

    # Partial records are not packable, and no source is idle yet
    assert list(traces.generate(flush_data=False, remove_packed=True)) == []
    assert len(traces) == 20

    # Idle segments flush, and each empty segment takes its trace ID with it
    time.sleep(1.1)
    records = list(
        traces.generate(flush_data=False, flush_idle_seconds=1, remove_packed=True)
    )
    assert len(records) == 20
    assert len(traces) == 0


def test_mstracelist_generate_abandoned_keeps_yielded_samples():
    """Pins the documented cost of abandoning generate() with remove_packed.

    libmseed trims a segment only when it finishes packing, so samples of the
    records already yielded stay in the trace list and a later generate()
    creates records for them again.
    """
    traces = MS3TraceList()
    traces.add_data(
        sourceid="FDSN:XX_STA__B_H_Z",
        data_samples=list(range(20000)),
        sample_type="i",
        sample_rate=100.0,
        starttime_str="2023-01-01T00:00:00Z",
    )
    samples = traces[0][0].numsamples

    generator = traces.generate(max_record_length=512, remove_packed=True)
    abandoned = 0
    for _record in generator:
        abandoned += 1
        if abandoned == 3:
            break
    generator.close()

    assert abandoned == 3
    assert traces[0][0].numsamples == samples

    # The full set of records is created again, duplicating the three above
    complete = len(list(traces.generate(max_record_length=512, remove_packed=True)))
    assert complete >= abandoned
    assert len(traces) == 0


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
    starttime = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    max_record_length = 512

    # Add 3 traces to the list
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_1",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        starttime=starttime,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_2",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        starttime=starttime,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_3",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        starttime=starttime,
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
    starttime = timestr2nstime("2024-01-01T15:13:55.123456789Z")

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            starttime=starttime,
        )

        starttime = sample_time(starttime, len(new_data), sample_rate)

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
        MS3TraceList("NOSUCHFILE")


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


def test_tracelist_filelike_record_list_raw_record_unavailable():
    """Filelike + record_list=True: raw record reads raise instead of returning
    stale bytes from the freed parse buffer."""
    with open(test_path3, "rb") as fp:
        buf = fp.read()
    traces = MS3TraceList.from_filelike(io.BytesIO(buf), record_list=True)

    gc.collect()
    scratch = bytearray(4 * 1024 * 1024)  # encourage reuse of the freed buffer
    del scratch

    for tid in traces:
        for recptr in tid[0].recordlist:
            record = recptr.record
            assert record.reclen > 0
            with pytest.raises(ValueError, match="No raw record available"):
                record.record
            with pytest.raises(ValueError, match="No raw record available"):
                record.record_mv


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
