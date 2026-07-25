"""
Tests for the sourceid / starttime / endtime selection filters on the
MS3Record.from_* factories.
"""

import io
import os

import pytest

from pymseed import MS3Record
from pymseed.exceptions import MiniSEEDError
from pymseed.logging import clear_error_messages, get_error_messages
from pymseed.util import timestr2nstime

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")

WINDOW_START = "2010-02-27T07:00:00Z"
WINDOW_END = "2010-02-27T07:30:00Z"

TOTAL_RECORDS = 1141
Z_RECORDS = 386
Z_WINDOWED_RECORDS = 159
ALL_WINDOWED_RECORDS = 480


@pytest.fixture
def buffer():
    with open(test_path3, "rb") as fp:
        return fp.read()


def _summary(records):
    """Reduce records to a comparable list of (sourceid, starttime) tuples"""
    return [(msr.sourceid, msr.starttime) for msr in records]


def _from_file(**kwargs):
    return _summary(MS3Record.from_file(test_path3, **kwargs))


def _from_buffer(buf, **kwargs):
    return _summary(MS3Record.from_buffer(buf, **kwargs))


def _from_filelike(buf, **kwargs):
    return _summary(MS3Record.from_filelike(io.BytesIO(buf), **kwargs))


# ---------------------------------------------------------------------------
# Source ID filtering
# ---------------------------------------------------------------------------


def test_sourceid_exact(buffer):
    """Exact source ID filter returns only records for that channel"""
    for records in (
        _from_file(sourceid="FDSN:IU_COLA_00_B_H_Z"),
        _from_buffer(buffer, sourceid="FDSN:IU_COLA_00_B_H_Z"),
        _from_filelike(buffer, sourceid="FDSN:IU_COLA_00_B_H_Z"),
    ):
        assert len(records) == Z_RECORDS
        assert {sourceid for sourceid, _ in records} == {"FDSN:IU_COLA_00_B_H_Z"}


def test_sourceid_glob(buffer):
    """Glob source ID filter matches all three channels in the test data"""
    assert len(_from_file(sourceid="FDSN:IU_COLA_00_B_H_*")) == TOTAL_RECORDS
    assert len(_from_buffer(buffer, sourceid="FDSN:IU_COLA_00_B_H_*")) == TOTAL_RECORDS
    assert len(_from_filelike(buffer, sourceid="FDSN:IU_COLA_00_B_H_*")) == TOTAL_RECORDS


def test_sourceid_no_match(buffer):
    """Non-matching source ID filter yields no records, and is not an error

    The file path is the interesting one: libmseed returns MS_NOTSEED once it
    reaches the end of the stream having returned no records, and the reader
    must not turn that into an exception while filtering is active.  libmseed's
    diagnostic is left in the log registry, drained here so it does not carry
    over into other tests.
    """
    assert _from_file(sourceid="FDSN:XX_NONE_*") == []
    assert any("not SEED" in message for message in get_error_messages())

    assert _from_buffer(buffer, sourceid="FDSN:XX_NONE_*") == []
    assert _from_filelike(buffer, sourceid="FDSN:XX_NONE_*") == []


def test_no_match_still_reports_truly_bad_input(tmp_path):
    """A non-miniSEED file is still an error, even with filtering active"""
    junk = tmp_path / "junk.mseed"
    junk.write_bytes(b"X" * 4096)

    with pytest.raises(MiniSEEDError):
        list(MS3Record.from_file(str(junk), sourceid="FDSN:XX_NONE_*"))

    clear_error_messages()


def test_no_filters_matches_unfiltered(buffer):
    """All-None filters read everything, as before"""
    assert len(_from_file()) == TOTAL_RECORDS
    assert len(_from_buffer(buffer)) == TOTAL_RECORDS
    assert len(_from_filelike(buffer)) == TOTAL_RECORDS


# ---------------------------------------------------------------------------
# Time window filtering
# ---------------------------------------------------------------------------


def test_time_window(buffer):
    """Time-window filter yields a subset of the records"""
    for records in (
        _from_file(starttime=WINDOW_START, endtime=WINDOW_END),
        _from_buffer(buffer, starttime=WINDOW_START, endtime=WINDOW_END),
        _from_filelike(buffer, starttime=WINDOW_START, endtime=WINDOW_END),
    ):
        assert len(records) == ALL_WINDOWED_RECORDS
        assert len(records) < TOTAL_RECORDS


def test_time_window_records_overlap_window():
    """Every selected record overlaps the requested window"""
    start_ns = timestr2nstime(WINDOW_START)
    end_ns = timestr2nstime(WINDOW_END)

    for msr in MS3Record.from_file(test_path3, starttime=WINDOW_START, endtime=WINDOW_END):
        assert msr.endtime >= start_ns
        assert msr.starttime <= end_ns


def test_open_start_window(buffer):
    """Only endtime given: open-ended start"""
    records = _from_file(endtime=WINDOW_END)
    assert 0 < len(records) < TOTAL_RECORDS
    assert records == _from_buffer(buffer, endtime=WINDOW_END)
    assert records == _from_filelike(buffer, endtime=WINDOW_END)


def test_open_end_window(buffer):
    """Only starttime given: open-ended end"""
    records = _from_file(starttime=WINDOW_START)
    assert 0 < len(records) < TOTAL_RECORDS
    assert records == _from_buffer(buffer, starttime=WINDOW_START)
    assert records == _from_filelike(buffer, starttime=WINDOW_START)


def test_sourceid_and_time_window(buffer):
    """Combined source ID and time-window filter"""
    kwargs = {
        "sourceid": "FDSN:IU_COLA_00_B_H_Z",
        "starttime": WINDOW_START,
        "endtime": WINDOW_END,
    }
    for records in (
        _from_file(**kwargs),
        _from_buffer(buffer, **kwargs),
        _from_filelike(buffer, **kwargs),
    ):
        assert len(records) == Z_WINDOWED_RECORDS
        assert {sourceid for sourceid, _ in records} == {"FDSN:IU_COLA_00_B_H_Z"}


def test_filelike_small_chunks_with_filters(buffer):
    """Filtering survives a sliding buffer that is smaller than a record"""
    kwargs = {
        "sourceid": "FDSN:IU_COLA_00_B_H_Z",
        "starttime": WINDOW_START,
        "endtime": WINDOW_END,
    }
    records = _summary(MS3Record.from_filelike(io.BytesIO(buffer), chunk_size=64, **kwargs))

    assert records == _from_file(**kwargs)


def test_all_paths_select_identical_records(buffer):
    """The C-level (file) and Python-loop (buffer, file-like) paths agree"""
    kwargs = {
        "sourceid": "FDSN:IU_COLA_00_B_H_Z",
        "starttime": WINDOW_START,
        "endtime": WINDOW_END,
    }
    from_file = _from_file(**kwargs)

    assert from_file == _from_buffer(buffer, **kwargs)
    assert from_file == _from_filelike(buffer, **kwargs)


# ---------------------------------------------------------------------------
# Data unpacking interaction (unpacking is deferred past the selection match)
# ---------------------------------------------------------------------------


def test_filtered_unpack_data(buffer):
    """Selected records still have their data samples unpacked on request"""
    kwargs = {
        "unpack_data": True,
        "sourceid": "FDSN:IU_COLA_00_B_H_Z",
        "starttime": WINDOW_START,
        "endtime": WINDOW_END,
    }

    for source in (
        MS3Record.from_file(test_path3, **kwargs),
        MS3Record.from_buffer(buffer, **kwargs),
        MS3Record.from_filelike(io.BytesIO(buffer), **kwargs),
    ):
        count = 0
        for msr in source:
            count += 1
            assert msr.numsamples == msr.samplecnt
            assert len(msr.datasamples) == msr.numsamples
        assert count == Z_WINDOWED_RECORDS


def test_filtered_unpack_data_samples_match_unfiltered(buffer):
    """Deferred unpacking produces the same samples as an unfiltered read"""
    unfiltered = {
        msr.starttime: list(msr.datasamples)
        for msr in MS3Record.from_buffer(buffer, unpack_data=True)
        if msr.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    }

    checked = 0
    for msr in MS3Record.from_buffer(
        buffer,
        unpack_data=True,
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime=WINDOW_START,
        endtime=WINDOW_END,
    ):
        assert list(msr.datasamples) == unfiltered[msr.starttime]
        checked += 1

    assert checked == Z_WINDOWED_RECORDS


# ---------------------------------------------------------------------------
# iter_records() forwards the filters on every dispatch path
# ---------------------------------------------------------------------------


def test_iter_records_filters(buffer):
    """iter_records() forwards filters for path, buffer, and file-like sources"""
    kwargs = {
        "sourceid": "FDSN:IU_COLA_00_B_H_Z",
        "starttime": WINDOW_START,
        "endtime": WINDOW_END,
    }

    for source in (test_path3, buffer, io.BytesIO(buffer)):
        records = _summary(MS3Record.iter_records(source, **kwargs))
        assert len(records) == Z_WINDOWED_RECORDS


def test_file_descriptor_filters():
    """Filters work when the source is an open file descriptor"""
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    fd = os.open(test_path3, flags)
    try:
        records = _summary(
            MS3Record.from_file(
                fd,
                sourceid="FDSN:IU_COLA_00_B_H_Z",
                starttime=WINDOW_START,
                endtime=WINDOW_END,
            )
        )
    finally:
        os.close(fd)

    assert len(records) == Z_WINDOWED_RECORDS


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kwarg", ["starttime", "endtime"])
def test_invalid_time_string(buffer, kwarg):
    """An unparsable time string raises ValueError on every path"""
    with pytest.raises(ValueError, match=f"Invalid {kwarg} time string"):
        MS3Record.from_file(test_path3, **{kwarg: "not-a-time"})

    with pytest.raises(ValueError, match=f"Invalid {kwarg} time string"):
        next(MS3Record.from_buffer(buffer, **{kwarg: "not-a-time"}))

    with pytest.raises(ValueError, match=f"Invalid {kwarg} time string"):
        next(MS3Record.from_filelike(io.BytesIO(buffer), **{kwarg: "not-a-time"}))


def test_inverted_time_window(buffer):
    """A start time later than the end time is rejected by libmseed"""
    inverted = {"starttime": WINDOW_END, "endtime": WINDOW_START}

    with pytest.raises(MiniSEEDError):
        MS3Record.from_file(test_path3, **inverted)

    with pytest.raises(MiniSEEDError):
        next(MS3Record.from_buffer(buffer, **inverted))

    with pytest.raises(MiniSEEDError):
        next(MS3Record.from_filelike(io.BytesIO(buffer), **inverted))


# ---------------------------------------------------------------------------
# Reader lifetime: selections are owned by the reader until it is closed
# ---------------------------------------------------------------------------


def test_reader_close_is_idempotent_with_selections():
    """Repeated close() with active selections does not double-free"""
    reader = MS3Record.from_file(
        test_path3,
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime=WINDOW_START,
        endtime=WINDOW_END,
    )
    assert reader.read() is not None
    reader.close()
    reader.close()

    with pytest.raises(ValueError):
        reader.read()


def test_reader_partial_iteration_with_selections():
    """Abandoning a filtered reader part way through cleans up without error"""
    with MS3Record.from_file(
        test_path3,
        sourceid="FDSN:IU_COLA_00_B_H_Z",
        starttime=WINDOW_START,
        endtime=WINDOW_END,
    ) as reader:
        for count, _msr in enumerate(reader, start=1):
            if count == 5:
                break


def test_generator_close_with_selections(buffer):
    """Closing a filtered generator early frees the selections"""
    gen = MS3Record.from_buffer(buffer, sourceid="FDSN:IU_COLA_00_B_H_Z")
    assert next(gen) is not None
    gen.close()

    gen = MS3Record.from_filelike(io.BytesIO(buffer), sourceid="FDSN:IU_COLA_00_B_H_Z")
    assert next(gen) is not None
    gen.close()
