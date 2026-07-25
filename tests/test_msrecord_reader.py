import gc
import os
import sys

import pytest

from pymseed import NSTMODULUS, DataEncoding, MS3Record, SubSecond, TimeFormat
from pymseed.clib import clibmseed
from pymseed.exceptions import MiniSEEDError

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")
test_path2 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")
test_60sec = os.path.join(test_dir, "data", "testdata-60sec-period.mseed3")


def test_msrecord_read_record_details():
    with MS3Record.from_file(test_path3, unpack_data=True) as msreader:
        # Read first record
        msr = msreader.read()

        assert msr.reclen == 478
        assert msr.swapflag == 2
        assert msr.swapflag_dict() == {"header_swapped": False, "payload_swapped": True}
        assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
        assert msr.formatversion == 3
        assert msr.flags == 4
        assert msr.flags_dict() == {"clock_locked": True}
        assert msr.starttime == 1267253400019539000
        assert msr.starttime_seconds == pytest.approx(1267253400.019539)
        assert (
            msr.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_Z) == "2010-02-27T06:50:00.019539Z"
        )
        assert (
            msr.starttime_str(timeformat=TimeFormat.SEEDORDINAL, subsecond=SubSecond.NONE)
            == "2010,058,06:50:00"
        )
        assert msr.samprate == 20.0
        assert msr.samprate_raw == 20.0
        assert msr.samprate_period_ns == 0.05 * NSTMODULUS
        assert msr.samprate_period_seconds == pytest.approx(0.05)
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
        assert msr.endtime_seconds == pytest.approx(1267253414.769539)

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [
            -502916,
            -502808,
            -502691,
            -502567,
            -502433,
            -502331,
        ]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [
            -508722,
            -508764,
            -508809,
            -508866,
            -508927,
            -508986,
        ]


def test_msrecord_read_unpack_data():
    with MS3Record.from_file(test_path3, unpack_data=False) as msreader:
        # Read first record
        msr = msreader.read()

        assert msr.samplecnt == 296
        assert msr.numsamples == 0
        assert not msr.datasamples
        assert msr.sampletype is None

        # Unpack data
        unpacked = msr.unpack_data()
        assert unpacked == 296

        assert msr.numsamples == 296
        assert msr.datasamples
        assert msr.sampletype == "i"

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [
            -502916,
            -502808,
            -502691,
            -502567,
            -502433,
            -502331,
        ]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [
            -508722,
            -508764,
            -508809,
            -508866,
            -508927,
            -508986,
        ]


def test_msrecord_read_record_60sec():
    with MS3Record.from_file(test_60sec, unpack_data=True) as msreader:
        # Read first record
        msr = msreader.read()

        assert msr.reclen == 4090
        assert msr.sourceid == "FDSN:XX_SIN__W_X_Y"
        assert msr.samprate == pytest.approx(0.01666667)
        assert msr.samprate_raw == -60.0
        assert msr.samprate_period_ns == 60 * NSTMODULUS
        assert msr.samprate_period_seconds == pytest.approx(60.0)


def test_msrecord_read_record_offsets():
    # miniSEED v3 file
    with MS3Record.from_file(
        test_path3, start_byte_offset=408442, end_byte_offset=408600, unpack_data=True
    ) as msreader:
        # Read first record
        msr = msreader.read()

        assert msr.reclen == 158
        assert msr.sourceid == "FDSN:IU_COLA_00_B_H_Z"
        assert msr.samprate == 20.0

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]

    # miniSEED v2 file
    with MS3Record.from_file(
        test_path2, start_byte_offset=386560, end_byte_offset=387072, unpack_data=True
    ) as msreader:
        # Read first record
        msr = msreader.read()

        assert msr.reclen == 512
        assert msr.sourceid == "FDSN:IU_COLA_00_B_H_Z"
        assert msr.samprate == 20.0

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]


def test_msrecord_from_file_rejects_negative_fd():
    """Negative ints are not valid file descriptors and must be rejected."""
    with pytest.raises(ValueError, match="non-negative"):
        MS3Record.from_file(-1)
    with pytest.raises(ValueError, match="non-negative"):
        MS3Record.from_file(-1234)


def test_iter_records_rejects_negative_fd():
    """iter_records routes int sources to from_file; the validation must surface."""
    # iter_records returns a generator, so the validation has to fire on first
    # iteration (next()) rather than at call time — confirm that's what happens.
    gen = MS3Record.iter_records(-1)
    with pytest.raises(ValueError, match="non-negative"):
        next(gen)


def test_msrecord_read_record_details_fd():
    # Test reading from a file descriptor - we simulate this using the buffer reader

    # File descriptor support is not implemented on Windows
    if sys.platform.lower().startswith("win"):
        return

    # Using a file for testing, but this could be stdin or any other input stream
    file_descriptor = None
    with open(test_path2, "rb", buffering=0) as fp:
        original_file_descriptor = fp.fileno()
        file_descriptor = os.dup(original_file_descriptor)

    # Provide the reader with the file descriptor
    with MS3Record.from_file(file_descriptor, unpack_data=True) as msreader:
        # Read first record
        msr = msreader.read()

        # Verify we got a valid record
        assert msr is not None

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [
            -502916,
            -502808,
            -502691,
            -502567,
            -502433,
            -502331,
        ]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [
            -508722,
            -508764,
            -508809,
            -508866,
            -508927,
            -508986,
        ]


def test_msrecord_read_records_summary():
    record_count = 0
    sample_count = 0

    # Direct iteration without context manager
    for msr in MS3Record.from_file(test_path2):
        record_count += 1
        sample_count += msr.samplecnt

    assert record_count == 1141
    assert sample_count == 252000


def test_msrecord_nosuchfile():
    with pytest.raises(MiniSEEDError):
        with MS3Record.from_file("NOSUCHFILE") as msreader:
            msreader.read()


def test_msrecord_reader_accepts_pathlike_source():
    # pathlib.Path (and any os.PathLike) is converted via os.fspath() rather
    # than reaching the path branch as a non-string and crashing on `.encode()`.
    import pathlib

    with MS3Record.from_file(pathlib.Path(test_path3), unpack_data=False) as msreader:
        msr = msreader.read()
        assert msr is not None


def test_msrecord_reader_half_initialized_object_cleans_up():
    # When __init__ raises before opening the file, __del__ must still be
    # able to run close() without tripping AttributeError on missing
    # attributes — i.e. all instance attributes must be initialised before
    # any code that can raise.
    import gc
    import io
    import sys

    from pymseed import MS3RecordReader

    # Capture any "Exception ignored in __del__" chatter that would indicate
    # the broad swallow in __del__ caught an AttributeError on a missing
    # attribute (rather than running close() cleanly).
    stderr_buf = io.StringIO()
    saved_stderr = sys.stderr
    sys.stderr = stderr_buf
    try:
        # Bad source type: TypeError raised during validation.
        with pytest.raises(TypeError):
            MS3RecordReader(3.14)
        # Missing required argument.
        with pytest.raises(TypeError):
            MS3RecordReader()
        # Negative fd: passes type validation, fails on the < 0 check
        # after C resources have been partially set up.
        with pytest.raises(ValueError):
            MS3RecordReader(-1)
        gc.collect()
    finally:
        sys.stderr = saved_stderr

    assert "Exception ignored" not in stderr_buf.getvalue(), (
        f"__del__ tripped on half-init object: {stderr_buf.getvalue()!r}"
    )


def test_msrecord_reader_rejects_invalid_source_types():
    # bytes, None, list, etc. used to fall into the path branch and raise a
    # confusing AttributeError on `.encode()`. They must now fail fast with
    # TypeError before any C resources are allocated.
    from pymseed import MS3RecordReader

    for bad in (b"some/path", None, ["a", "b"], 3.14, {"x": 1}):
        with pytest.raises(TypeError, match="source must be"):
            MS3RecordReader(bad)


def test_msrecord_reader_handles_surrogateescape_paths(tmp_path):
    # Filesystem APIs on POSIX with a non-UTF-8 locale can return paths that
    # contain surrogate-escaped bytes (e.g. b"foo\xff" -> "foo\udcff"). Using
    # str.encode("utf-8") on such paths raises UnicodeEncodeError; os.fsencode
    # round-trips them. Verify the wrapper survives __init__ on a surrogate
    # path — libmseed will raise MiniSEEDError on the first read (file doesn't
    # exist), which is the *correct* failure mode, not UnicodeEncodeError.
    if sys.platform.startswith("win"):
        pytest.skip("surrogateescape paths are POSIX-only")

    weird_path = str(tmp_path / "missing_\udcff_byte.mseed")
    with pytest.raises(MiniSEEDError):
        with MS3Record.from_file(weird_path) as r:
            r.read()


def test_msrecord_reader_rejects_invalid_byte_offsets():
    # libmseed silently treats negative offsets as "no offset" — that hides
    # caller bugs. The Python wrapper must reject them.
    from pymseed import MS3RecordReader

    with pytest.raises(ValueError, match="start_byte_offset must be non-negative"):
        MS3RecordReader(test_path3, start_byte_offset=-1)
    with pytest.raises(ValueError, match="end_byte_offset must be non-negative"):
        MS3RecordReader(test_path3, end_byte_offset=-5)
    with pytest.raises(ValueError, match=r"end_byte_offset .* must be >="):
        MS3RecordReader(test_path3, start_byte_offset=1000, end_byte_offset=500)

    # Boundary: end_byte_offset == 0 means "read to EOF" and must be allowed
    # regardless of start_byte_offset.
    with MS3RecordReader(test_path3, start_byte_offset=1000, end_byte_offset=0) as r:
        # Just confirm construction succeeds; reading may or may not return
        # data depending on the offset, but we don't care here.
        assert r is not None


def test_msrecord_reader_input_kwarg_is_deprecated_alias():
    # Passing the legacy `input=` keyword must still work but emit a
    # DeprecationWarning pointing users at the new `source=` name.
    from pymseed import MS3RecordReader

    with pytest.warns(DeprecationWarning, match="'input' is a deprecated alias"):
        reader = MS3RecordReader(input=test_path3)
    try:
        msr = reader.read()
        assert msr is not None
    finally:
        reader.close()

    with pytest.raises(TypeError, match="missing required argument"):
        MS3RecordReader()


def test_msrecord_reader_rejects_source_and_input_together():
    """Supplying both must not silently discard one of them."""
    from pymseed import MS3RecordReader

    with pytest.raises(TypeError, match="both 'source' and its deprecated alias"):
        MS3RecordReader(source=test_path3, input="nonexistent.mseed")

    # Also when source is positional
    with pytest.raises(TypeError, match="both 'source' and its deprecated alias"):
        MS3RecordReader(test_path3, input="nonexistent.mseed")

    # Reaching the reader through MS3Record.from_file() is no different
    with pytest.raises(TypeError, match="both 'source' and its deprecated alias"):
        MS3Record.from_file(test_path3, input="nonexistent.mseed")


def test_msrecord_reader_rejects_use_after_close():
    # Reading or iterating after close() must not silently resurrect the
    # underlying libmseed file param (which would re-open the file from the
    # start because *msfp == NULL is libmseed's lazy-init signal).
    reader = MS3Record.from_file(test_path3)
    next(reader)
    reader.close()

    with pytest.raises(ValueError, match="closed"):
        reader.read()
    with pytest.raises(ValueError, match="closed"):
        next(reader)

    # Context-manager exit closes too; same contract applies.
    with MS3Record.from_file(test_path3) as msreader:
        msreader.read()
    with pytest.raises(ValueError, match="closed"):
        msreader.read()


def test_record_survives_temporary_reader():
    """A record must not read freed memory when its reader is a temporary.

    MS3RecordReader.read() hands out a wrapper around the reader's own struct.
    Without a reference back to the reader, garbage collection freed the struct
    and access crashed or silently returned an empty record.
    """
    raw = next(iter(MS3Record.from_file(test_path3))).record
    gc.collect()
    _churn = [bytearray(4096) for _ in range(3000)]

    assert raw[:2] == b"MS"
    assert len(raw) == 478

    msr = next(iter(MS3Record.from_file(test_path3, unpack_data=True)))
    gc.collect()
    _churn = [bytearray(4096) for _ in range(3000)]

    assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert msr.numsamples == msr.samplecnt
    assert msr.datasamples[0] == -502916


def test_msrecord_reader_reports_truncated_final_record(tmp_path):
    """A file ending mid-record must not read as a clean end of stream."""
    with open(test_path3, "rb") as fp:
        data = fp.read()
    truncated = tmp_path / "truncated.mseed3"
    truncated.write_bytes(data[:-100])

    records = []
    with pytest.raises(MiniSEEDError, match="100 more bytes needed") as excinfo:
        for msr in MS3Record.from_file(str(truncated)):
            records.append(msr.sourceid)

    assert excinfo.value.status_code == clibmseed.MS_ENDOFFILE
    assert len(records) == 1140

    # Skipping non-data accepts the remnant as the end of the stream
    assert sum(1 for _ in MS3Record.from_file(str(truncated), skip_not_data=True)) == 1140


def test_msrecord_reader_reports_trailing_bytes(tmp_path):
    """Bytes too few for any record must be reported, not dropped."""
    with open(test_path3, "rb") as fp:
        data = fp.read()
    padded = tmp_path / "padded.mseed3"
    padded.write_bytes(data + b"xx")

    with pytest.raises(MiniSEEDError, match="2 unparsed bytes"):
        list(MS3Record.from_file(str(padded)))

    assert sum(1 for _ in MS3Record.from_file(str(padded), skip_not_data=True)) == 1141


def test_msrecord_reader_reports_byte_range_ending_mid_record():
    """A byte range ending inside a record is as truncated as a short file."""
    with pytest.raises(MiniSEEDError, match="more bytes needed"):
        list(MS3Record.from_file(test_path3, end_byte_offset=600))

    # The first record ends exactly at the requested end offset
    assert sum(1 for _ in MS3Record.from_file(test_path3, end_byte_offset=477)) == 1
