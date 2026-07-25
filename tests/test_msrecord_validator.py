"""
Tests for MS3RecordValidator - record-by-record buffer parsing with error accumulation.
"""

import os

import pytest

from pymseed import MS3RecordValidator, MS3TraceList, ValidationError

# Test data paths
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, "data")
TEST_MSEED3_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed3")
TEST_MSEED2_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed2")

# miniSEED v3 fixed header: byte 15 is the data encoding field
_V3_ENCODING_OFFSET = 15

# miniSEED v3 fixed header: bytes 36-39 are the payload length (uint32, LE)
_V3_DATALENGTH_OFFSET = 36


def get_test_buffer(filepath: str) -> bytes:
    """Read test file into a buffer."""
    with open(filepath, "rb") as f:
        return f.read()


def get_test_records(filepath: str) -> list[bytes]:
    """Get individual record bytes from a test file."""
    from pymseed import MS3Record

    return [msr.record for msr in MS3Record.from_file(filepath)]


def get_corrupted_record() -> bytes:
    """Get a corrupted miniSEED v3 record that will trigger a CRC error."""
    records = get_test_records(TEST_MSEED3_FILE)
    corrupted = bytearray(records[0])
    corrupted[100] = 0xFF
    corrupted[101] = 0xFF
    corrupted[102] = 0xFF
    return bytes(corrupted)


def _get_record_with_bad_extra_headers() -> bytes:
    """Get a miniSEED v3 record whose extra headers fail FDSN schema validation."""
    records = get_test_records(TEST_MSEED3_FILE)
    rec = bytearray(records[0])
    old = b'{"FDSN":{"Time":{"Quality":100}}}'
    new = b'{"FDSN":{"Time":{"Quality":"X"}}}'
    idx = rec.find(old)
    assert idx >= 0, "expected extra headers not found in test record"
    rec[idx : idx + len(old)] = new
    return bytes(rec)


def _get_record_with_bad_encoding() -> bytes:
    """Get a miniSEED v3 record with an invalid data encoding value (0xFF)."""
    records = get_test_records(TEST_MSEED3_FILE)
    rec = bytearray(records[0])
    rec[_V3_ENCODING_OFFSET] = 0xFF
    return bytes(rec)


def _get_record_with_oversized_datalength() -> bytes:
    """Get a miniSEED v3 record claiming a payload far beyond MAXRECLEN."""
    import struct

    rec = bytearray(get_test_records(TEST_MSEED3_FILE)[0])
    struct.pack_into("<I", rec, _V3_DATALENGTH_OFFSET, 0xFFFFFF00)
    return bytes(rec)


class _CountingStream:
    """Forward-only stream of `head` followed by `filler` NUL bytes, tracking
    how much has been read."""

    def __init__(self, head: bytes = b"", filler: int = 0) -> None:
        self._head = head
        self.total = len(head) + filler
        self.pos = 0

    def read(self, n: int) -> bytes:
        n = min(n, self.total - self.pos)
        start, end = self.pos, self.pos + n
        self.pos = end
        head = self._head[start:end]
        return head + b"\x00" * (n - len(head))


class TestMS3RecordValidatorBasic:
    """Basic functionality tests."""

    def test_validate_clean_buffer_mseed3(self) -> None:
        """Test parsing a clean miniSEED v3 buffer with no errors."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=False).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_clean_buffer_mseed2(self) -> None:
        """Test parsing a clean miniSEED v2 buffer with no errors."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=False).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_with_unpack_data(self) -> None:
        """Test parsing with data unpacking enabled produces no errors on clean data."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()

        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_empty_buffer(self) -> None:
        """Test parsing an empty buffer produces no errors and no traces."""
        errors, traces = MS3RecordValidator.from_buffer(b"").validate()

        assert len(traces) == 0
        assert len(errors) == 0

    def test_return_trace_list_false(self) -> None:
        """Test that return_trace_list=False returns None for traces."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, return_trace_list=False).validate()

        assert traces is None
        assert len(errors) == 0

    def test_validate_garbage_data(self) -> None:
        """Test that non-miniSEED data produces a detection error."""
        errors, traces = MS3RecordValidator.from_buffer(b"X" * 256).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert "No miniSEED detected" in errors[0].message

    def test_validate_incomplete_record(self) -> None:
        """A truncated record (header parseable, body missing) must be reported.
        Stopping silently made "no errors" mean "valid file" for input whose
        records were never actually checked."""
        records = get_test_records(TEST_MSEED3_FILE)
        # 60 bytes: enough for ms3_detect to read the header and return a record
        # length, but far less than the actual record.
        truncated = records[0][:60]

        errors, traces = MS3RecordValidator.from_buffer(truncated).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert errors[0].offset == 0
        assert "Incomplete record at end of source" in errors[0].message
        assert f"{len(records[0])} bytes needed, 60 available" in errors[0].message


class TestMS3RecordValidatorCRCValidation:
    """Tests for CRC validation error handling."""

    def test_crc_error_detected_and_logged(self) -> None:
        """Test that CRC errors produce a validation error mentioning CRC."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=True).validate()

        assert len(errors) >= 1
        crc_errors = [e for e in errors if "CRC" in e.message]
        assert len(crc_errors) >= 1

    def test_crc_validation_disabled(self) -> None:
        """Test that disabling CRC validation suppresses CRC errors."""
        corrupted = get_corrupted_record()

        errors_with, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=True).validate()
        errors_without, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=False).validate()

        crc_with = [e for e in errors_with if "CRC" in e.message]
        crc_without = [e for e in errors_without if "CRC" in e.message]
        assert len(crc_with) >= 1
        assert len(crc_without) == 0


class TestMS3RecordValidatorDataUnpacking:
    """Tests for data unpacking error handling."""

    def test_unpack_data_errors_logged(self) -> None:
        """Test that an invalid encoding triggers an unpack error."""
        buffer = _get_record_with_bad_encoding()

        errors, _ = MS3RecordValidator.from_buffer(
            buffer,
            unpack_data=True,
            validate_crc=False,
        ).validate()

        assert len(errors) >= 1

    def test_unpack_data_disabled_suppresses_errors(self) -> None:
        """Test that unpack_data=False suppresses data unpacking errors."""
        pytest.importorskip("jsonschema_rs")
        buffer = _get_record_with_bad_encoding()

        errors_on, _ = MS3RecordValidator.from_buffer(
            buffer,
            unpack_data=True,
            validate_crc=False,
        ).validate()
        errors_off, _ = MS3RecordValidator.from_buffer(
            buffer,
            unpack_data=False,
            validate_crc=False,
        ).validate()

        assert len(errors_on) >= 1
        assert len(errors_off) == 0


class TestMS3RecordValidatorExtraHeaders:
    """Tests for extra headers validation."""

    def test_extra_headers_validation_enabled(self) -> None:
        """Test that invalid extra headers are detected when validation is enabled."""
        buffer = _get_record_with_bad_extra_headers()

        errors, traces = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=True,
        ).validate()

        assert len(traces) > 0
        eh_errors = [e for e in errors if "Extra headers" in e.message]
        assert len(eh_errors) >= 1

    def test_extra_headers_validation_disabled(self) -> None:
        """Test that validate_extra_headers=False suppresses extra header errors."""
        buffer = _get_record_with_bad_extra_headers()

        errors_on, _ = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=True,
        ).validate()
        errors_off, _ = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=False,
        ).validate()

        eh_errors_on = [e for e in errors_on if "Extra headers" in e.message]
        eh_errors_off = [e for e in errors_off if "Extra headers" in e.message]
        assert len(eh_errors_on) >= 1
        assert len(eh_errors_off) == 0


class TestExtraHeadersSchemaLoadFailures:
    """Tests for broad catch of schema-loading failures (issue: schema-loading
    exceptions too narrow). All failure modes should produce a descriptive
    per-record ValidationError rather than aborting validate() with an
    unhandled exception.

    The loader is module-level cached via functools.cache, so these tests
    clear the cache before AND after each scenario so the patched failure
    mode is what gets loaded, and so a poisoned negative result doesn't
    leak into other tests.
    """

    @pytest.fixture(autouse=True)
    def _clear_schema_cache(self):
        from pymseed import _extra_headers_jsonschema as ehjs

        ehjs.load_extra_headers_validator.cache_clear()
        yield
        ehjs.load_extra_headers_validator.cache_clear()

    def test_corrupt_schema_file_does_not_abort_validation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If json_loads on the bundled schema raises, validate() must still
        complete and emit a 'schema load failed' warning per affected record."""
        from pymseed import _extra_headers_jsonschema as ehjs

        def boom(_data: bytes) -> object:
            raise ValueError("simulated corrupt schema JSON")

        monkeypatch.setattr(ehjs, "json_loads", boom)

        buffer = _get_record_with_bad_extra_headers()
        errors, _ = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=True,
        ).validate()

        skipped = [e for e in errors if "Extra headers validation skipped" in e.message]
        # Emit-once: even if every record carries extra headers, only the
        # first one produces the warning.
        assert len(skipped) == 1
        assert "failed to load JSON schema" in skipped[0].message
        assert "simulated corrupt schema JSON" in skipped[0].message

    def test_missing_schema_file_does_not_abort_validation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing bundled schema (corrupt install) must downgrade to a
        per-record warning, not crash."""
        from pymseed import _extra_headers_jsonschema as ehjs

        class _FakeJoin:
            def joinpath(self, *_args: object) -> "_FakeJoin":
                return self

            def read_bytes(self) -> bytes:
                raise FileNotFoundError("simulated missing schema file")

        monkeypatch.setattr(ehjs, "files", lambda _pkg: _FakeJoin())

        buffer = _get_record_with_bad_extra_headers()
        errors, _ = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=True,
        ).validate()

        skipped = [e for e in errors if "Extra headers validation skipped" in e.message]
        assert len(skipped) == 1
        assert "bundled schema file unavailable" in skipped[0].message

    def test_load_warning_emitted_at_most_once_per_validate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On a multi-record buffer where every record carries extra headers,
        the schema-load failure must produce exactly one 'skipped' warning,
        not one-per-record — otherwise the errors list is drowned in
        duplicated noise."""
        from pymseed import _extra_headers_jsonschema as ehjs

        monkeypatch.setattr(
            ehjs, "json_loads", lambda _b: (_ for _ in ()).throw(ValueError("boom"))
        )

        # Build a buffer with several records, all carrying extra headers.
        records = get_test_records(TEST_MSEED3_FILE)
        records_with_eh = [r for r in records if b'"FDSN"' in r]
        assert len(records_with_eh) >= 3, "need >=3 records with extra headers"
        buffer = b"".join(records_with_eh[:5])

        errors, _ = MS3RecordValidator.from_buffer(
            buffer,
            validate_crc=False,
            validate_extra_headers=True,
        ).validate()

        skipped = [e for e in errors if "Extra headers validation skipped" in e.message]
        assert len(skipped) == 1, (
            f"expected exactly one skip warning across all records; got {len(skipped)}"
        )

    def test_schema_loader_is_cached(self) -> None:
        """The loader's cached outcome must be reused across repeated
        validate() calls and across separate validator instances using the
        same schema_id — verified by inspecting cache_info."""
        from pymseed import _extra_headers_jsonschema as ehjs

        buffer = _get_record_with_bad_extra_headers()

        # First call populates the cache (1 miss, 0 hits).
        MS3RecordValidator.from_buffer(
            buffer, validate_crc=False, validate_extra_headers=True
        ).validate()
        info1 = ehjs.load_extra_headers_validator.cache_info()
        assert info1.misses == 1
        assert info1.hits == 0

        # Second validate() call on a fresh instance must hit the cache.
        MS3RecordValidator.from_buffer(
            buffer, validate_crc=False, validate_extra_headers=True
        ).validate()
        info2 = ehjs.load_extra_headers_validator.cache_info()
        assert info2.misses == 1, "expected no additional schema load"
        assert info2.hits == 1


class TestMS3RecordValidatorPartialData:
    """Tests for handling partial/incomplete data."""

    def test_partial_record_at_end(self) -> None:
        """An incomplete record at the end of a buffer is reported while every
        complete record before it is still validated and returned."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        full_errors, full_traces = MS3RecordValidator.from_buffer(buffer).validate()
        assert len(full_errors) == 0

        # Truncate buffer mid-record — chop a few bytes off the end
        truncated = buffer[: len(buffer) - 10]

        errors, traces = MS3RecordValidator.from_buffer(truncated).validate()

        full_samples = sum(seg.samplecnt for tid in full_traces for seg in tid)
        trunc_samples = sum(seg.samplecnt for tid in traces for seg in tid)

        assert len(traces) > 0
        assert trunc_samples < full_samples

        # The shortfall must be reported, not swallowed: "no errors" has to keep
        # meaning "every record in the source was checked".
        assert len(errors) == 1
        assert "Incomplete record at end of source" in errors[0].message
        assert errors[0].offset < len(truncated)

    def test_partial_record_at_end_of_stream(self) -> None:
        """The stream source must report the same shortfall as the buffer source,
        at the same offset."""
        pytest.importorskip("jsonschema_rs")
        import io

        truncated = get_test_buffer(TEST_MSEED3_FILE)[:-100]

        buffer_errors, _ = MS3RecordValidator.from_buffer(truncated).validate()
        stream_errors, _ = MS3RecordValidator.from_filelike(io.BytesIO(truncated)).validate()

        assert len(buffer_errors) == 1
        assert [(e.offset, e.message) for e in stream_errors] == [
            (e.offset, e.message) for e in buffer_errors
        ]

    def test_itemsize_views_validate_the_whole_buffer(self) -> None:
        """len() on a buffer-protocol object with itemsize > 1 is the element
        count, not the byte count. Sizing the source with it made the validator
        examine only 1/itemsize of the data and report a clean, short result."""
        pytest.importorskip("jsonschema_rs")
        import array

        data = get_test_buffer(TEST_MSEED3_FILE)
        assert len(data) % 2 == 0, "test data must divide evenly into 2-byte items"

        ref_errors, ref_traces = MS3RecordValidator.from_buffer(data).validate()
        expected = (
            len(ref_errors),
            len(ref_traces),
            sum(seg.samplecnt for tid in ref_traces for seg in tid),
        )
        assert expected[0] == 0, "reference file must validate cleanly"

        views: list[tuple[str, object]] = [
            ("memoryview", memoryview(data)),
            ("memoryview.cast('H')", memoryview(data).cast("H")),
            ("array('H')", array.array("H", data)),
        ]

        np = pytest.importorskip("numpy", reason="numpy views are the common case")
        views.append(("numpy int16", np.frombuffer(data, dtype=np.int16)))

        for label, view in views:
            errors, traces = MS3RecordValidator.from_buffer(view).validate()
            got = (len(errors), len(traces), sum(seg.samplecnt for tid in traces for seg in tid))
            assert got == expected, f"{label}: got {got}, expected {expected}"

    def test_undeterminable_record_length_reported(self) -> None:
        """A v2 record whose length cannot be determined (no B1000 blockette and
        no following header to scan to) leaves its payload unchecked, so it must
        be reported rather than ending iteration silently."""
        import io
        import struct

        rec = bytearray(get_test_buffer(TEST_MSEED2_FILE)[:512])

        # Retype the B1000 blockette so the record length is undeterminable.
        blkt_offset = struct.unpack_from(">H", rec, 46)[0]
        assert struct.unpack_from(">H", rec, blkt_offset)[0] == 1000
        struct.pack_into(">H", rec, blkt_offset, 1001)
        rec = bytes(rec)

        for errors, _ in (
            MS3RecordValidator.from_buffer(rec).validate(),
            MS3RecordValidator.from_filelike(io.BytesIO(rec)).validate(),
        ):
            assert len(errors) == 1
            assert errors[0].offset == 0
            assert errors[0].message == "Record length could not be determined"

    def test_complete_source_reports_no_shortfall(self) -> None:
        """Guard against over-reporting: a source ending exactly on a record
        boundary must stay clean, for both sources and both format versions."""
        pytest.importorskip("jsonschema_rs")
        import io

        for path in (TEST_MSEED3_FILE, TEST_MSEED2_FILE):
            buffer = get_test_buffer(path)

            errors, _ = MS3RecordValidator.from_buffer(buffer).validate()
            assert errors == [], f"{path} reported {errors}"

            errors, _ = MS3RecordValidator.from_filelike(io.BytesIO(buffer)).validate()
            assert errors == [], f"{path} (stream) reported {errors}"

            # An empty source has no records to be incomplete.
            assert MS3RecordValidator.from_buffer(b"").validate()[0] == []


class TestMS3RecordValidatorErrorAccumulation:
    """Tests for error accumulation and structure."""

    def test_error_structure(self) -> None:
        """Test that errors are ValidationError instances with correct types."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=True).validate()

        assert len(errors) >= 1
        for error in errors:
            assert isinstance(error, ValidationError)
            assert isinstance(error.offset, int)
            assert isinstance(error.message, str)

    def test_error_fields_populated(self) -> None:
        """Test that ValidationError has reclen when record length is determinable."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=True).validate()

        assert len(errors) >= 1
        for error in errors:
            assert isinstance(error.reclen, int)
            assert error.reclen > 0

    def test_error_offset_tracking(self) -> None:
        """Test that error offsets correspond to record boundaries."""
        corrupted = get_corrupted_record()
        reclen = len(corrupted)

        errors, _ = MS3RecordValidator.from_buffer(corrupted * 3, validate_crc=True).validate()

        assert len(errors) >= 3
        offsets = {e.offset for e in errors}
        assert {0, reclen, 2 * reclen}.issubset(offsets)

    def test_validation_error_is_frozen(self) -> None:
        """Test that ValidationError instances are immutable."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(corrupted, validate_crc=True).validate()

        assert len(errors) >= 1
        with pytest.raises(AttributeError):
            errors[0].message = "tampered"


class TestMS3RecordValidatorMixedData:
    """Tests for handling mixed valid/invalid data."""

    def test_mixed_valid_and_corrupted(self) -> None:
        """Test that valid records are preserved alongside corrupted ones."""
        records = get_test_records(TEST_MSEED3_FILE)
        assert len(records) >= 3

        corrupted = bytearray(records[1])
        corrupted[100] = 0xFF
        corrupted[101] = 0xFF
        corrupted[102] = 0xFF

        mixed_buffer = records[0] + bytes(corrupted) + records[2]

        errors, traces = MS3RecordValidator.from_buffer(mixed_buffer, validate_crc=True).validate()

        assert len(traces) > 0
        assert len(errors) >= 1
        error_offsets = {e.offset for e in errors}
        assert len(records[0]) in error_offsets


class TestMS3RecordValidatorFromFile:
    """Tests for from_file() classmethod."""

    def test_from_file_clean_mseed3(self) -> None:
        """Test validating a clean miniSEED v3 file."""
        pytest.importorskip("jsonschema_rs")
        errors, traces = MS3RecordValidator.from_file(TEST_MSEED3_FILE, unpack_data=True).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_from_file_clean_mseed2(self) -> None:
        """Test validating a clean miniSEED v2 file."""
        pytest.importorskip("jsonschema_rs")
        errors, traces = MS3RecordValidator.from_file(TEST_MSEED2_FILE, unpack_data=True).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_from_file_equivalence_with_from_buffer(self) -> None:
        """Test that from_file and from_buffer produce identical results."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        buf_errors, buf_traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()
        file_errors, file_traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE, unpack_data=True
        ).validate()

        assert len(buf_errors) == len(file_errors)
        assert len(buf_traces) == len(file_traces)

        for traceid_b, traceid_f in zip(buf_traces, file_traces, strict=True):
            assert traceid_b.sourceid == traceid_f.sourceid

    def test_from_file_small_chunk_size_mseed3(self) -> None:
        """Test that small chunk sizes produce correct results for v3."""
        reference_errors, reference_traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE,
            unpack_data=True,
        ).validate()

        errors, traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE,
            chunk_size=256,
            unpack_data=True,
        ).validate()

        assert len(errors) == len(reference_errors)
        assert len(traces) == len(reference_traces)

    def test_from_file_small_chunk_size_mseed2(self) -> None:
        """Test that small chunk sizes produce correct results for v2."""
        reference_errors, reference_traces = MS3RecordValidator.from_file(
            TEST_MSEED2_FILE,
            unpack_data=True,
        ).validate()

        errors, traces = MS3RecordValidator.from_file(
            TEST_MSEED2_FILE,
            chunk_size=512,
            unpack_data=True,
        ).validate()

        assert len(errors) == len(reference_errors)
        assert len(traces) == len(reference_traces)

    def test_from_file_error_tracking(self, tmp_path) -> None:
        """Test that from_file reports byte offsets matching from_buffer."""
        records = get_test_records(TEST_MSEED3_FILE)
        corrupted = bytearray(records[0])
        corrupted[100] = 0xFF
        mixed = bytes(corrupted) + b"".join(records[1:])

        buf_errors, _ = MS3RecordValidator.from_buffer(mixed, validate_crc=True).validate()
        buf_offsets = [e.offset for e in buf_errors]

        tmp_file = tmp_path / "mixed.mseed"
        tmp_file.write_bytes(mixed)

        file_errors, _ = MS3RecordValidator.from_file(str(tmp_file), validate_crc=True).validate()
        file_offsets = [e.offset for e in file_errors]
        assert buf_offsets == file_offsets

    def test_from_file_sample_counts(self) -> None:
        """Test that from_file reports same sample counts as from_buffer for mseed2."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        _, buf_traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()
        _, file_traces = MS3RecordValidator.from_file(TEST_MSEED2_FILE, unpack_data=True).validate()

        buf_samples = sum(seg.samplecnt for tid in buf_traces for seg in tid)
        file_samples = sum(seg.samplecnt for tid in file_traces for seg in tid)

        assert buf_samples == file_samples == 252000

    def test_from_file_nonexistent(self) -> None:
        """Test that a nonexistent file raises FileNotFoundError."""
        validator = MS3RecordValidator.from_file("/nonexistent/path.mseed")

        with pytest.raises(FileNotFoundError):
            validator.validate()

    def test_from_file_non_mseed_content(self, tmp_path) -> None:
        """Test that a file with non-miniSEED content produces a detection error."""
        tmp_file = tmp_path / "bad.mseed"
        tmp_file.write_bytes(b"This is not miniSEED data at all." * 10)

        errors, traces = MS3RecordValidator.from_file(str(tmp_file)).validate()
        assert len(errors) >= 1
        assert len(traces) == 0


class TestMS3RecordValidatorIntegration:
    """Integration tests with real-world scenarios."""

    def test_full_file_validate_mseed3(self) -> None:
        """Test parsing entire miniSEED v3 file and verify sample counts."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()

        total_samples = sum(seg.samplecnt for tid in traces for seg in tid)
        assert total_samples > 0
        assert len(errors) == 0

    def test_full_file_validate_mseed2(self) -> None:
        """Test parsing entire miniSEED v2 file and verify sample counts."""
        pytest.importorskip("jsonschema_rs")
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()

        total_samples = sum(seg.samplecnt for tid in traces for seg in tid)
        assert total_samples == 252000
        assert len(errors) == 0

    def test_multiple_validate_calls_same_validator(self) -> None:
        """Test that validate() can be called multiple times on one instance."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator.from_buffer(buffer)
        errors1, traces1 = validator.validate()
        errors2, traces2 = validator.validate()

        assert len(traces1) == len(traces2)
        assert len(errors1) == len(errors2)


class TestMS3RecordValidatorFromFilelike:
    """Tests for MS3RecordValidator.from_filelike."""

    def test_validate_clean_mseed3(self) -> None:
        """from_filelike on a BytesIO wrapping v3 data produces zero errors."""
        pytest.importorskip("jsonschema_rs")
        import io

        buffer = get_test_buffer(TEST_MSEED3_FILE)
        errors, traces = MS3RecordValidator.from_filelike(
            io.BytesIO(buffer), unpack_data=False
        ).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_matches_from_file(self) -> None:
        """from_filelike produces identical error/trace counts as from_file."""
        import io

        buffer = get_test_buffer(TEST_MSEED3_FILE)

        fl_errors, fl_traces = MS3RecordValidator.from_filelike(
            io.BytesIO(buffer), unpack_data=True
        ).validate()
        f_errors, f_traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE, unpack_data=True
        ).validate()

        assert len(fl_errors) == len(f_errors)
        assert len(fl_traces) == len(f_traces)

    def test_chunk_size_validation(self) -> None:
        """chunk_size <= 0 and > 1 GiB raise ValueError."""
        import io

        fh = io.BytesIO(b"")
        with pytest.raises(ValueError):
            MS3RecordValidator.from_filelike(fh, chunk_size=0)
        with pytest.raises(ValueError):
            MS3RecordValidator.from_filelike(fh, chunk_size=-1)
        with pytest.raises(ValueError):
            MS3RecordValidator.from_filelike(fh, chunk_size=1_073_741_825)

    def test_from_file_propagates_open_errors(self, tmp_path) -> None:
        """File-open failures must propagate as OSError subclasses from
        validate(), not get swallowed into the errors list. The caller relies
        on this to distinguish 'source unavailable' from 'source contains bad
        records'."""

        # FileNotFoundError on missing path.
        with pytest.raises(FileNotFoundError):
            MS3RecordValidator.from_file(tmp_path / "does_not_exist.mseed").validate()

        # Pointing at a directory: IsADirectoryError on POSIX, PermissionError
        # on Windows. The contract we care about is that it's an OSError
        # subclass propagated through validate(), not swallowed.
        with pytest.raises((IsADirectoryError, PermissionError)):
            MS3RecordValidator.from_file(tmp_path).validate()

    def test_from_filelike_rejects_non_filelike(self) -> None:
        """Without a .read() method, fail fast at the factory rather than
        with a context-free AttributeError deep inside the iterator on the
        first chunk read."""

        class _NoReadMethod:
            pass

        class _NonCallableRead:
            read = "this is not a method"

        for bad in (
            b"raw bytes",
            "/some/path",
            None,
            42,
            [1, 2],
            _NoReadMethod(),
            _NonCallableRead(),
        ):
            with pytest.raises(TypeError, match="callable .read"):
                MS3RecordValidator.from_filelike(bad)

    def test_non_seekable_stream(self) -> None:
        """from_filelike works with a forward-only stream that has no seek/tell."""
        pytest.importorskip("jsonschema_rs")

        class _ReadOnly:
            """Wraps bytes, exposing only .read(n) — no seek or tell."""

            def __init__(self, data: bytes) -> None:
                self._data = memoryview(data)
                self._pos = 0

            def read(self, n: int = -1) -> bytes:
                if n < 0:
                    chunk = bytes(self._data[self._pos :])
                    self._pos = len(self._data)
                else:
                    chunk = bytes(self._data[self._pos : self._pos + n])
                    self._pos += len(chunk)
                return chunk

        buffer = get_test_buffer(TEST_MSEED3_FILE)
        errors, traces = MS3RecordValidator.from_filelike(
            _ReadOnly(buffer), unpack_data=False
        ).validate()

        assert len(errors) == 0
        assert len(traces) > 0


class TestMS3RecordValidatorUndetectableInput:
    """Detection failures must be reported once they are conclusive, rather
    than reading the rest of the stream in the hope of resolving them."""

    def test_non_mseed_stream_reported_without_reading_it_all(self) -> None:
        """Detection needs only MINRECLEN bytes to rule out miniSEED."""
        chunk_size = 65536
        stream = _CountingStream(filler=64 * chunk_size)

        errors, traces = MS3RecordValidator.from_filelike(stream, chunk_size=chunk_size).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert "No miniSEED detected" in errors[0].message
        assert errors[0].offset == 0
        assert stream.pos <= chunk_size

    def test_oversized_record_length_reported(self) -> None:
        """A payload length beyond MAXRECLEN can never be satisfied, so it is
        an error rather than a reason to keep reading."""
        chunk_size = 65536
        stream = _CountingStream(
            head=_get_record_with_oversized_datalength(), filler=64 * chunk_size
        )

        errors, traces = MS3RecordValidator.from_filelike(stream, chunk_size=chunk_size).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert "exceeds the maximum supported" in errors[0].message
        assert errors[0].offset == 0
        assert stream.pos <= chunk_size

    def test_oversized_record_length_reported_from_buffer(self) -> None:
        """The buffer source reports the same oversized length as the stream
        source."""
        errors, traces = MS3RecordValidator.from_buffer(
            _get_record_with_oversized_datalength()
        ).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert "exceeds the maximum supported" in errors[0].message
        assert errors[0].offset == 0

    def test_record_spanning_several_chunks_still_validates(self) -> None:
        """The early report must not pre-empt the legitimate 'need more bytes'
        path, where a record is longer than a single chunk."""
        pytest.importorskip("jsonschema_rs")
        stream = _CountingStream(head=get_test_buffer(TEST_MSEED3_FILE))

        errors, traces = MS3RecordValidator.from_filelike(stream, chunk_size=64).validate()

        assert len(errors) == 0
        assert len(traces) > 0


class TestMS3RecordValidatorFutureData:
    """Tests for the future-data check and the future_data_tolerance option.

    The bundled test data is from 2010, so it is never "future" against a real
    clock.  Most of these tests therefore patch ``msrecord_validator.system_time``
    to move "now" relative to the data instead of fabricating future records.
    """

    _FUTURE_MSG = "future data"

    def _first_record(self) -> tuple[bytes, int]:
        """Return (record_bytes, endtime_ns) for the first v3 test record.

        Both values are read inside the reader's iteration, since a yielded
        MS3Record is only valid while the reader is alive.
        """
        from pymseed import MS3Record

        for msr in MS3Record.from_file(TEST_MSEED3_FILE):
            return msr.record, msr.endtime

        raise AssertionError(f"no records in {TEST_MSEED3_FILE}")

    def test_clean_historical_data_has_no_future_errors(self) -> None:
        """Data from the past passes the check with the default tolerance."""
        pytest.importorskip("jsonschema_rs")
        for path in (TEST_MSEED3_FILE, TEST_MSEED2_FILE):
            errors, _ = MS3RecordValidator.from_buffer(get_test_buffer(path)).validate()
            assert errors == []

    def test_default_tolerance_is_five_seconds(self) -> None:
        """The documented default is 5 seconds."""
        validator = MS3RecordValidator.from_buffer(b"")
        assert validator._future_data_tolerance == 5.0
        assert validator._future_tolerance_ns == 5_000_000_000

    def test_every_future_record_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With "now" set before the data, every record is flagged."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv
        from pymseed.util import timestr2nstime

        past = timestr2nstime("2009-01-01T00:00:00Z")
        monkeypatch.setattr(mv, "system_time", lambda: past)

        records = get_test_records(TEST_MSEED3_FILE)
        errors, traces = MS3RecordValidator.from_buffer(
            get_test_buffer(TEST_MSEED3_FILE)
        ).validate()

        assert len(errors) == len(records)
        assert all(self._FUTURE_MSG in e.message for e in errors)
        # Flagged records are still parsed into the trace list.
        assert len(traces) == 3

    def test_tolerance_none_disables_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """future_data_tolerance=None skips the check entirely."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv
        from pymseed.util import timestr2nstime

        past = timestr2nstime("2009-01-01T00:00:00Z")
        monkeypatch.setattr(mv, "system_time", lambda: past)

        errors, _ = MS3RecordValidator.from_buffer(
            get_test_buffer(TEST_MSEED3_FILE), future_data_tolerance=None
        ).validate()

        assert errors == []

    @pytest.mark.parametrize(
        "tolerance,expected_errors",
        [
            (5.0, 0),  # 3 s ahead is within the default tolerance
            (3.5, 0),  # just inside
            (1.0, 1),  # outside
            (0, 1),  # any data past "now" is reported
        ],
    )
    def test_tolerance_boundary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tolerance: float,
        expected_errors: int,
    ) -> None:
        """A record ending 3 s past "now" is reported only when the tolerance
        is smaller than the excess."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv

        record, endtime = self._first_record()
        now = endtime - 3 * 1_000_000_000
        monkeypatch.setattr(mv, "system_time", lambda: now)

        errors, _ = MS3RecordValidator.from_buffer(
            record, future_data_tolerance=tolerance
        ).validate()

        assert len(errors) == expected_errors

    def test_stale_cutoff_is_refreshed_before_reporting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A record that only looks future against a stale clock reading is
        cleared by the re-read, not reported."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv
        from pymseed.util import system_time as real_system_time
        from pymseed.util import timestr2nstime

        calls: list[int] = []
        past = timestr2nstime("2009-01-01T00:00:00Z")

        def creeping_clock() -> int:
            calls.append(1)
            # First reading (the pre-loop snapshot) is stale; the re-read on a
            # suspected violation returns the true current time.
            return past if len(calls) == 1 else real_system_time()

        monkeypatch.setattr(mv, "system_time", creeping_clock)

        record, _ = self._first_record()
        errors, _ = MS3RecordValidator.from_buffer(record).validate()

        assert errors == []
        assert len(calls) == 2

    def test_real_future_record_is_detected(self) -> None:
        """A record actually stamped an hour ahead is caught against the real
        system clock, with no clock patching."""
        from pymseed import MS3Record
        from pymseed.util import system_time

        msr = MS3Record()
        msr.sourceid = "FDSN:XX_TEST__L_H_Z"
        msr.samprate = 1
        msr.starttime = system_time() + 3600 * 1_000_000_000
        buffer = b"".join(msr.generate(data_samples=[1, 2, 3, 4, 5], sample_type="i"))

        errors, _ = MS3RecordValidator.from_buffer(buffer).validate()
        assert len(errors) == 1
        assert self._FUTURE_MSG in errors[0].message

        # A tolerance wider than the offset accepts the same record.
        tolerated, _ = MS3RecordValidator.from_buffer(buffer, future_data_tolerance=7200).validate()
        assert tolerated == []

    def test_future_error_fields_populated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The future-data ValidationError carries the record context."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv
        from pymseed.util import timestr2nstime

        past = timestr2nstime("2009-01-01T00:00:00Z")
        monkeypatch.setattr(mv, "system_time", lambda: past)

        record, _ = self._first_record()
        # Two copies, so the second error must report a non-zero offset.
        errors, _ = MS3RecordValidator.from_buffer(record + record).validate()

        assert len(errors) == 2
        for error, expected_offset in zip(errors, (0, len(record)), strict=True):
            assert error.offset == expected_offset
            assert error.reclen == len(record)
            assert error.sourceid == "FDSN:IU_COLA_00_B_H_1"
            assert error.starttime is not None and error.starttime > 0
            # The end time and the tolerance both appear in the message.
            assert "2010" in error.message
            assert "5.0 seconds" in error.message

    @pytest.mark.parametrize("tolerance", [-1, -0.001, float("inf"), float("nan")])
    def test_invalid_tolerance_rejected(self, tolerance: float) -> None:
        """Negative, infinite and NaN tolerances are rejected at construction."""
        with pytest.raises(ValueError, match="future_data_tolerance"):
            MS3RecordValidator.from_buffer(b"", future_data_tolerance=tolerance)

    def test_non_numeric_tolerance_rejected(self) -> None:
        """A non-numeric tolerance raises TypeError."""
        with pytest.raises(TypeError):
            MS3RecordValidator.from_buffer(b"", future_data_tolerance="5")  # type: ignore[arg-type]

    def test_unformattable_end_time_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An end time libmseed cannot format still yields an error message
        rather than propagating ValueError out of validate()."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import msrecord_validator as mv

        def unformattable(_nstime: int) -> str:
            raise ValueError("simulated conversion failure")

        monkeypatch.setattr(mv, "nstime2timestr", unformattable)

        record, endtime = self._first_record()
        monkeypatch.setattr(mv, "system_time", lambda: endtime - 3600 * 1_000_000_000)

        errors, _ = MS3RecordValidator.from_buffer(record).validate()

        assert len(errors) == 1
        assert f"{endtime} ns" in errors[0].message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
