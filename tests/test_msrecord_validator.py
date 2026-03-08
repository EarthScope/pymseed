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


class TestMS3RecordValidatorBasic:
    """Basic functionality tests."""

    def test_validate_clean_buffer_mseed3(self) -> None:
        """Test parsing a clean miniSEED v3 buffer with no errors."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=False
        ).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_clean_buffer_mseed2(self) -> None:
        """Test parsing a clean miniSEED v2 buffer with no errors."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=False
        ).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_with_unpack_data(self) -> None:
        """Test parsing with data unpacking enabled produces no errors on clean data."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True
        ).validate()

        assert len(traces) > 0
        assert len(errors) == 0

    def test_validate_empty_buffer(self) -> None:
        """Test parsing an empty buffer produces no errors and no traces."""
        errors, traces = MS3RecordValidator.from_buffer(b"").validate()

        assert len(traces) == 0
        assert len(errors) == 0

    def test_return_trace_list_false(self) -> None:
        """Test that return_trace_list=False returns None for traces."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, return_trace_list=False
        ).validate()

        assert traces is None
        assert len(errors) == 0

    def test_validate_garbage_data(self) -> None:
        """Test that non-miniSEED data produces a detection error."""
        errors, traces = MS3RecordValidator.from_buffer(b"X" * 256).validate()

        assert len(traces) == 0
        assert len(errors) == 1
        assert "No miniSEED detected" in errors[0].message

    def test_validate_incomplete_record(self) -> None:
        """Test that a truncated record (header parseable, body missing) stops silently."""
        records = get_test_records(TEST_MSEED3_FILE)
        # 60 bytes: enough for ms3_detect to read the header and return a record
        # length, but far less than the actual record — should stop without error.
        truncated = records[0][:60]

        errors, traces = MS3RecordValidator.from_buffer(truncated).validate()

        assert len(traces) == 0
        assert len(errors) == 0


class TestMS3RecordValidatorCRCValidation:
    """Tests for CRC validation error handling."""

    def test_crc_error_detected_and_logged(self) -> None:
        """Test that CRC errors produce a validation error mentioning CRC."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=True
        ).validate()

        assert len(errors) >= 1
        crc_errors = [e for e in errors if "CRC" in e.message]
        assert len(crc_errors) >= 1

    def test_crc_validation_disabled(self) -> None:
        """Test that disabling CRC validation suppresses CRC errors."""
        corrupted = get_corrupted_record()

        errors_with, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=True
        ).validate()
        errors_without, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=False
        ).validate()

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
            buffer, unpack_data=True, validate_crc=False,
        ).validate()

        assert len(errors) >= 1

    def test_unpack_data_disabled_suppresses_errors(self) -> None:
        """Test that unpack_data=False suppresses data unpacking errors."""
        buffer = _get_record_with_bad_encoding()

        errors_on, _ = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True, validate_crc=False,
        ).validate()
        errors_off, _ = MS3RecordValidator.from_buffer(
            buffer, unpack_data=False, validate_crc=False,
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
            buffer, validate_crc=False, validate_extra_headers=True,
        ).validate()
        errors_off, _ = MS3RecordValidator.from_buffer(
            buffer, validate_crc=False, validate_extra_headers=False,
        ).validate()

        eh_errors_on = [e for e in errors_on if "Extra headers" in e.message]
        eh_errors_off = [e for e in errors_off if "Extra headers" in e.message]
        assert len(eh_errors_on) >= 1
        assert len(eh_errors_off) == 0


class TestMS3RecordValidatorPartialData:
    """Tests for handling partial/incomplete data."""

    def test_partial_record_at_end(self) -> None:
        """Test handling of incomplete record at end of buffer."""
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
        assert len(errors) == 0


class TestMS3RecordValidatorErrorAccumulation:
    """Tests for error accumulation and structure."""

    def test_error_structure(self) -> None:
        """Test that errors are ValidationError instances with correct types."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=True
        ).validate()

        assert len(errors) >= 1
        for error in errors:
            assert isinstance(error, ValidationError)
            assert isinstance(error.offset, int)
            assert isinstance(error.message, str)

    def test_error_fields_populated(self) -> None:
        """Test that ValidationError has reclen when record length is determinable."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=True
        ).validate()

        assert len(errors) >= 1
        for error in errors:
            assert isinstance(error.reclen, int)
            assert error.reclen > 0

    def test_error_offset_tracking(self) -> None:
        """Test that error offsets correspond to record boundaries."""
        corrupted = get_corrupted_record()
        reclen = len(corrupted)

        errors, _ = MS3RecordValidator.from_buffer(
            corrupted * 3, validate_crc=True
        ).validate()

        assert len(errors) >= 3
        offsets = {e.offset for e in errors}
        assert {0, reclen, 2 * reclen}.issubset(offsets)

    def test_validation_error_is_frozen(self) -> None:
        """Test that ValidationError instances are immutable."""
        corrupted = get_corrupted_record()

        errors, _ = MS3RecordValidator.from_buffer(
            corrupted, validate_crc=True
        ).validate()

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

        errors, traces = MS3RecordValidator.from_buffer(
            mixed_buffer, validate_crc=True
        ).validate()

        assert len(traces) > 0
        assert len(errors) >= 1
        error_offsets = {e.offset for e in errors}
        assert len(records[0]) in error_offsets


class TestMS3RecordValidatorFromFile:
    """Tests for from_file() classmethod."""

    def test_from_file_clean_mseed3(self) -> None:
        """Test validating a clean miniSEED v3 file."""
        errors, traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE, unpack_data=True
        ).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_from_file_clean_mseed2(self) -> None:
        """Test validating a clean miniSEED v2 file."""
        errors, traces = MS3RecordValidator.from_file(
            TEST_MSEED2_FILE, unpack_data=True
        ).validate()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        assert len(errors) == 0

    def test_from_file_equivalence_with_from_buffer(self) -> None:
        """Test that from_file and from_buffer produce identical results."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        buf_errors, buf_traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True
        ).validate()
        file_errors, file_traces = MS3RecordValidator.from_file(
            TEST_MSEED3_FILE, unpack_data=True
        ).validate()

        assert len(buf_errors) == len(file_errors)
        assert len(buf_traces) == len(file_traces)

        for traceid_b, traceid_f in zip(buf_traces, file_traces):
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

        buf_errors, _ = MS3RecordValidator.from_buffer(
            mixed, validate_crc=True
        ).validate()
        buf_offsets = [e.offset for e in buf_errors]

        tmp_file = tmp_path / "mixed.mseed"
        tmp_file.write_bytes(mixed)

        file_errors, _ = MS3RecordValidator.from_file(
            str(tmp_file), validate_crc=True
        ).validate()
        file_offsets = [e.offset for e in file_errors]
        assert buf_offsets == file_offsets

    def test_from_file_sample_counts(self) -> None:
        """Test that from_file reports same sample counts as from_buffer for mseed2."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        _, buf_traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True
        ).validate()
        _, file_traces = MS3RecordValidator.from_file(
            TEST_MSEED2_FILE, unpack_data=True
        ).validate()

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
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True
        ).validate()

        total_samples = sum(seg.samplecnt for tid in traces for seg in tid)
        assert total_samples > 0
        assert len(errors) == 0

    def test_full_file_validate_mseed2(self) -> None:
        """Test parsing entire miniSEED v2 file and verify sample counts."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        errors, traces = MS3RecordValidator.from_buffer(
            buffer, unpack_data=True
        ).validate()

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
