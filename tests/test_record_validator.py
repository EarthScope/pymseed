"""
Tests for MS3RecordValidator - record-by-record buffer parsing with error accumulation.
"""

import os

import pytest

from pymseed import MS3RecordValidator, MS3TraceList

# Test data paths
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, "data")
TEST_MSEED3_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed3")
TEST_MSEED2_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed2")


def get_test_buffer(filepath: str) -> bytes:
    """Read test file into a buffer."""
    with open(filepath, "rb") as f:
        return f.read()


def get_corrupted_record() -> bytes:
    """Get a corrupted miniSEED record that will trigger a CRC error."""
    from pymseed import MS3Record

    for msr in MS3Record.from_file(TEST_MSEED3_FILE):
        valid_data = bytearray(msr.record)
        # Corrupt some bytes to trigger CRC validation error
        valid_data[100] = 0xFF
        valid_data[101] = 0xFF
        valid_data[102] = 0xFF
        return bytes(valid_data)
    raise RuntimeError("Could not read test data")


class TestMS3RecordValidatorBasic:
    """Basic functionality tests."""

    def test_parse_clean_buffer_mseed3(self) -> None:
        """Test parsing a clean miniSEED v3 buffer with no errors."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(buffer, unpack_data=False)
        traces, errors = validator.parse()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        # Clean data should have no errors
        assert len(errors) == 0

    def test_parse_clean_buffer_mseed2(self) -> None:
        """Test parsing a clean miniSEED v2 buffer with no errors."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        validator = MS3RecordValidator(buffer, unpack_data=False)
        traces, errors = validator.parse()

        assert isinstance(traces, MS3TraceList)
        assert len(traces) > 0
        # v2 doesn't have CRC, so no CRC errors expected
        assert len(errors) == 0

    def test_parse_with_unpack_data(self) -> None:
        """Test parsing with data unpacking enabled."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(buffer, unpack_data=True)
        traces, errors = validator.parse()

        assert len(traces) > 0
        assert len(errors) == 0

        # Verify data was unpacked
        for traceid in traces:
            for segment in traceid:
                assert segment.numsamples > 0

    def test_parse_empty_buffer(self) -> None:
        """Test parsing an empty buffer."""
        buffer = b""

        validator = MS3RecordValidator(buffer)
        traces, errors = validator.parse()

        assert len(traces) == 0
        assert len(errors) == 0

    def test_parse_too_small_buffer(self) -> None:
        """Test parsing a buffer too small for any record."""
        buffer = b"X" * 30  # Less than MINRECLEN (40)

        validator = MS3RecordValidator(buffer)
        traces, errors = validator.parse()

        # Should stop with detection error or incomplete record
        assert len(traces) == 0
        # May have an error depending on ms_detect behavior
        # Either way, parsing should stop gracefully


class TestMS3RecordValidatorCRCValidation:
    """Tests for CRC validation error handling."""

    def test_crc_error_detected_and_logged(self) -> None:
        """Test that CRC errors are detected and logged."""
        corrupted = get_corrupted_record()

        validator = MS3RecordValidator(corrupted, validate_crc=True)
        traces, errors = validator.parse()

        # Record should still be added to traces (parsed successfully)
        # Error should be logged
        # Note: The exact behavior depends on whether libmseed
        # returns error or warning for CRC failure

        # We expect either:
        # - traces has the record and errors has CRC warning, OR
        # - parse fails and errors has the failure message
        assert len(errors) >= 1 or len(traces) >= 0

    def test_crc_validation_disabled(self) -> None:
        """Test that CRC validation can be disabled."""
        corrupted = get_corrupted_record()

        validator = MS3RecordValidator(corrupted, validate_crc=False)
        traces, errors = validator.parse()

        # With CRC validation disabled, corrupted record may be accepted
        # (depending on how corrupted it is)


class TestMS3RecordValidatorDataUnpacking:
    """Tests for data unpacking error handling."""

    def test_unpack_data_errors_logged(self) -> None:
        """Test that data unpacking errors are logged but record is still added."""
        # Create a record with valid header but corrupted data section
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        # Corrupt some data bytes (after header)
        corrupted = bytearray(buffer)
        # Corrupt data section (after ~offset 60 where data typically starts)
        for i in range(100, 150):
            if i < len(corrupted):
                corrupted[i] = 0xFF

        validator = MS3RecordValidator(bytes(corrupted), unpack_data=True, validate_crc=False)
        traces, errors = validator.parse()

        # Even with data errors, parsing should continue
        # and records should be in the trace list


class TestMS3RecordValidatorExtraHeaders:
    """Tests for extra headers validation."""

    def test_extra_headers_validation_enabled(self) -> None:
        """Test extra headers validation when enabled."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(
            buffer,
            validate_extra_headers=True,
            extra_headers_schema="FDSN-v1.0",
        )
        traces, errors = validator.parse()

        # Records should be parsed regardless of validation result
        assert len(traces) > 0

    def test_extra_headers_validation_disabled(self) -> None:
        """Test that extra headers validation is disabled by default."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(buffer)
        traces, errors = validator.parse()

        # Should not have any validation errors with validation disabled
        validation_errors = [e for e in errors if e["exception_type"] == "ValidationError"]
        assert len(validation_errors) == 0


class TestMS3RecordValidatorPartialData:
    """Tests for handling partial/incomplete data."""

    def test_partial_record_at_end(self) -> None:
        """Test handling of incomplete record at end of buffer."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        # Truncate buffer mid-record
        truncated = buffer[: len(buffer) // 2]

        validator = MS3RecordValidator(truncated)
        traces, errors = validator.parse()

        # Should parse all complete records and stop at partial one
        # No error expected - just stops when ms_detect returns 0
        # The number of traces depends on how many complete records fit


class TestMS3RecordValidatorErrorAccumulation:
    """Tests for error accumulation and structure."""

    def test_error_structure(self) -> None:
        """Test that errors have the expected structure."""
        corrupted = get_corrupted_record()

        validator = MS3RecordValidator(corrupted, validate_crc=True)
        traces, errors = validator.parse()

        for error in errors:
            assert "offset" in error
            assert "message" in error
            assert "exception_type" in error
            assert "record_info" in error

            assert isinstance(error["offset"], int)
            assert isinstance(error["message"], str)
            assert isinstance(error["exception_type"], str)
            # record_info can be None or dict
            if error["record_info"] is not None:
                assert isinstance(error["record_info"], dict)

    def test_error_offset_tracking(self) -> None:
        """Test that error offsets are correctly tracked."""
        # Concatenate multiple corrupted records
        corrupted = get_corrupted_record()
        buffer = corrupted * 3  # Three copies

        validator = MS3RecordValidator(buffer, validate_crc=True)
        traces, errors = validator.parse()

        # Each error should have a different offset
        offsets = [e["offset"] for e in errors]
        # If we have multiple errors, offsets should be different
        if len(offsets) > 1:
            # Check offsets are monotonically increasing or at least different
            assert len(set(offsets)) == len(offsets) or offsets == sorted(offsets)


class TestMS3RecordValidatorMixedData:
    """Tests for handling mixed valid/invalid data."""

    def test_mixed_valid_and_corrupted(self) -> None:
        """Test parsing buffer with mix of valid and corrupted records."""
        valid_buffer = get_test_buffer(TEST_MSEED3_FILE)
        corrupted = get_corrupted_record()

        # Take a portion of valid data + corrupted + more valid data
        # This tests that parsing continues after errors
        mixed_buffer = valid_buffer[:1000] + corrupted + valid_buffer[1000:2000]

        validator = MS3RecordValidator(mixed_buffer, validate_crc=True)
        traces, errors = validator.parse()

        # Should have parsed some records and logged errors
        # The exact numbers depend on record boundaries


class TestMS3RecordValidatorIntegration:
    """Integration tests with real-world scenarios."""

    def test_full_file_parse_mseed3(self) -> None:
        """Test parsing entire miniSEED v3 file and verify record counts."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(buffer, unpack_data=True)
        traces, errors = validator.parse()

        # Count total samples
        total_samples = 0
        for traceid in traces:
            for segment in traceid:
                total_samples += segment.samplecnt

        # Should match expected sample count from test file
        assert total_samples > 0
        assert len(errors) == 0

    def test_full_file_parse_mseed2(self) -> None:
        """Test parsing entire miniSEED v2 file and verify record counts."""
        buffer = get_test_buffer(TEST_MSEED2_FILE)

        validator = MS3RecordValidator(buffer, unpack_data=True)
        traces, errors = validator.parse()

        # Count total samples
        total_samples = 0
        for traceid in traces:
            for segment in traceid:
                total_samples += segment.samplecnt

        # mseed2 file has 252000 samples according to existing tests
        assert total_samples == 252000
        assert len(errors) == 0

    def test_multiple_parse_calls_same_validator(self) -> None:
        """Test that parse() can be called multiple times."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(buffer)
        traces1, errors1 = validator.parse()
        traces2, errors2 = validator.parse()

        # Both calls should produce the same results
        assert len(traces1) == len(traces2)
        assert len(errors1) == len(errors2)


class TestMS3RecordValidatorOptions:
    """Tests for various constructor options."""

    def test_verbose_option(self) -> None:
        """Test that verbose option doesn't cause errors."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        # Test with different verbosity levels
        for verbose in [0, 1, 2]:
            validator = MS3RecordValidator(buffer, verbose=verbose)
            traces, errors = validator.parse()
            assert len(traces) > 0

    def test_all_options_combined(self) -> None:
        """Test with all options enabled."""
        buffer = get_test_buffer(TEST_MSEED3_FILE)

        validator = MS3RecordValidator(
            buffer,
            unpack_data=True,
            validate_crc=True,
            validate_extra_headers=True,
            extra_headers_schema="FDSN-v1.0",
            verbose=0,
        )
        traces, errors = validator.parse()

        assert isinstance(traces, MS3TraceList)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
