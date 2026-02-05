"""
Tests for pymseed logging capture functionality.
"""

import os

import pytest

from pymseed import (
    clear_error_messages,
    configure_logging,
    get_error_messages,
)

# Path to test data
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TEST_MSEED3_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed3")


def get_corrupted_record() -> bytes:
    """Get a corrupted miniSEED record that will trigger a CRC error."""
    from pymseed import MS3Record

    # Read a valid miniSEED record
    for msr in MS3Record.from_file(TEST_MSEED3_FILE):
        valid_data = bytearray(msr.record)
        # Corrupt some bytes to trigger CRC validation error
        valid_data[100] = 0xFF
        valid_data[101] = 0xFF
        valid_data[102] = 0xFF
        return bytes(valid_data)
    raise RuntimeError("Could not read test data")


class TestLoggingCapture:
    """Tests for libmseed log message capture."""

    def setup_method(self) -> None:
        """Clear any existing log messages before each test."""
        clear_error_messages()

    def test_configure_logging_is_idempotent(self) -> None:
        """Test that calling configure_logging multiple times is safe."""
        # Should not raise
        configure_logging()
        configure_logging()
        configure_logging()

    def test_get_error_messages_returns_empty_list_when_empty(self) -> None:
        """Test that get_error_messages returns empty list when no messages."""
        result = get_error_messages()
        assert result == []

    def test_clear_error_messages_returns_zero_when_empty(self) -> None:
        """Test that clear_error_messages returns 0 when no messages."""
        count = clear_error_messages()
        assert count == 0

    def test_capture_error_from_corrupted_record(self) -> None:
        """Test that parsing corrupted miniSEED data generates captured errors."""
        from pymseed import MS3Record
        from pymseed import MiniSEEDError

        # Clear any existing messages
        clear_error_messages()

        # Get a corrupted record that will trigger CRC error
        corrupted_data = get_corrupted_record()

        with pytest.raises(MiniSEEDError):
            with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                for _ in reader:
                    pass

        # Check that error messages were captured
        messages = get_error_messages()

        # We should have at least one error message
        assert len(messages) >= 1

        # Check message structure
        for text in messages:
            assert isinstance(text, str)
            assert "CRC" in text  # Should mention CRC error

    def test_capture_multiple_errors(self) -> None:
        """Test that multiple errors are captured."""
        from pymseed import MS3Record
        from pymseed import MiniSEEDError

        clear_error_messages()

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate multiple errors by trying to parse corrupted data multiple times
        for _ in range(3):
            try:
                with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                    for _ in reader:
                        pass
            except MiniSEEDError:
                pass

        messages = get_error_messages()

        # Should have captured messages from all three attempts
        assert len(messages) >= 3

    def test_clear_removes_all_messages(self) -> None:
        """Test that clear_error_messages removes all messages."""
        from pymseed import MS3Record
        from pymseed import MiniSEEDError

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate some error messages
        try:
            with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                for _ in reader:
                    pass
        except MiniSEEDError:
            pass

        # Clear them
        cleared = clear_error_messages()
        assert cleared >= 1

        # Verify empty
        messages = get_error_messages()
        assert messages == []

    def test_get_error_messages_removes_messages(self) -> None:
        """Test that getting messages removes them from the registry."""
        from pymseed import MS3Record
        from pymseed import MiniSEEDError

        clear_error_messages()

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate an error
        try:
            with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                for _ in reader:
                    pass
        except MiniSEEDError:
            pass

        # Get messages
        messages = get_error_messages()
        assert len(messages) >= 1

        # Getting again should return empty list
        messages2 = get_error_messages()
        assert messages2 == []
