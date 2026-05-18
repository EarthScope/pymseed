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

    def test_configure_logging_rejects_negative_max_messages(self) -> None:
        """Negative max_messages is silently ignored by libmseed; the wrapper
        must surface this as a clear ValueError instead. Zero remains valid
        — it's the documented 'disable registry' mode."""
        with pytest.raises(ValueError, match="max_messages must be >= 0"):
            configure_logging(max_messages=-1)
        with pytest.raises(ValueError, match="max_messages must be >= 0"):
            configure_logging(max_messages=-100)

        # max_messages=0 is legal (disables the registry); should not raise.
        configure_logging(max_messages=0)
        # Restore a sane registry for any subsequent tests in this class.
        configure_logging(max_messages=10)

    def test_configure_logging_twice_with_distinct_prefixes(self) -> None:
        """Reconfiguring with different prefixes must not corrupt libmseed's
        stored prefix pointer.

        libmseed keeps the prefix by pointer (not by copy). If the wrapper
        drops the previous prefix bytes from TLS *before* updating libmseed
        to a new pointer, libmseed briefly holds a dangling pointer. The
        next log emission can then crash, scramble, or leak the freed
        memory. We force several real log emissions across reconfiguration
        cycles and force CPython to actually free dangling objects so any
        latent use-after-free surfaces as a crash here rather than in
        production.
        """
        import gc

        from pymseed import MS3Record

        for cycle in range(5):
            configure_logging(
                log_prefix=f"LOG-{cycle}-{'X' * cycle}: ",
                error_prefix=f"ERR-{cycle}-{'Y' * cycle}: ",
            )
            # Force any newly-unreferenced bytes objects to actually be reclaimed.
            gc.collect()

            # Generate a real error message via libmseed and drain it. If the
            # prefix pointer is dangling, libmseed will dereference freed
            # memory while constructing the log message.
            clear_error_messages()
            try:
                for _ in MS3Record.from_buffer(get_corrupted_record()):
                    pass
            except Exception:
                pass
            # Drain so the message store doesn't fill up over cycles.
            get_error_messages()

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
        from pymseed import MiniSEEDError, MS3Record

        # Clear any existing messages
        clear_error_messages()

        # Get a corrupted record that will trigger CRC error
        corrupted_data = get_corrupted_record()

        with pytest.raises(MiniSEEDError):
            for _ in MS3Record.from_buffer(corrupted_data, unpack_data=True):
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
        from pymseed import MiniSEEDError, MS3Record

        clear_error_messages()

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate multiple errors by trying to parse corrupted data multiple times
        for _ in range(3):
            try:
                for _ in MS3Record.from_buffer(corrupted_data, unpack_data=True):
                    pass
            except MiniSEEDError:
                pass

        messages = get_error_messages()

        # Should have captured messages from all three attempts
        assert len(messages) >= 3

    def test_clear_removes_all_messages(self) -> None:
        """Test that clear_error_messages removes all messages."""
        from pymseed import MiniSEEDError, MS3Record

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate some error messages
        try:
            for _ in MS3Record.from_buffer(corrupted_data, unpack_data=True):
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
        from pymseed import MiniSEEDError, MS3Record

        clear_error_messages()

        # Get corrupted data
        corrupted_data = get_corrupted_record()

        # Generate an error
        try:
            for _ in MS3Record.from_buffer(corrupted_data, unpack_data=True):
                pass
        except MiniSEEDError:
            pass

        # Get messages
        messages = get_error_messages()
        assert len(messages) >= 1

        # Getting again should return empty list
        messages2 = get_error_messages()
        assert messages2 == []
