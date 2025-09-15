"""
Core MS3RecordBufferReader implementation for pymseed

"""

from typing import Any, Optional

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .msrecord import MS3Record


class MS3RecordBufferReader:
    """Read miniSEED records sequentially from a memory buffer.

    Use MS3Record.from_buffer() instead of this class directly.

    This class provides an efficient way to read miniSEED records from in-memory
    buffers such as bytearray, bytes, memoryview, numpy arrays, or any object
    that supports the buffer protocol. The reader maintains an internal offset
    and can be used as an iterator or context manager.

    The buffer is read-only and will not be modified by this class. Records are
    parsed sequentially from the beginning of the buffer using the underlying
    libmseed library.

    Args:
        buffer: A buffer-like object containing miniSEED records. Must support
            the buffer protocol (e.g., bytearray, bytes, memoryview, numpy.ndarray).
        unpack_data (bool, optional): If True, decode and unpack the data samples from each record.
            If False, only header information is parsed. Default is False.
        validate_crc (bool, optional): If True, validate CRC checksums when present in records.
            miniSEED v3 records contain CRCs, but v2 records do not. Default is True.
        verbose (int, optional): Verbosity level for libmseed operations. Higher values produce more
            diagnostic output. Default is 0 (silent).

    Raises:
        MiniSEEDError: If there are errors parsing records from the buffer.

    Examples:
        Reading records from a raw buffer (bytes-like object):

    >>> from pymseed import MS3Record

    >>> with open('examples/example_data.mseed', 'rb') as f:
    ...     buffer = f.read()

    >>> with MS3Record.from_buffer(buffer, unpack_data=True) as reader:
    ...     total_samples = 0
    ...     for record in reader:
    ...         total_samples += record.numsamples
    ...     print(f"Total samples: {total_samples}")
    Total samples: 12600

    Notes:
        Once a reader reaches the end of the buffer, it cannot be reset

    See Also:
        MS3Record.from_buffer(): use this instead of MS3RecordBufferReader directly

    """

    def __init__(
        self,
        buffer: Any,
        unpack_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> None:
        self._msr_ptr = ffi.new("MS3Record **")
        self._buffer_ptr = ffi.from_buffer(buffer)
        self._buffer_offset = 0
        self.verbose = verbose

        # Construct parse flags
        self.parse_flags = 0
        if unpack_data:
            self.parse_flags |= clibmseed.MSF_UNPACKDATA
        if validate_crc:
            self.parse_flags |= clibmseed.MSF_VALIDATECRC

    def __enter__(self) -> "MS3RecordBufferReader":
        """Context manager entry point - returns self for use in 'with' statements."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Context manager exit point - ensures proper cleanup by calling close()."""
        self.close()

    def __iter__(self) -> "MS3RecordBufferReader":
        """Iterator protocol - allows the reader to be used in for loops."""
        return self

    def __next__(self) -> MS3Record:
        """Iterator protocol - returns the next record or raises StopIteration."""
        next = self.read()
        if next is not None:
            return next
        else:
            raise StopIteration

    def read(self) -> Optional[MS3Record]:
        """Read the next miniSEED record from the buffer"""
        remaining_bytes = len(self._buffer_ptr) - self._buffer_offset
        if remaining_bytes < clibmseed.MINRECLEN:
            return None

        status = clibmseed.msr3_parse(
            self._buffer_ptr + self._buffer_offset,
            remaining_bytes,
            self._msr_ptr,
            self.parse_flags,
            self.verbose,
        )

        if status == clibmseed.MS_NOERROR:
            self._buffer_offset += self._msr_ptr[0].reclen
            return MS3Record(recordptr=self._msr_ptr[0])
        elif status > 0:  # Record detected but not enough data
            return None
        else:
            raise MiniSEEDError(status, "Error reading miniSEED record")

    def __del__(self) -> None:
        """Ensure cleanup when object is garbage collected"""
        try:
            self.close()
        except Exception:
            # Silently ignore exceptions in __del__ to avoid issues during interpreter shutdown
            pass

    def close(self) -> None:
        """Close the reader and free any allocated memory"""
        if self._msr_ptr[0] != ffi.NULL:
            clibmseed.msr3_free(self._msr_ptr)
            self._msr_ptr[0] = ffi.NULL
