"""
Core MS3RecordBufferReader implementation for pymseed

"""

from typing import Any, Optional

from .clib import clibmseed, ffi
from .msrecord import MS3Record
from .exceptions import MiniSEEDError


class MS3RecordBufferReader:
    """Read miniSEED records from a buffer, i.e. bytearray or numpy.array

    The `source` object must support the buffer protocol, e.g. bytearray,
    bytes, memoryview, numpy.array, etc.  This class will not modify the
    source buffer.

    If `unpack_data` is True, the data samples will be decoded.

    If `validate_crc` is True, the CRC will be validated if contained in
    the record (legacy miniSEED v2 contains no CRCs).  The CRC provides an
    internal integrity check of the record contents.
    """

    def __init__(
        self,
        source: Any,
        unpack_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> None:
        self._msr_ptr = ffi.new("MS3Record **")
        self.source = ffi.from_buffer(source)
        self.source_offset = 0
        self.verbose = verbose

        # Construct parse flags
        self.parse_flags = 0
        if unpack_data:
            self.parse_flags |= clibmseed.MSF_UNPACKDATA
        if validate_crc:
            self.parse_flags |= clibmseed.MSF_VALIDATECRC

    def __enter__(self) -> "MS3RecordBufferReader":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def __iter__(self) -> "MS3RecordBufferReader":
        return self

    def __next__(self) -> MS3Record:
        next = self.read()
        if next is not None:
            return next
        else:
            raise StopIteration

    def read(self) -> Optional[MS3Record]:
        """Read the next miniSEED record from the buffer"""
        remaining_bytes = len(self.source) - self.source_offset
        if remaining_bytes < clibmseed.MINRECLEN:
            return None

        status = clibmseed.msr3_parse(
            self.source + self.source_offset,
            remaining_bytes,
            self._msr_ptr,
            self.parse_flags,
            self.verbose,
        )

        if status == clibmseed.MS_NOERROR:
            self.source_offset += self._msr_ptr[0].reclen
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
