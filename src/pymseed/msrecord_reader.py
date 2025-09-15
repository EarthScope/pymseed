"""
Core miniSEED file reader implementation for pymseed.

"""

from typing import Any, Optional, Union

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .msrecord import MS3Record


class MS3RecordReader:
    """Read miniSEED records from a file or file descriptor.

    Use MS3Record.from_file() instead of this class directly.

    This class provides a Python interface for reading miniSEED records from
    files or file descriptors.

    The reader can be used as an iterator to process records sequentially, or as
    a context manager for automatic resource cleanup.

    Args:
        input (Union[str, int]): File path (string) or open file descriptor (integer).
            If an integer, it must be a valid open file descriptor. The file or
            descriptor will be automatically closed when close() is called or when
            the object is used as a context manager.
        unpack_data (bool, optional): Whether to decode/unpack the data samples from
            the records. If False, only metadata is parsed and data remains in
            compressed format. Defaults to False for better performance when only
            metadata is needed.
        skip_not_data (bool, optional): Whether to skip non-data bytes in the input
            stream until a valid miniSEED record is found. Useful for reading from
            streams that may contain other data mixed with miniSEED records.
            Defaults to False.
        validate_crc (bool, optional): If True, validate CRC checksums when present in records.
            miniSEED v3 records contain CRCs, but v2 records do not. Default is True.
        verbose (int, optional): Verbosity level for for libmseed operations. Higher values
            produce more detailed output. 0 = no output, 1+ = increasing verbosity.
            Defaults to 0 (silent).

    Raises:
        MiniSEEDError: If the file or file descriptor cannot be initialized for reading.

    Examples:
        Basic usage with a file path as a context manager:

    >>> from pymseed import MS3Record

    >>> with MS3Record.from_file('examples/example_data.mseed', unpack_data=True) as reader:
    ...     total_samples = 0
    ...     for record in reader:
    ...         total_samples += record.numsamples
    ...     print(f"Total samples: {total_samples}")
    Total samples: 12600


        Using with an open file descriptor:

    >>> import os
    >>> fd = os.open('examples/example_data.mseed', os.O_RDONLY)

    >>> with MS3Record.from_file(fd, unpack_data=True) as reader:
    ...     records = list(reader)  # Read all records
    ...     print(f"Total records: {len(records)}")
    Total records: 107

    Note:
        This class is not thread-safe. Each thread should use its own reader instance.
        The underlying libmseed library handles the actual parsing and decompression.

    See Also:
        MS3Record.from_file(): use this instead of MS3RecordReader directly
    """

    def __init__(
        self,
        input: Union[str, int],
        unpack_data: bool = False,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> None:
        self._msfp_ptr = ffi.new("MS3FileParam **")
        self._msr_ptr = ffi.new("MS3Record **")
        self._selections = ffi.NULL
        self.verbose = verbose

        # Construct parse flags
        self.parse_flags = 0
        if unpack_data:
            self.parse_flags |= clibmseed.MSF_UNPACKDATA
        if skip_not_data:
            self.parse_flags |= clibmseed.MSF_SKIPNOTDATA
        if validate_crc:
            self.parse_flags |= clibmseed.MSF_VALIDATECRC

        # If the stream is an integer, assume an open file descriptor
        if isinstance(input, int):
            self._msfp_ptr[0] = clibmseed.ms3_mstl_init_fd(input)

            if self._msfp_ptr[0] == ffi.NULL:
                raise MiniSEEDError(
                    clibmseed.MS_GENERROR,
                    f"Error initializing file descriptor {input}",
                )

            self.stream_name = ffi.new("char[]", f"File Descriptor {input}".encode())
        # Otherwise, assume a path name
        else:
            self.stream_name = ffi.new("char[]", input.encode())

    def __enter__(self) -> "MS3RecordReader":
        """Context manager entry point - returns self for use in 'with' statements."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Context manager exit point - ensures proper cleanup by calling close()."""
        self.close()

    def __iter__(self) -> "MS3RecordReader":
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
        """Read the next miniSEED record from the file or file descriptor"""

        status = clibmseed.ms3_readmsr_selection(
            self._msfp_ptr,
            self._msr_ptr,
            self.stream_name,
            self.parse_flags,
            self._selections,
            self.verbose,
        )

        if status == clibmseed.MS_NOERROR:
            return MS3Record(recordptr=self._msr_ptr[0])
        elif status == clibmseed.MS_ENDOFFILE:
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

        # Perform cleanup by calling the function with NULL stream name
        if self._msfp_ptr[0] != ffi.NULL or self._msr_ptr[0] != ffi.NULL:
            clibmseed.ms3_readmsr_selection(
                self._msfp_ptr,
                self._msr_ptr,
                ffi.NULL,  # NULL stream name signals cleanup
                self.parse_flags,
                self._selections,
                self.verbose,
            )
            # Mark as closed to prevent double cleanup
            self._msfp_ptr[0] = ffi.NULL
            self._msr_ptr[0] = ffi.NULL
