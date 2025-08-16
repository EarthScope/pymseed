"""
Core file reader implementation for pymseed

"""

from typing import Any, Optional, Union

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .msrecord import MS3Record


class MS3RecordReader:
    """Read miniSEED records from a file or file descriptor

    If `input` is an integer, it is assumed to be an open file descriptor,
    otherwise it is assumed to be a path (file) name.  In all cases the
    file or descriptor will be closed when the object's close() is called.

    If `unpack_data` is True, the data samples will be decoded.

    If `skip_not_data` is True, bytes from the input stream will be skipped
    until a record is found.

    If `validate_crc` is True, the CRC will be validated if contained in
    the record (legacy miniSEED v2 contains no CRCs).  The CRC provides an
    internal integrity check of the record contents.
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

            self.stream_name = ffi.new("char[]", f"File Descriptor {input}".encode("utf-8"))
        # Otherwise, assume a path name
        else:
            self.stream_name = ffi.new("char[]", input.encode("utf-8"))

    def __enter__(self) -> "MS3RecordReader":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def __iter__(self) -> "MS3RecordReader":
        return self

    def __next__(self) -> MS3Record:
        next = self.read()
        if next is not None:
            return next
        else:
            raise StopIteration

    def read(self) -> Optional[MS3Record]:
        """Read the next miniSEED record from the file/descriptor"""

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
