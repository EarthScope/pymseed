"""
Core miniSEED file reader implementation for pymseed.

"""

import os
import sys
import warnings
from typing import Any

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .msrecord import MS3Record

_INPUT_SENTINEL: Any = object()


class MS3RecordReader:
    """Read miniSEED records from a file or file descriptor.

    Use MS3Record.from_file() instead of this class directly.

    This class provides a Python interface for reading miniSEED records from
    files or file descriptors.

    The reader can be used as an iterator to process records sequentially, or as
    a context manager for automatic resource cleanup.

    .. warning::
        Each :class:`MS3Record` returned by :meth:`read` (and therefore by
        iteration via :meth:`__next__`) shares a single C struct with the
        reader. The record is only valid until the **next** call to
        :meth:`read` / :func:`next` on this reader, and is fully invalidated
        when the reader is exhausted or after :meth:`close` is called. If
        you need to retain a record beyond the current iteration step, copy
        the fields you need (or load the data with :meth:`MS3Record.parse`,
        :meth:`MS3Record.from_buffer`, or :class:`MS3TraceList`).

    Args:
        source (str | os.PathLike | int): File path (``str`` or any
            :class:`os.PathLike`, e.g. :class:`pathlib.Path`) or open file
            descriptor (``int``). Any other type raises :class:`TypeError`.
            If an integer, it must be a non-negative, currently-open file
            descriptor (e.g. obtained from :func:`os.open`). Negative integers
            are rejected with :class:`ValueError`. The class will not verify
            that an arbitrary non-negative integer corresponds to a valid open
            descriptor — passing the wrong number will silently read from
            whatever ``fd`` is currently bound to that slot (commonly
            ``0=stdin``, ``1=stdout``, ``2=stderr``).

            Ownership semantics differ by source type:

            * **Path (str):** libmseed opens an internal file handle and
              closes it automatically on :meth:`close`, context-manager exit,
              or garbage collection.
            * **File descriptor (int):** the caller retains ownership of the
              descriptor. libmseed reads through an internal ``dup`` of the
              fd and closes only the duplicate; the original fd is **not**
              closed by :meth:`close`, ``__exit__``, or ``__del__``, and
              the caller is responsible for closing it.

        start_byte_offset (int, optional): Start byte offset in the input bytes stream.
            Defaults to 0.

        end_byte_offset (int, optional): End byte offset in the input bytes stream.
            Defaults to 0, which means read until the end of the stream.

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

    >>> total_samples = 0
    >>> for msr in MS3Record.from_file('examples/example_data.mseed', unpack_data=True):
    ...     total_samples += msr.numsamples
    >>> print(f"Total samples: {total_samples}")
    Total samples: 12600


        Using with an open file descriptor (caller closes the fd):

    >>> import os
    >>> flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) # For Windows portability
    >>> fd = os.open('examples/example_data.mseed', flags)
    >>> try:
    ...     total_records = 0
    ...     for msr in MS3Record.from_file(fd, unpack_data=False):
    ...         total_records += 1
    ... finally:
    ...     os.close(fd)
    >>> print(f"Total records: {total_records}")
    Total records: 107

    Note:
        This class is not thread-safe. Each thread should use its own reader instance.
        The underlying libmseed library handles the actual parsing and decompression.

    See Also:
        MS3Record.from_file(): use this instead of MS3RecordReader directly
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | int = _INPUT_SENTINEL,
        start_byte_offset: int = 0,
        end_byte_offset: int = 0,
        unpack_data: bool = False,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
        input: str | os.PathLike[str] | int = _INPUT_SENTINEL,
    ) -> None:
        self._msfp_ptr = ffi.new("MS3FileParam **")
        self._msr_ptr = ffi.new("MS3Record **")
        self._selections = ffi.NULL
        self.stream_name = ffi.NULL
        self.verbose = verbose
        self.parse_flags = 0

        if input is not _INPUT_SENTINEL:
            warnings.warn(
                "'input' is a deprecated alias for 'source' and will be removed "
                "in a future release; use 'source' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            source = input
        if source is _INPUT_SENTINEL:
            raise TypeError("MS3RecordReader() missing required argument: 'source'")

        # Validate and normalize source
        if isinstance(source, int):
            pass
        elif isinstance(source, str):
            pass
        elif isinstance(source, os.PathLike):
            source = os.fspath(source)
        else:
            raise TypeError(
                "source must be str, int (file descriptor), or os.PathLike; "
                f"got {type(source).__name__}"
            )

        # Construct parse flags
        if unpack_data:
            self.parse_flags |= clibmseed.MSF_UNPACKDATA
        if skip_not_data:
            self.parse_flags |= clibmseed.MSF_SKIPNOTDATA
        if validate_crc:
            self.parse_flags |= clibmseed.MSF_VALIDATECRC

        # If the stream is an integer, assume an open file descriptor
        if isinstance(source, int):
            if source < 0:
                raise ValueError(
                    f"File descriptor must be non-negative; got {source}. "
                    "(A negative value typically indicates an unopened or "
                    "already-closed descriptor.)"
                )
            self._msfp_ptr[0] = clibmseed.ms3_msfp_init(start_byte_offset, end_byte_offset, source)

            if self._msfp_ptr[0] == ffi.NULL:
                raise MiniSEEDError(
                    clibmseed.MS_GENERROR,
                    f"Error initializing file descriptor {source}",
                )

            self.stream_name = ffi.new("char[]", f"File Descriptor {source}".encode())
        # Otherwise, source is a str path
        else:
            # Encode upfront so a UnicodeEncodeError allows fast failure.
            encoded_path = source.encode("utf-8")
            self._msfp_ptr[0] = clibmseed.ms3_msfp_init(start_byte_offset, end_byte_offset, -1)

            if self._msfp_ptr[0] == ffi.NULL:
                raise MiniSEEDError(
                    clibmseed.MS_GENERROR,
                    f"Error initializing file {source}",
                )

            self.stream_name = ffi.new("char[]", encoded_path)

    def __enter__(self) -> "MS3RecordReader":
        """Context manager entry point - returns self for use in 'with' statements."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Context manager exit point - ensures proper cleanup by calling close()."""
        self.close()

    def __iter__(self) -> "MS3RecordReader":
        """Iterator protocol - allows the reader to be used in for loops."""
        return self

    def read(self) -> MS3Record | None:
        """Read the next miniSEED record from the file or file descriptor.

        Returns the next :class:`MS3Record`, or ``None`` at end of stream.
        Raises :class:`ValueError` if the reader has been closed.

        .. warning::
            The returned :class:`MS3Record` shares a single C struct with
            this reader. It is only valid until the next call to
            :meth:`read` / :func:`next` on this reader, and is fully
            invalidated when the reader is exhausted or after :meth:`close`
            is called. Copy the fields you need before reading the next
            record if you need to retain them.
        """
        if self._msfp_ptr[0] == ffi.NULL:
            raise ValueError("I/O operation on closed MS3RecordReader")

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
        if status == clibmseed.MS_ENDOFFILE:
            return None
        raise MiniSEEDError(status, "Error reading miniSEED record")

    def __next__(self) -> MS3Record:
        """Iterator protocol - returns the next record or raises StopIteration.

        See :meth:`read` for the lifetime contract of the returned record
        (each yielded :class:`MS3Record` is invalidated by the next call).
        """
        msr = self.read()
        if msr is None:
            raise StopIteration
        return msr

    def __del__(self) -> None:
        """Ensure cleanup when object is garbage collected"""
        if sys.is_finalizing():
            return
        try:
            self.close()
        except (AttributeError, TypeError):
            # Module-teardown race: clibmseed/ffi/cdata fields may have been
            # nulled out by Python before sys.is_finalizing() flipped. Nothing
            # actionable; let any other exception propagate via Python's
            # "Exception ignored in" mechanism so real bugs surface.
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
            # Set to NULL to prevent double cleanup
            self._msfp_ptr[0] = ffi.NULL
            self._msr_ptr[0] = ffi.NULL
