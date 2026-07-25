"""
Core miniSEED file reader implementation for pymseed.

"""

import os
import sys
import warnings
from collections.abc import Callable
from typing import Any

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .logging import ensure_thread_logging
from .msrecord import MS3Record, _truncated_source_message
from .selections import build_selections

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
            Defaults to 0, which means read until the end of the stream.  A range
            ending part way through a record raises :class:`MiniSEEDError` after
            the records that fit within it, as a truncated stream does.

        unpack_data (bool, optional): Whether to decode/unpack the data samples from
            the records. If False, only metadata is parsed and data remains in
            compressed format. Defaults to False for better performance when only
            metadata is needed.

        sourceid (str, optional): Source ID glob pattern to select matching
            records (e.g. ``"FDSN:IU_COLA_*"``). None matches all source IDs.
            Defaults to None.

        starttime (str, optional): Start of time window as a formatted date-time
            string (e.g. ``"2024-01-01T00:00:00Z"``). Only records containing
            data after this time are returned. None means open start.
            Defaults to None.

        endtime (str, optional): End of time window as a formatted date-time
            string. Only records containing data before this time are returned.
            None means open end. Defaults to None.

        skip_not_data (bool, optional): Whether to skip non-data bytes in the input
            stream until a valid miniSEED record is found. Useful for reading from
            streams that may contain other data mixed with miniSEED records.
            Defaults to False.

        validate_crc (bool, optional): If True, validate CRC checksums when present in records.
            miniSEED v3 records contain CRCs, but v2 records do not. Default is True.

        verbose (int, optional): Verbosity level for for libmseed operations. Higher values
            produce more detailed output. 0 = no output, 1+ = increasing verbosity.
            Defaults to 0 (silent).

        input: Deprecated alias for ``source``; passing it emits a
            ``DeprecationWarning``, and passing both raises :class:`TypeError`.
            This alias will be removed in a future release.

    Raises:
        TypeError: If ``source`` is missing, is not a supported type, or is passed
            together with the deprecated ``input`` alias.
        ValueError: If ``starttime`` or ``endtime`` is not a valid date-time string.
        MiniSEEDError: If the file or file descriptor cannot be initialized for reading,
            or if the stream ends part way through a record, or with bytes remaining
            that are too few for one.  The truncated cases carry a status of
            ``MS_ENDOFFILE`` and are reported as a clean end of stream when
            ``skip_not_data`` is set.

    Examples:
        Basic usage with a file path as a context manager:

    >>> from pymseed import MS3Record

    >>> total_samples = 0
    >>> for msr in MS3Record.from_file('examples/example_data.mseed', unpack_data=True):
    ...     total_samples += msr.numsamples
    >>> print(f"Total samples: {total_samples}")
    Total samples: 12600

        Selecting records by source ID and time window.  The filtering is
        applied by libmseed while reading, non-matching records are skipped
        without leaving the C layer and their data samples are never decoded:

    >>> records = 0
    >>> for msr in MS3Record.from_file(
    ...     'examples/example_data.mseed',
    ...     sourceid='FDSN:IU_COLA_00_L_H_Z',
    ...     starttime='2010-02-27T07:00:00Z',
    ...     endtime='2010-02-27T07:30:00Z',
    ... ):
    ...     records += 1
    >>> print(f"Matching records: {records}")
    Matching records: 15

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
        sourceid: str | None = None,
        starttime: str | None = None,
        endtime: str | None = None,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
        input: str | os.PathLike[str] | int = _INPUT_SENTINEL,
    ) -> None:
        ensure_thread_logging()

        self._msfp_ptr = ffi.new("MS3FileParam **")
        self._msr_ptr = ffi.new("MS3Record **")
        self._selections = ffi.NULL
        self._free_selections: Callable[[], None] | None = None
        self.stream_name = ffi.NULL
        self.verbose = verbose
        self.parse_flags = 0

        if input is not _INPUT_SENTINEL:
            if source is not _INPUT_SENTINEL:
                raise TypeError(
                    "MS3RecordReader() got both 'source' and its deprecated alias "
                    "'input'; pass only 'source'"
                )
            warnings.warn(
                "'input' is a deprecated alias and will be removed in a future "
                "release; use 'source' instead.",
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

        # Validate byte offsets
        if start_byte_offset < 0:
            raise ValueError(f"start_byte_offset must be non-negative; got {start_byte_offset}")
        if end_byte_offset < 0:
            raise ValueError(f"end_byte_offset must be non-negative; got {end_byte_offset}")
        if 0 < end_byte_offset < start_byte_offset:
            raise ValueError(
                f"end_byte_offset ({end_byte_offset}) must be >= "
                f"start_byte_offset ({start_byte_offset})"
            )

        # Construct parse flags
        if unpack_data:
            self.parse_flags |= clibmseed.MSF_UNPACKDATA
        if skip_not_data:
            self.parse_flags |= clibmseed.MSF_SKIPNOTDATA
        if validate_crc:
            self.parse_flags |= clibmseed.MSF_VALIDATECRC

        # Build selections, if sourceid, starttime, or endtime are specified.
        # Done before opening the input so an invalid time string raises without
        # leaving a stream open.  libmseed copies the pattern into the selection
        # structures, which it owns until _free_selections() is called in close().
        self._selections, self._free_selections = build_selections(sourceid, starttime, endtime)

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
            encoded_path = os.fsencode(source)
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
        Raises :class:`ValueError` if the reader has been closed, and
        :class:`MiniSEEDError` if the stream ends part way through a record.

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
            # Hold the reader so its struct cannot be freed by garbage collection
            # while this record is still referenced.
            return MS3Record(recordptr=self._msr_ptr[0], owner=self)
        if status == clibmseed.MS_ENDOFFILE:
            # libmseed returns MS_ENDOFFILE for both a clean end of stream and a
            # record the stream ends part way through; unconsumed bytes left
            # buffered distinguish the truncated case.  Skipping non-data is a
            # request to tolerate exactly this sort of trailing remnant.
            if not self.parse_flags & clibmseed.MSF_SKIPNOTDATA:
                msfp = self._msfp_ptr[0]
                buffered = msfp.readlength - msfp.readoffset if msfp != ffi.NULL else 0
                if buffered > 0:
                    # Redetect the remnant for the same shortfall the buffer and
                    # file-like iterators report; the read call does not return it.
                    formatversion = ffi.new("uint8_t *")
                    reclen = clibmseed.ms3_detect(
                        msfp.readbuffer + msfp.readoffset, buffered, formatversion
                    )
                    needed = reclen - buffered if reclen > buffered else 0
                    raise MiniSEEDError(
                        status, _truncated_source_message("stream", buffered, needed)
                    )
            return None

        # libmseed reports MS_NOTSEED for two different conditions: a record
        # that could not be parsed as miniSEED, and reaching the end of the
        # stream having returned no records at all.  An active selection that
        # rejects every record in an otherwise valid stream produces the
        # latter, which is a valid empty result rather than an error (mirroring
        # MS3TraceList.add_buffer()).
        #
        # The two are told apart by how much unconsumed data is left buffered:
        # the end-of-stream case is only reached with less than a minimum
        # record remaining, whereas a parse failure stops with the offending
        # (full-length) record still buffered.  A non-miniSEED stream therefore
        # still raises, even while filtering.  libmseed's "No data records
        # read, not SEED?" diagnostic remains in the log registry and is
        # visible through get_error_messages().
        if status == clibmseed.MS_NOTSEED and self._selections != ffi.NULL:
            msfp = self._msfp_ptr[0]
            buffered = msfp.readlength - msfp.readoffset if msfp != ffi.NULL else 0
            if buffered < clibmseed.MINRECLEN:
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
        """Close the reader and free any allocated memory.

        Idempotent: safe to call multiple times.
        """
        # Perform cleanup by calling the function with NULL stream name.
        # The pointer-NULL guard makes this method idempotent.
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

        # Free the selections after the final read call, which is handed the
        # pointer above.  build_selections()'s free function is idempotent.
        if self._free_selections is not None:
            self._free_selections()
            self._free_selections = None
            self._selections = ffi.NULL
