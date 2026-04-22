"""
Record-by-record miniSEED validator with error accumulation.

This module provides MS3RecordValidator for validating miniSEED records
from memory buffers or files, accumulating errors while continuing to
parse when possible.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from .clib import clibmseed, ffi
from ._json import json_loads
from .logging import clear_error_messages, get_error_messages
from .mstracelist import MS3TraceList

# (buf_ptr, absolute_offset, record_length) — or (None, offset, error_code) for detection failure
_RecordTuple = tuple[Any, int, int]

# Maps supported schema IDs to their bundled JSON Schema filenames
_KNOWN_SCHEMAS: dict[str, str] = {
    "FDSN-v1.0": "ExtraHeaders-FDSN-v1.0.schema-2020-12.json",
}


@dataclass(frozen=True)
class ValidationError:
    """An error or warning found during miniSEED record validation.

    Attributes:
        offset: Byte offset in the source where the issue occurred.
        message: Description of the issue.
        sourceid: Source identifier from the record header, if parseable.
        starttime: Start time as nanoseconds since Unix epoch, if parseable.
        reclen: Record length in bytes, if determinable.
    """

    offset: int
    message: str
    sourceid: str | None = None
    starttime: int | None = None
    reclen: int | None = None


class _BufferSource:
    """Iterate over detected records in a contiguous memory buffer."""

    def __init__(self, buffer: Any) -> None:
        self._buffer = buffer

    def __iter__(self) -> Iterator[_RecordTuple]:
        buf_ptr = ffi.from_buffer(self._buffer)
        buf_size = len(self._buffer)
        format_version = ffi.new("uint8_t *")
        offset = 0

        while offset < buf_size:
            reclen = clibmseed.ms3_detect(
                buf_ptr + offset,
                buf_size - offset,
                format_version,
            )
            if reclen < 0:
                yield (None, offset, reclen)
                return
            if reclen == 0 or offset + reclen > buf_size:
                return
            yield (buf_ptr + offset, offset, reclen)
            offset += reclen


class _FileLikeSource:
    """Iterate over detected records in a forward-only file-like stream.

    Reads from any object with a ``.read(n)`` method that returns ``bytes``.
    The stream does not need to be seekable. The caller retains ownership
    of the file-like object and is responsible for closing it.
    """

    def __init__(self, fh: Any, chunk_size: int = 10_485_760) -> None:
        self._fh = fh
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator[_RecordTuple]:
        format_version = ffi.new("uint8_t *")
        buf = bytearray()
        buf_offset = 0
        file_offset = 0
        eof = False
        buf_base = None
        buf_generation = -1
        generation = 0

        while True:
            # --- Fill buffer ---
            if not eof:
                chunk = self._fh.read(self._chunk_size)
                if chunk:
                    buf_base = None

                    if buf_offset > self._chunk_size:
                        del buf[:buf_offset]
                        buf_offset = 0

                    buf.extend(chunk)
                    generation += 1
                else:
                    eof = True

            remaining = len(buf) - buf_offset
            if remaining <= 0:
                return

            # --- Drain records from current buffer ---
            while True:
                remaining = len(buf) - buf_offset
                if remaining <= 0:
                    break

                if buf_generation != generation:
                    buf_base = None
                    buf_base = ffi.from_buffer(buf)
                    buf_generation = generation

                record_ptr = buf_base + buf_offset

                reclen = clibmseed.ms3_detect(
                    record_ptr,
                    remaining,
                    format_version,
                )

                if reclen < 0:
                    # If not at EOF, we may simply not have enough
                    # bytes for detection. Break to read more data.
                    if eof:
                        yield (None, file_offset, reclen)
                        return
                    break

                if reclen == 0 or reclen > remaining:
                    if eof:
                        return
                    break

                yield (record_ptr, file_offset, reclen)
                buf_offset += reclen
                file_offset += reclen

            if eof:
                return


class _FileSource:
    """Iterate over detected records in a file using a sliding buffer."""

    def __init__(self, filename: str, chunk_size: int = 10_485_760) -> None:
        self._filename = filename
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator[_RecordTuple]:
        with open(self._filename, "rb") as f:
            yield from _FileLikeSource(f, self._chunk_size)


class MS3RecordValidator:
    """Validate miniSEED records with comprehensive error detection.

    Processes records from a buffer or file using a 5-step process:

    1. Determine record length (handled by the record source)
    2. Parse record metadata without unpacking data
    3. Optionally validate extra headers
    4. Optionally add record to a trace coverage list (with no data samples)
    5. Optionally decompress data samples and test for decoding errors

    This approach ensures maximum information recovery — all records with
    parseable headers are added to the trace list, with complete error tracking.

    Use the factory classmethods ``from_buffer`` and ``from_file`` to create
    instances, then call ``validate()`` to run validation.

    Args:
        source: A record source iterable (``_BufferSource`` or ``_FileSource``).
            Use ``from_buffer()`` or ``from_file()`` instead of constructing directly.
        return_trace_list: If True, build and return an MS3TraceList.
        unpack_data: If True, decompress data samples to detect decoding errors.
        validate_crc: If True, validate CRC checksums (miniSEED v3 only).
        validate_extra_headers: If True, validate extra headers against a schema.
        extra_headers_schema: Schema ID for extra headers validation.
        verbose: Verbosity level for libmseed operations.

    Examples:
        Validate a buffer:

        >>> from pymseed import MS3RecordValidator
        >>> with open('examples/example_data.mseed', 'rb') as f:
        ...     buffer = f.read()
        >>> errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()
        >>> print(f"Parsed {len(traces)} trace IDs with {len(errors)} errors")
        Parsed 3 trace IDs with 0 errors

        Validate a file without loading it entirely into memory::

            errors, traces = MS3RecordValidator.from_file("data.mseed").validate()

    Notes:
        - Validation stops only when record length cannot be determined
        - Each error is a ``ValidationError`` with ``offset``, ``message``,
          and optional ``sourceid``, ``starttime``, ``reclen``
    """

    def __init__(
        self,
        source: _BufferSource | _FileSource | _FileLikeSource,
        *,
        return_trace_list: bool = True,
        unpack_data: bool = True,
        validate_crc: bool = True,
        validate_extra_headers: bool = True,
        extra_headers_schema: str = "FDSN-v1.0",
        verbose: int = 0,
    ) -> None:
        self._source = source
        self._return_trace_list = return_trace_list
        self._unpack_data = unpack_data
        self._validate_crc = validate_crc
        self._validate_extra_headers = validate_extra_headers
        self._extra_headers_schema = extra_headers_schema
        self._verbose = verbose

        self._parse_flags = 0
        if validate_crc:
            self._parse_flags |= clibmseed.MSF_VALIDATECRC

    @classmethod
    def from_buffer(cls, buffer: Any, **kwargs: Any) -> "MS3RecordValidator":
        """Create a validator from a miniSEED buffer.

        Args:
            buffer: A buffer-like object containing miniSEED records.
                Must support the buffer protocol (bytes, bytearray, memoryview, etc.).
            **kwargs: Passed to ``MS3RecordValidator.__init__``.

        Returns:
            A new ``MS3RecordValidator`` instance.

        Example::

            errors, traces = MS3RecordValidator.from_buffer(buffer, unpack_data=True).validate()
        """
        return cls(_BufferSource(buffer), **kwargs)

    @classmethod
    def from_file(
        cls,
        filename: str,
        *,
        chunk_size: int = 10_485_760,
        **kwargs: Any,
    ) -> "MS3RecordValidator":
        """Create a validator for a miniSEED file.

        Reads the file in chunks using a sliding buffer, so the entire
        file does not need to fit in memory.

        Args:
            filename: Path to miniSEED file.
            chunk_size: Read chunk size in bytes. Default is 10 MiB.
            **kwargs: Passed to ``MS3RecordValidator.__init__``.

        Returns:
            A new ``MS3RecordValidator`` instance.

        Example::

            errors, traces = MS3RecordValidator.from_file("data.mseed").validate()
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")
        elif chunk_size > 1_073_741_824:
            raise ValueError("chunk_size must be less than 1 GiB")

        return cls(_FileSource(filename, chunk_size), **kwargs)

    @classmethod
    def from_filelike(
        cls,
        fh: Any,
        *,
        chunk_size: int = 10_485_760,
        **kwargs: Any,
    ) -> "MS3RecordValidator":
        """Create a validator for a miniSEED file-like stream.

        Reads from any object exposing ``.read(n) -> bytes`` (e.g.
        ``io.BytesIO``, ``sys.stdin.buffer``, an HTTP response body, a
        socket file) using a sliding buffer, so the full stream does not
        need to fit in memory.  The stream is **not** required to be
        seekable.  The caller retains ownership of ``fh`` and is
        responsible for closing it.

        Args:
            fh: A file-like object with a ``.read(n)`` method returning bytes.
            chunk_size: Read chunk size in bytes. Default is 10 MiB.
            **kwargs: Passed to ``MS3RecordValidator.__init__``.

        Returns:
            A new ``MS3RecordValidator`` instance.

        Example::

            errors, traces = MS3RecordValidator.from_filelike(fh).validate()
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")
        elif chunk_size > 1_073_741_824:
            raise ValueError("chunk_size must be less than 1 GiB")

        return cls(_FileLikeSource(fh, chunk_size), **kwargs)

    def validate(self) -> tuple[list[ValidationError], MS3TraceList | None]:
        """Validate records and return accumulated errors and a trace list.

        Returns:
            A tuple of (errors, traces):
            - errors: List of ``ValidationError`` instances describing errors
              and warnings encountered during parsing.
            - traces: ``MS3TraceList`` built from all successfully parsed records.
              Records with validation warnings are included. ``None`` if
              ``return_trace_list=False``.

        Note:
            Validation stops when:
            - All records have been processed
            - Incomplete record at end of source
            - Cannot determine record length
        """
        errors: list[ValidationError] = []
        tracelist = MS3TraceList() if self._return_trace_list else None

        msr_ptr = ffi.new("MS3Record **")

        # Pre-load JSON schema validator once — avoid reloading per-record
        _eh_validator: Any = None
        _eh_import_error = False
        if self._validate_extra_headers:
            if self._extra_headers_schema not in _KNOWN_SCHEMAS:
                raise ValueError(f"Unknown schema_id: {self._extra_headers_schema}")
            try:
                from ._extra_headers_jsonschema import (
                    validator_for_extra_headers_schema,
                )

                schema_bytes = (
                    files("pymseed.schemas")
                    .joinpath(_KNOWN_SCHEMAS[self._extra_headers_schema])
                    .read_bytes()
                )
                _eh_validator = validator_for_extra_headers_schema(
                    json_loads(schema_bytes)
                )
            except ImportError:
                _eh_import_error = True

        try:
            for buf_ptr, offset, record_length in self._source:
                # Detection failure — source signals this with buf_ptr=None
                if buf_ptr is None:
                    if record_length == -1:
                        reason = "No miniSEED detected"
                    else:
                        reason = f"Record detection failed: {record_length}"
                    errors.append(
                        ValidationError(
                            offset=offset,
                            message=reason,
                        )
                    )
                    break

                clear_error_messages()

                # Step 2: Parse record metadata (without unpacking data samples)
                status = clibmseed.msr3_parse(
                    buf_ptr,
                    record_length,
                    msr_ptr,
                    self._parse_flags,
                    self._verbose,
                )

                if status != clibmseed.MS_NOERROR:
                    error_messages = get_error_messages()

                    # Add a default error message if no messages are available
                    if not error_messages:
                        error_messages = [f"Parse error: {status}"]

                    for msg in error_messages:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=msg,
                                reclen=record_length,
                            )
                        )
                    continue

                # Read metadata directly from C struct
                msr = msr_ptr[0]
                sourceid = ffi.string(msr.sid).decode("utf-8")

                # Check for parse warnings (CRC validation, etc.)
                parse_messages = get_error_messages()
                if parse_messages:
                    for msg in parse_messages:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=msg,
                                sourceid=sourceid,
                                starttime=msr.starttime,
                                reclen=record_length,
                            )
                        )

                # Step 3: Optionally validate extra headers
                if self._validate_extra_headers and msr.extralength > 0:
                    if _eh_import_error:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message="Extra headers validation skipped: jsonschema-rs not installed",
                                sourceid=sourceid,
                                starttime=msr.starttime,
                                reclen=record_length,
                            )
                        )
                    else:
                        try:
                            extra_str = (
                                ffi.string(msr.extra).decode("utf-8")
                                if msr.extra != ffi.NULL
                                else ""
                            )
                            if extra_str:
                                for ve in _eh_validator.iter_errors(
                                    json_loads(extra_str)
                                ):
                                    errors.append(
                                        ValidationError(
                                            offset=offset,
                                            message=f"Extra headers validation error: {ve.message} at {ve.instance_path}",
                                            sourceid=sourceid,
                                            starttime=msr.starttime,
                                            reclen=record_length,
                                        )
                                    )
                        except Exception as e:
                            errors.append(
                                ValidationError(
                                    offset=offset,
                                    message=f"Extra headers validation error: {e}",
                                    sourceid=sourceid,
                                    starttime=msr.starttime,
                                    reclen=record_length,
                                )
                            )

                # Step 4: Add record to trace list
                if tracelist is not None:
                    segptr = clibmseed.mstl3_addmsr_recordptr(
                        tracelist._mstl,
                        msr,
                        ffi.NULL,
                        0,  # splitversion
                        1,  # autoheal
                        0,  # flags
                        ffi.NULL,  # tolerance
                    )

                    if segptr == ffi.NULL:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message="Failed to add record to trace list",
                                sourceid=sourceid,
                                starttime=msr.starttime,
                                reclen=record_length,
                            )
                        )
                        continue

                # Step 5: Optionally decompress data samples to detect decoding errors
                if self._unpack_data:
                    clear_error_messages()
                    status = clibmseed.msr3_unpack_data(msr, self._verbose)

                    error_messages = get_error_messages()

                    # Check for unpack errors
                    if status < 0:
                        # Add a default error message if no messages are available
                        if not error_messages:
                            error_messages = [f"Data unpack error: {status}"]

                        for msg in error_messages:
                            errors.append(
                                ValidationError(
                                    offset=offset,
                                    message=msg,
                                    sourceid=sourceid,
                                    starttime=msr.starttime,
                                    reclen=record_length,
                                )
                            )
                    # Check for unpack warning messages, e.g. decoding integrity checks
                    # (historically common for these to be warnings, not errors)
                    elif error_messages:
                        for msg in error_messages:
                            errors.append(
                                ValidationError(
                                    offset=offset,
                                    message=msg,
                                    sourceid=sourceid,
                                    starttime=msr.starttime,
                                    reclen=record_length,
                                )
                            )

        finally:
            if msr_ptr[0] != ffi.NULL:
                clibmseed.msr3_free(msr_ptr)

        return errors, tracelist
