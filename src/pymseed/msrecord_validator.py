"""
Record-by-record miniSEED validator with error accumulation.

This module provides MS3RecordValidator for validating miniSEED records
from memory buffers or files, accumulating errors while continuing to
parse when possible.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from .clib import clibmseed, ffi
from .logging import clear_error_messages, get_error_messages
from .msrecord import MS3Record
from .mstracelist import MS3TraceList

# (buf_ptr, absolute_offset, record_length) — or (None, offset, error_code) for detection failure
_RecordTuple = tuple[Any, int, int]


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


class _FileSource:
    """Iterate over detected records in a file using a sliding buffer.

    Reads the file in chunks so the entire file need not fit in memory.
    Each yielded record is backed by its own bytes object, so the pointer
    remains valid regardless of subsequent buffer operations.
    """

    def __init__(self, filename: str, chunk_size: int = 1_048_576) -> None:
        self._filename = filename
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator[_RecordTuple]:
        format_version = ffi.new("uint8_t *")
        buf = bytearray()
        file_offset = 0
        eof = False

        with open(self._filename, "rb") as f:
            while True:
                if not eof:
                    chunk = f.read(self._chunk_size)
                    if chunk:
                        buf.extend(chunk)
                    else:
                        eof = True

                if not buf:
                    return

                reclen = clibmseed.ms3_detect(
                    ffi.from_buffer(buf),
                    len(buf),
                    format_version,
                )

                if reclen < 0:
                    yield (None, file_offset, reclen)
                    return

                if reclen == 0 or reclen > len(buf):
                    if eof:
                        return
                    continue

                record_bytes = bytes(buf[:reclen])
                yield (ffi.from_buffer(record_bytes), file_offset, reclen)

                buf = buf[reclen:]
                file_offset += reclen


class MS3RecordValidator:
    """Validate miniSEED records with comprehensive error detection.

    Processes records from a buffer or file using a 4-step process:

    1. Determine record length (handled by the record source)
    2. Parse record metadata without unpacking data
    3. Optionally add record to a trace coverage list (with no data samples)
    4. Optionally decompress data samples and test for decoding errors

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
        source: _BufferSource | _FileSource,
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
        chunk_size: int = 1_048_576,
        **kwargs: Any,
    ) -> "MS3RecordValidator":
        """Create a validator for a miniSEED file.

        Reads the file in chunks using a sliding buffer, so the entire
        file does not need to fit in memory.

        Args:
            filename: Path to miniSEED file.
            chunk_size: Read chunk size in bytes. Default is 1 MiB.
            **kwargs: Passed to ``MS3RecordValidator.__init__``.

        Returns:
            A new ``MS3RecordValidator`` instance.

        Example::

            errors, traces = MS3RecordValidator.from_file("data.mseed").validate()
        """
        if (chunk_size <= 0):
            raise ValueError("chunk_size must be greater than 0")
        elif (chunk_size > 1_073_741_824):
            raise ValueError("chunk_size must be less than 1 GiB")

        return cls(_FileSource(filename, chunk_size), **kwargs)

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

        clear_error_messages()

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

                # Step 2: Parse record structure (without unpacking data samples)
                status = clibmseed.msr3_parse(
                    buf_ptr,
                    record_length,
                    msr_ptr,
                    self._parse_flags,
                    self._verbose,
                )

                if status != clibmseed.MS_NOERROR:
                    parse_messages = get_error_messages()
                    error_msg = f"Parse error: {status}"
                    if parse_messages:
                        error_msg += f" ({'; '.join(parse_messages)})"

                    errors.append(
                        ValidationError(
                            offset=offset,
                            message=error_msg,
                            reclen=record_length,
                        )
                    )
                    continue

                record = MS3Record(recordptr=msr_ptr[0])
                rec_fields = {
                    "sourceid": record.sourceid,
                    "starttime": record.starttime,
                    "reclen": record_length,
                }

                # Check for parse warnings (CRC validation, etc.)
                parse_messages = get_error_messages()
                if parse_messages:
                    for msg in parse_messages:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=msg,
                                **rec_fields,
                            )
                        )

                if self._validate_extra_headers and record.extralength > 0:
                    try:
                        if not record.valid_extra_headers(
                            schema_id=self._extra_headers_schema
                        ):
                            errors.append(
                                ValidationError(
                                    offset=offset,
                                    message="Extra headers validation failed",
                                    **rec_fields,
                                )
                            )
                    except ImportError:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message="Extra headers validation skipped: jsonschema not installed",
                                **rec_fields,
                            )
                        )
                    except Exception as e:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=f"Extra headers validation error: {e}",
                                **rec_fields,
                            )
                        )

                # Step 3: Add record to trace list
                if tracelist is not None:
                    segptr = clibmseed.mstl3_addmsr_recordptr(
                        tracelist._mstl,
                        record._msr,
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
                                **rec_fields,
                            )
                        )
                        continue

                # Step 4: Optionally decompress data samples to detect decoding errors
                if self._unpack_data:
                    clear_error_messages()
                    status = clibmseed.msr3_unpack_data(msr_ptr[0], self._verbose)

                    if status < 0:
                        unpack_messages = get_error_messages()
                        error_msg = f"Data unpack error: {status}"
                        if unpack_messages:
                            error_msg += f" ({'; '.join(unpack_messages)})"
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=error_msg,
                                **rec_fields,
                            )
                        )

                    if record.samplecnt > 0 and record.numsamples == 0:
                        errors.append(
                            ValidationError(
                                offset=offset,
                                message=f"Data unpacking incomplete: expected {record.samplecnt}, got {record.numsamples}",
                                **rec_fields,
                            )
                        )

        finally:
            if msr_ptr[0] != ffi.NULL:
                clibmseed.msr3_free(msr_ptr)

        return errors, tracelist
