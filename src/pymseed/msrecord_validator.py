"""
Record-by-record buffer parser with error accumulation.

This module provides MS3RecordValidator for robust parsing of miniSEED buffers,
accumulating errors while continuing to parse when possible.
"""

from typing import Any, Optional

from .clib import clibmseed, ffi
from .logging import clear_error_messages, get_error_messages
from .msrecord import MS3Record
from .mstracelist import MS3TraceList


class MS3RecordValidator:
    """Parse miniSEED buffer record-by-record with comprehensive error handling.

    This class provides robust parsing of miniSEED data from a memory buffer,
    using a 4-step process for each record:

    1. **ms_detect()** - Determine record length (enables reliable continuation)
    2. **msr3_parse()** - Parse record metadata without unpacking data
    3. **Add to tracelist** - Add record even if validation errors occurred
    4. **msr3_unpack_data()** - Optionally decompress data samples

    This approach ensures maximum data recovery - all records with parseable
    metadata are added to the trace list, with complete error tracking.

    Args:
        buffer: A buffer-like object containing miniSEED records. Must support
            the buffer protocol (e.g., bytearray, bytes, memoryview, numpy.ndarray).
        unpack_data: If True, decode and decompress data samples from each record.
            If False, only header information is parsed. Default is False.
        validate_crc: If True, validate CRC checksums when present in records.
            miniSEED v3 records contain CRCs, but v2 records do not. Default is True.
        validate_extra_headers: If True, validate extra headers against the
            specified schema. Records with invalid extra headers are still added
            to the trace list, but an error is logged. Default is False.
        extra_headers_schema: Schema ID for extra headers validation.
            Currently only "FDSN-v1.0" is supported. Default is "FDSN-v1.0".
        verbose: Verbosity level for libmseed operations. Higher values produce
            more diagnostic output. Default is 0 (silent).

    Examples:
        Basic usage - parse buffer and check for errors:

        >>> from pymseed import MS3RecordValidator
        >>> with open('data.mseed', 'rb') as f:
        ...     buffer = f.read()
        >>> validator = MS3RecordValidator(buffer, unpack_data=True)
        >>> traces, errors = validator.parse()
        >>> print(f"Parsed {len(traces)} trace IDs with {len(errors)} errors")

        Parse with extra headers validation:

        >>> validator = MS3RecordValidator(
        ...     buffer,
        ...     validate_extra_headers=True,
        ...     extra_headers_schema="FDSN-v1.0"
        ... )
        >>> traces, errors = validator.parse()
        >>> for error in errors:
        ...     print(f"Offset {error['offset']}: {error['message']}")

        Handling mixed valid/invalid data:

        >>> validator = MS3RecordValidator(buffer, unpack_data=True, validate_crc=True)
        >>> traces, errors = validator.parse()
        >>> # All parseable records are in traces, even those with errors
        >>> # errors list contains detailed info about each issue
        >>> crc_errors = [e for e in errors if 'CRC' in e['message']]
        >>> data_errors = [e for e in errors if e['exception_type'] == 'DataError']

    Notes:
        - Parsing stops only when ms_detect() cannot determine record length
        - Records with CRC/validation errors are still added to trace list
        - Records with data decompression errors are added but without decoded samples
        - Error dict keys: offset, message, exception_type, record_info
    """

    def __init__(
        self,
        buffer: Any,
        unpack_data: bool = False,
        validate_crc: bool = True,
        validate_extra_headers: bool = False,
        extra_headers_schema: str = "FDSN-v1.0",
        verbose: int = 0,
    ) -> None:
        """Initialize validator with buffer and options."""
        self._buffer = buffer
        self._buffer_ptr = ffi.from_buffer(buffer)
        self._buffer_size = len(buffer)
        self._unpack_data = unpack_data
        self._validate_crc = validate_crc
        self._validate_extra_headers = validate_extra_headers
        self._extra_headers_schema = extra_headers_schema
        self._verbose = verbose

        # Build parse flags
        self._parse_flags = 0
        if unpack_data:
            self._parse_flags |= clibmseed.MSF_UNPACKDATA
        if validate_crc:
            self._parse_flags |= clibmseed.MSF_VALIDATECRC

    def parse(self) -> tuple[MS3TraceList, list[dict[str, Any]]]:
        """Parse the buffer and return traces and accumulated errors.

        Uses a 4-step process for each record:
        1. ms_detect() - Determine record length
        2. msr3_parse() - Parse record structure (without data unpacking)
        3. Add to tracelist - Add record even if there were validation warnings
        4. msr3_unpack_data() - Optionally decompress samples (if unpack_data=True)

        Returns:
            A tuple containing:
            - MS3TraceList: Traces built from all successfully parsed records.
              Records with validation errors (CRC, extra headers) are included.
            - list[dict]: Structured error information. Each dict contains:
                - offset (int): Byte offset in buffer where error occurred
                - message (str): Error description
                - exception_type (str): Type of error (e.g., "MiniSEEDError",
                  "ValidationError", "DataError", "ParseWarning")
                - record_info (dict | None): Partial record info if available,
                  with keys: sourceid, starttime, reclen

        Note:
            Parsing stops when:
            - End of buffer is reached
            - ms_detect() returns 0 (incomplete record at end)
            - ms_detect() returns negative (cannot determine length)
        """
        errors: list[dict[str, Any]] = []
        tracelist = MS3TraceList()

        # Allocate MS3Record pointer for parsing
        msr_ptr = ffi.new("MS3Record **")

        offset = 0

        # Clear any pre-existing log messages
        clear_error_messages()

        try:
            while offset < self._buffer_size:
                remaining_bytes = self._buffer_size - offset

                # Step 1: Detect record and get length
                format_version = ffi.new("uint8_t *")
                record_length = clibmseed.ms3_detect(
                    self._buffer_ptr + offset,
                    remaining_bytes,
                    format_version,
                )

                if record_length < 0:
                    # Detection error - cannot determine length, must stop
                    errors.append({
                        "offset": offset,
                        "message": f"Record detection failed: {record_length}",
                        "exception_type": "MiniSEEDError",
                        "record_info": None,
                    })
                    break
                elif record_length == 0:
                    # Not enough data for a complete record
                    break

                # Clear messages before parsing this record
                clear_error_messages()

                # Step 2: Parse record structure (without unpacking data samples)
                status = clibmseed.msr3_parse(
                    self._buffer_ptr + offset,
                    record_length,
                    msr_ptr,
                    self._parse_flags,
                    self._verbose,
                )

                if status != clibmseed.MS_NOERROR:
                    # Parse error - log and skip this record
                    # Collect any messages from the failed parse
                    parse_messages = get_error_messages()
                    error_msg = f"Parse error: {status}"
                    if parse_messages:
                        error_msg += f" ({'; '.join(parse_messages)})"

                    errors.append({
                        "offset": offset,
                        "message": error_msg,
                        "exception_type": "MiniSEEDError",
                        "record_info": {"reclen": record_length},
                    })
                    offset += record_length
                    continue

                # Create record wrapper
                record = MS3Record(recordptr=msr_ptr[0])
                record_info: dict[str, Any] = {
                    "sourceid": record.sourceid,
                    "starttime": record.starttime,
                    "reclen": record_length,
                }

                # Check for parse warnings (CRC validation, etc.)
                parse_messages = get_error_messages()
                if parse_messages:
                    for msg in parse_messages:
                        errors.append({
                            "offset": offset,
                            "message": msg,
                            "exception_type": "ParseWarning",
                            "record_info": record_info.copy(),
                        })

                # Optional: validate extra headers
                if self._validate_extra_headers and record.extralength > 0:
                    try:
                        if not record.valid_extra_headers(schema_id=self._extra_headers_schema):
                            errors.append({
                                "offset": offset,
                                "message": "Extra headers validation failed",
                                "exception_type": "ValidationError",
                                "record_info": record_info.copy(),
                            })
                    except ImportError:
                        # jsonschema not installed - log warning and continue
                        errors.append({
                            "offset": offset,
                            "message": "Extra headers validation skipped: jsonschema not installed",
                            "exception_type": "ValidationError",
                            "record_info": record_info.copy(),
                        })
                    except Exception as e:
                        errors.append({
                            "offset": offset,
                            "message": f"Extra headers validation error: {e}",
                            "exception_type": "ValidationError",
                            "record_info": record_info.copy(),
                        })

                # Step 3: Add record to trace list
                # (even if there were validation warnings above)
                flags = clibmseed.MSF_PPUPDATETIME
                segptr = clibmseed.mstl3_addmsr_recordptr(
                    tracelist._mstl,
                    record._msr,
                    ffi.NULL,  # no record pointer tracking
                    0,  # splitversion = False
                    1,  # autoheal = True (merge adjacent segments)
                    flags,
                    ffi.NULL,  # no tolerance
                )

                if segptr == ffi.NULL:
                    errors.append({
                        "offset": offset,
                        "message": "Failed to add record to trace list",
                        "exception_type": "MiniSEEDError",
                        "record_info": record_info.copy(),
                    })
                    # Continue anyway - advance to next record
                    offset += record_length
                    continue

                # Step 4: Check for data unpacking errors (if unpack_data was requested)
                # Data is unpacked during msr3_parse when MSF_UNPACKDATA flag is set.
                # Check if there were any data decoding issues by comparing expected vs actual
                if self._unpack_data:
                    if record.numsamples < 0:
                        # Negative numsamples indicates unpacking error
                        errors.append({
                            "offset": offset,
                            "message": f"Data unpacking error: {record.numsamples} samples decoded",
                            "exception_type": "DataError",
                            "record_info": record_info.copy(),
                        })
                    elif record.samplecnt > 0 and record.numsamples == 0:
                        # Expected samples but none decoded
                        errors.append({
                            "offset": offset,
                            "message": f"Data unpacking incomplete: expected {record.samplecnt}, got {record.numsamples}",
                            "exception_type": "DataError",
                            "record_info": record_info.copy(),
                        })

                # Advance to next record
                offset += record_length

        finally:
            # Clean up the MS3Record pointer if it was allocated
            if msr_ptr[0] != ffi.NULL:
                clibmseed.msr3_free(msr_ptr)

        return tracelist, errors
