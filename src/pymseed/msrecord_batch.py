"""Read miniSEED records as batches of columns, one array per header field.

Iterating :class:`~pymseed.MS3Record` objects costs a Python object per record
and a call per field, which dominates the cost of reading headers. An
:class:`MS3RecordBatch` holds many consecutive records' header fields as arrays
filled in C, plus the raw records themselves, so they can be handed to numpy or
Arrow as whole columns without a per-record loop in Python.

Read batches with :class:`MS3RecordBatchReader`. Batches, and the readers that
yield them, implement the Arrow PyCapsule interface, so Arrow libraries
(pyarrow, polars, DuckDB, ...) can read them directly, without copying. Arrow
export requires ``pyarrow``, installed with the ``[pyarrow]`` optional dependency.
"""

from __future__ import annotations

import functools
import os
from collections.abc import Generator, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Literal, get_args

from .clib import buffer_pointer, clibmseed, ffi
from .exceptions import MiniSEEDError
from .logging import begin_operation, clear_error_messages
from .util import require_optional

if TYPE_CHECKING:
    import pyarrow as pa

DEFAULT_BATCH_SIZE = 8192
"""Records per batch, unless set otherwise.

Like :data:`DEFAULT_BLOCK_SIZE`, a power of two, so a block of fixed-length records
of the common lengths (512 or 4096 bytes) divides into whole batches rather than
ending with a smaller one."""

DEFAULT_BLOCK_SIZE = 8 * 1024**2
"""Bytes read from a file or stream at a time. A batch never spans blocks: a record
straddling two is read with the second, so with records of 4096 bytes or more, a
batch holds at most a block's worth, plus that record."""


@functools.cache
def _arrow_schema(columns: tuple[Column, ...]) -> pa.Schema:
    """The schema of a batch with `columns`, in :data:`COLUMNS` order."""
    pa = require_optional("pyarrow", "pyarrow", min_version=14)
    timestamp = pa.timestamp("ns", tz="UTC")
    types = {
        "sourceid": pa.dictionary(pa.int32(), pa.string()),
        "starttime": timestamp,
        "endtime": timestamp,
        "samprate": pa.float64(),
        "samplecnt": pa.int64(),
        "encoding": pa.int16(),
        "pubversion": pa.uint8(),
        "formatversion": pa.uint8(),
        "record": pa.binary(),
    }
    # Only endtime is nullable: null where not representable
    return pa.schema([pa.field(c, types[c], nullable=c == "endtime") for c in columns])


Column = Literal[
    "sourceid",
    "starttime",
    "endtime",
    "samprate",
    "samplecnt",
    "encoding",
    "pubversion",
    "formatversion",
    "record",
]
"""A column a reader can read."""

COLUMNS: tuple[Column, ...] = get_args(Column)
"""The columns a reader can read, in the order batches hold them."""

# Column -> its array in PMRecordColumns
_ARRAYS = {
    "sourceid": "sid_index",
    "starttime": "starttime",
    "endtime": "endtime",
    "samprate": "samprate",
    "samplecnt": "samplecnt",
    "encoding": "encoding",
    "pubversion": "pubversion",
    "formatversion": "formatversion",
    "record": "offset",
}

# Allocate column arrays without zeroing them: the scanner writes every element read
_new = ffi.new_allocator(should_clear_after_alloc=False)

# Array -> (C type, memoryview format)
_COLUMNS: dict[str, tuple[str, Any]] = {
    "sid_index": ("int32_t", "i"),
    "starttime": ("int64_t", "q"),
    "endtime": ("int64_t", "q"),
    "samprate": ("double", "d"),
    "samplecnt": ("int64_t", "q"),
    "encoding": ("int16_t", "h"),
    "pubversion": ("uint8_t", "B"),
    "formatversion": ("uint8_t", "B"),
}


class MS3RecordBatch:
    """Header fields and raw bytes of consecutive miniSEED records, as columns.

    Read batches with :class:`MS3RecordBatchReader`. Each column is a read-only
    :class:`memoryview` with one
    element per record, e.g. ``batch.starttimes[i]``; wrap one with
    ``numpy.frombuffer`` for an array without copying. :meth:`to_arrow` converts
    the batch to a ``pyarrow.RecordBatch``, also without copying.

    A batch holds only the columns its reader was asked for; reading another
    raises :class:`ValueError`.

    Records' bytes are not copied: a batch with the ``record`` column references
    the buffer it was read from, keeping it alive. Don't modify a buffer passed to
    :meth:`MS3RecordBatchReader.from_buffer` while batches read from it are in use.
    """

    __slots__ = (
        "_data",
        "_base",
        "_count",
        "_columns",
        "_arrays",
        "_sourceids",
        "_endtime_nulls",
        "__weakref__",
    )

    def __init__(
        self,
        data: Any,
        cols: Any,
        columns: tuple[Column, ...],
        arrays: dict[str, Any],
        sourceids: list[str],
    ) -> None:
        # Internal: read batches with MS3RecordBatchReader. `data` is None without
        # the record column, so the buffer isn't kept alive; a view of it, it keeps
        # a bytearray from being resized under its records.
        self._data = data
        self._base = cols.base
        self._count = cols.count
        self._columns = columns
        self._arrays = arrays
        self._sourceids = sourceids
        self._endtime_nulls = cols.endtime_nulls

    def __len__(self) -> int:
        return self._count

    def __repr__(self) -> str:
        return f"MS3RecordBatch({self._count} records)"

    def _array(self, column: str) -> Any:
        array = self._arrays.get(_ARRAYS[column])
        if array is None:
            raise ValueError(
                f"The {column!r} column wasn't read; include it in the reader's columns"
            )
        return array

    def _column(self, column: str) -> memoryview:
        ctype, fmt = _COLUMNS[_ARRAYS[column]]
        array = self._array(column)
        return memoryview(ffi.buffer(array, self._count * ffi.sizeof(ctype))).cast(fmt).toreadonly()

    @property
    def num_records(self) -> int:
        """Number of records in the batch."""
        return self._count

    @property
    def sourceids(self) -> list[str]:
        """The batch's distinct source identifiers, indexed by :attr:`sourceid_indices`."""
        self._array("sourceid")
        return list(self._sourceids)

    @property
    def sourceid_indices(self) -> memoryview:
        """Each record's source identifier, as an index into :attr:`sourceids` (int32)."""
        return self._column("sourceid")

    @property
    def starttimes(self) -> memoryview:
        """Each record's first sample time, in nanoseconds since the epoch (int64)."""
        return self._column("starttime")

    @property
    def endtimes(self) -> memoryview:
        """Each record's last sample time, in nanoseconds since the epoch (int64).

        :data:`~pymseed.NSTERROR` where it isn't representable, e.g. with a corrupt
        sample rate; Arrow exports have nulls there.
        """
        return self._column("endtime")

    @property
    def samprates(self) -> memoryview:
        """Each record's nominal sample rate in Hz (float64)."""
        return self._column("samprate")

    @property
    def samplecnts(self) -> memoryview:
        """Each record's number of samples (int64)."""
        return self._column("samplecnt")

    @property
    def encodings(self) -> memoryview:
        """Each record's data encoding, see :class:`~pymseed.DataEncoding` (int16)."""
        return self._column("encoding")

    @property
    def pubversions(self) -> memoryview:
        """Each record's publication version (uint8)."""
        return self._column("pubversion")

    @property
    def formatversions(self) -> memoryview:
        """Each record's miniSEED format major version (uint8)."""
        return self._column("formatversion")

    @property
    def record_offsets(self) -> memoryview:
        """Where each record starts in :attr:`records`, followed by where the last ends (int32)."""
        array = self._array("record")
        return memoryview(ffi.buffer(array, (self._count + 1) * 4)).cast("i").toreadonly()

    @property
    def records(self) -> memoryview:
        """The batch's raw records, back to back."""
        end = self._base + self._array("record")[self._count]
        return self._data[self._base : end].toreadonly()

    def record(self, i: int) -> bytes:
        """Raw bytes of the batch's `i`-th record (copy)."""
        if not -self._count <= i < self._count:
            raise IndexError("record index out of range")
        i %= self._count
        offsets = self._array("record")
        return bytes(self._data[self._base + offsets[i] : self._base + offsets[i + 1]])

    def sourceid(self, i: int) -> str:
        """Source identifier of the batch's `i`-th record."""
        return self._sourceids[self.sourceid_indices[i]]

    def __arrow_c_array__(self, requested_schema: Any = None) -> tuple[Any, Any]:
        """Export the batch as ``arrow_schema`` and ``arrow_array`` PyCapsules.

        The Arrow PyCapsule interface, through :meth:`to_arrow`: a struct array with
        a field per column, referencing the batch's columns and records without
        copying them. Requires ``pyarrow``.
        """
        return self.to_arrow().__arrow_c_array__(requested_schema)

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the batch as a one-batch ``arrow_array_stream`` PyCapsule.

        For consumers that only read streams, e.g. DuckDB. Requires ``pyarrow``.
        """
        reader = MS3RecordBatchReader(iter([self]), self._columns)
        return reader.__arrow_c_stream__(requested_schema)

    def to_arrow(self) -> pa.RecordBatch:
        """Convert to a ``pyarrow.RecordBatch`` without copying the records or columns.

        Columns, of those read: ``sourceid`` (dictionary of string), ``starttime`` and ``endtime``
        (timestamp[ns, UTC]), ``samprate`` (float64), ``samplecnt`` (int64),
        ``encoding`` (int16), ``pubversion`` and ``formatversion`` (uint8), and
        ``record`` (binary, the raw record). Only ``endtime`` is nullable: null
        where the end time isn't representable, see :attr:`endtimes`.

        The Arrow arrays reference the batch's columns and records, keeping them
        alive. Requires ``pyarrow`` 14 or later (the ``[pyarrow]`` optional dependency).
        """
        pa = require_optional("pyarrow", "pyarrow", min_version=14)
        schema = _arrow_schema(self._columns)
        n = self._count
        arrays = []
        for field in schema:
            column = field.name
            if column == "sourceid":
                indices = pa.Array.from_buffers(
                    pa.int32(), n, [None, pa.py_buffer(self.sourceid_indices)]
                )
                array = pa.DictionaryArray.from_arrays(
                    indices, pa.array(self._sourceids, pa.string())
                )
            elif column == "record":
                buffers = [None, pa.py_buffer(self.record_offsets), pa.py_buffer(self.records)]
                array = pa.Array.from_buffers(field.type, n, buffers)
            elif column == "endtime" and self._endtime_nulls:
                validity = ffi.buffer(self._arrays["endtime_validity"], (n + 7) // 8)
                buffers = [pa.py_buffer(validity), pa.py_buffer(self.endtimes)]
                array = pa.Array.from_buffers(
                    field.type, n, buffers, null_count=self._endtime_nulls
                )
            else:
                buffers = [None, pa.py_buffer(self._column(column))]
                array = pa.Array.from_buffers(field.type, n, buffers)
            arrays.append(array)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)


class MS3RecordBatchReader:
    """Reads miniSEED records as :class:`MS3RecordBatch` es; also an Arrow stream.

    Create one with :meth:`from_file`, :meth:`from_buffer` or :meth:`from_filelike`.
    Iterate over it for batches, or pass it to an Arrow library to read them all
    as one stream, e.g. ``pyarrow.RecordBatchReader.from_stream(reader)``,
    ``polars.DataFrame(reader)`` or a DuckDB query. Either way, each batch is read
    once: a stream continues from wherever iteration left off.
    """

    __slots__ = ("_batches", "_columns")

    COLUMNS = COLUMNS

    def __init__(self, batches: Iterator[MS3RecordBatch], columns: tuple[Column, ...]) -> None:
        # Internal: use from_buffer(), from_file() or from_filelike()
        self._batches = batches
        self._columns = columns

    @classmethod
    def from_buffer(
        cls,
        buffer: Any,
        *,
        columns: Iterable[Column] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records in a buffer.

        Args:
            buffer: A bytes-like object holding miniSEED records back to back.
            columns: The columns to read, from :attr:`COLUMNS`; all of them by
                default. Others are neither filled nor allocated, and without
                ``record``, batches don't keep the buffer alive.
            batch_size: Records per batch, at most. A batch may hold fewer at the
                end of a block, or before input skipped with ``skip_not_data``.
            skip_not_data: Skip input that isn't miniSEED, a byte at a time, until
                a record is found, including an incomplete record at the end.
            validate_crc: Validate CRCs when present (miniSEED 3).
            verbose: libmseed verbosity.

        Raises:
            MiniSEEDError: While reading, if a record can't be parsed, or the buffer
                ends partway through one, after yielding the records before it.
                Not raised for either with ``skip_not_data``.
        """
        scanner = _Scanner(columns, batch_size, skip_not_data, validate_crc, verbose)
        return cls(_read_buffer(scanner, buffer), scanner.columns)

    @classmethod
    def from_filelike(
        cls,
        fh: Any,
        *,
        columns: Iterable[Column] | None = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records read from a file-like object.

        Reads ``block_size`` bytes at a time with ``fh.read``. A batch never spans
        blocks: a record straddling two is read with the second, which is read in
        after a copy of the record with ``fh.readinto`` if ``fh`` has it, else copied
        after the record. With the ``record`` column, a batch references the block
        it was read from.

        Args:
            fh: An object with a ``read(n)`` method returning bytes.
            block_size: Bytes read at a time.
            columns, batch_size, skip_not_data, validate_crc, verbose: As for
                :meth:`from_buffer`.

        Raises:
            MiniSEEDError: As for :meth:`from_buffer`.
        """
        if block_size <= 0:
            raise ValueError("block_size must be greater than 0")
        scanner = _Scanner(columns, batch_size, skip_not_data, validate_crc, verbose)
        return cls(_read_filelike(scanner, fh, block_size), scanner.columns)

    @classmethod
    def from_file(
        cls,
        filename: str | os.PathLike[str],
        *,
        columns: Iterable[Column] | None = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records in a file. See :meth:`from_filelike`.

        The file is opened when reading starts, and closed when it ends or the
        reader is closed.
        """
        if block_size <= 0:
            raise ValueError("block_size must be greater than 0")
        scanner = _Scanner(columns, batch_size, skip_not_data, validate_crc, verbose)
        return cls(_read_file(scanner, filename, block_size), scanner.columns)

    def __iter__(self) -> MS3RecordBatchReader:
        return self

    def __next__(self) -> MS3RecordBatch:
        return next(self._batches)

    def close(self) -> None:
        """Stop reading, closing a file opened by :meth:`from_file`."""
        close = getattr(self._batches, "close", None)
        if close is not None:
            close()

    def __enter__(self) -> MS3RecordBatchReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the remaining batches as an ``arrow_array_stream`` PyCapsule.

        The Arrow PyCapsule interface, through :meth:`to_arrow`. The consumer pulls
        batches as it reads, each referencing the batch's columns and records
        without copying them. A batch that fails to read ends the stream with an
        error. Requires ``pyarrow``.
        """
        return self.to_arrow().__arrow_c_stream__(requested_schema)

    def to_arrow(self) -> pa.RecordBatchReader:
        """The remaining batches as a ``pyarrow.RecordBatchReader``, read as it's read.

        Requires ``pyarrow`` 14 or later (the ``[pyarrow]`` optional dependency).
        """
        pa = require_optional("pyarrow", "pyarrow", min_version=14)
        schema = _arrow_schema(self._columns)
        return pa.RecordBatchReader.from_batches(schema, (batch.to_arrow() for batch in self))


def _read_buffer(scanner: _Scanner, buffer: Any) -> Iterator[MS3RecordBatch]:
    begin_operation()
    yield from scanner.scan(buffer, 0, at_end=True)


def _read_filelike(scanner: _Scanner, fh: Any, block_size: int) -> Iterator[MS3RecordBatch]:
    begin_operation()
    readinto = getattr(fh, "readinto", None)
    tail = b""
    position = 0  # Offset in the stream of `tail`
    while True:
        data = _read_block(fh, readinto, tail, block_size)
        if not data:
            return
        at_end = len(data) == len(tail)
        consumed = yield from scanner.scan(data, 0, at_end, position)
        tail = data[consumed:]
        position += consumed
        if at_end:
            return


def _read_block(fh: Any, readinto: Any, tail: bytes, block_size: int) -> Any:
    """`tail`, the start of a record straddling blocks, then the next block from `fh`.

    Without a tail, the block is used as `fh.read` returns it. With one, and with
    `readinto`, the block is read in after a copy of `tail`, rather than copied after
    it. Each block is newly allocated, since batches keep referencing theirs.
    """
    if not tail or readinto is None:
        chunk = fh.read(block_size) or b""
        return tail + chunk if tail else chunk
    block = _new("char[]", len(tail) + block_size)
    ffi.memmove(block, tail, len(tail))
    read = readinto(ffi.buffer(block + len(tail), block_size)) or 0
    return ffi.buffer(block, len(tail) + read)


def _read_file(
    scanner: _Scanner, filename: str | os.PathLike[str], block_size: int
) -> Iterator[MS3RecordBatch]:
    with open(filename, "rb") as fh:
        yield from _read_filelike(scanner, fh, block_size)


class _Scanner:
    """Runs `pm_scan_records` over a buffer, a batch at a time."""

    def __init__(
        self,
        columns: Iterable[Column] | None,
        batch_size: int,
        skip_not_data: bool,
        validate_crc: bool,
        verbose: int,
    ) -> None:
        if isinstance(columns, str):
            raise TypeError("columns must be a list of column names, not a string")
        requested = set(COLUMNS if columns is None else columns)
        if unknown := requested - set(COLUMNS):
            raise ValueError(f"Unknown columns {sorted(unknown)}; choose from {list(COLUMNS)}")
        if not requested:
            raise ValueError("columns must name at least one column")
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        # In COLUMNS order, the order batches hold them
        self.columns = tuple(c for c in COLUMNS if c in requested)
        self.batch_size = batch_size
        self.flags = (clibmseed.MSF_VALIDATECRC if validate_crc else 0) | (
            clibmseed.MSF_SKIPNOTDATA if skip_not_data else 0
        )
        self.verbose = verbose
        self.consumed = ffi.new("uint64_t *")
        if "sourceid" in self.columns:
            # Scratch space for finding source IDs, reused for every batch: a hash
            # table at most half full, since a batch has no more source IDs than records
            slots = 1 << (2 * batch_size - 1).bit_length()
            self.sid_slots = _new("int32_t[]", slots)
            self.sid_slot_mask = slots - 1
            self.sid_hashes = _new("uint32_t[]", batch_size)

    def scan(
        self, data: Any, start: int, at_end: bool, position: int = 0
    ) -> Generator[MS3RecordBatch, None, int]:
        """Yield batches of the records in `data` from `start`; return where they end.

        At the end of the stream (`at_end`), raise if anything follows the last record,
        unless skipping non-data.
        `position` is the offset of `data` in the stream, for error messages.
        """
        flags = self.flags | (clibmseed.MSF_ATENDOFFILE if at_end else 0)
        ptr = buffer_pointer(data)
        size = len(ptr)
        # Keeps a bytearray from being resized under the batches' records, and
        # indexes them by byte, as the offsets are
        view = memoryview(data).cast("B") if "record" in self.columns else None
        self.consumed[0] = start
        while True:
            cols, arrays = self._columns()
            clear_error_messages()
            status = clibmseed.pm_scan_records(ptr, size, flags, self.verbose, cols, self.consumed)
            # Created now to capture libmseed's log, but raised after yielding the
            # records before the one that failed
            error = (
                MiniSEEDError(
                    status, f"Error parsing miniSEED record at byte {position + self.consumed[0]}"
                )
                if status < 0
                else None
            )
            if cols.count:
                sourceids = [
                    ffi.string(cols.sids + i * clibmseed.LM_SIDLEN).decode("utf-8")
                    for i in range(cols.nsids)
                ]
                yield MS3RecordBatch(view, cols, self.columns, arrays, sourceids)
            if error is not None:
                raise error
            if status == clibmseed.PM_SCAN_NEEDMORE:
                break

        consumed = self.consumed[0]
        if at_end and self.flags & clibmseed.MSF_SKIPNOTDATA:
            return size
        if at_end and consumed < size:
            code = (
                clibmseed.MS_NOTSEED
                if position + consumed == 0 and size < clibmseed.MINRECLEN
                else clibmseed.MS_ENDOFFILE
            )
            raise MiniSEEDError(
                code,
                f"Stream ends with {size - consumed} bytes of an incomplete record "
                f"at byte {position + consumed}",
            )
        return consumed

    def _columns(self) -> tuple[Any, dict[str, Any]]:
        """Allocate a batch's arrays for the columns read, leaving the rest NULL."""
        n = self.batch_size
        arrays: dict[str, Any] = {}
        for column in self.columns:
            name = _ARRAYS[column]
            if column == "record":
                arrays[name] = _new("int32_t[]", n + 1)
            else:
                arrays[name] = _new(f"{_COLUMNS[name][0]}[]", n)
        if "sourceid" in self.columns:
            # Pages are only touched for the source IDs a batch actually holds
            arrays["sids"] = _new("char[]", n * clibmseed.LM_SIDLEN)
        if "endtime" in self.columns:
            # Only written if the batch has a null
            arrays["endtime_validity"] = _new("uint8_t[]", (n + 7) // 8)
        cols = ffi.new("PMRecordColumns *")
        cols.capacity = n
        for name, array in arrays.items():
            setattr(cols, name, array)
        if "sourceid" in self.columns:
            cols.sid_slots = self.sid_slots
            cols.sid_slot_mask = self.sid_slot_mask
            cols.sid_hashes = self.sid_hashes
        return cols, arrays
