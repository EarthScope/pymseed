"""Read miniSEED records as batches of columns, one array per header field.

Iterating :class:`~pymseed.MS3Record` objects costs a Python object per record
and a call per field, which dominates the cost of reading headers. An
:class:`MS3RecordBatch` holds many consecutive records' header fields as arrays
filled in C, plus the raw records themselves, so they can be handed to numpy or
Arrow as whole columns without a per-record loop in Python.

Read batches with :class:`MS3RecordBatchReader`. Batches, and the readers that
yield them, implement the Arrow PyCapsule
interface, so Arrow libraries (pyarrow, polars, DuckDB, nanoarrow, ...) can read
them directly, without copying and without pymseed depending on any of them.
"""

from __future__ import annotations

import ctypes
import errno
import functools
import os
from collections.abc import Generator, Iterator
from typing import TYPE_CHECKING, Any

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .logging import begin_operation

if TYPE_CHECKING:
    import pyarrow as pa

DEFAULT_BATCH_SIZE = 8192
"""Records per batch, unless set otherwise.

Like :data:`DEFAULT_BLOCK_SIZE`, a power of two, so a block of fixed-length records
of the common lengths (512 or 4096 bytes) divides into whole batches rather than
ending with a smaller one."""

DEFAULT_BLOCK_SIZE = 8 * 1024**2
"""Bytes read from a file or stream at a time. A batch never spans blocks, so with
records of 4096 bytes or more, a batch holds at most a block's worth."""

# Exported batches, by the address of the handle each export's C state holds, so
# a batch stays alive until every consumer has released its arrays
_exports: dict[int, Any] = {}


@ffi.def_extern()  # type: ignore[untyped-decorator]
def pm_arrow_release_owner(owner: Any) -> None:
    """Called from C, on any thread, when the last array of an export is released."""
    _exports.pop(int(ffi.cast("uintptr_t", owner)), None)


# Exported readers, by the address of the handle each stream's C state holds
_streams: dict[int, Any] = {}


def _export_handle(registry: dict[int, Any], obj: Any) -> Any:
    """A handle to `obj` for C to hold, keeping `obj` alive until C releases it."""
    handle = ffi.new_handle(obj)
    registry[int(ffi.cast("uintptr_t", handle))] = handle
    return handle


@ffi.def_extern()  # type: ignore[untyped-decorator]
def pm_arrow_stream_next(source: Any, out: Any, error: Any) -> int:
    """Called from C, on any thread, for a stream's next batch."""
    reader: MS3RecordBatchReader = ffi.from_handle(source)
    try:
        batch = next(reader, None)
        if batch is None:
            out.release = ffi.NULL  # end of stream
            return 0
        owner = _export_handle(_exports, batch)
        if clibmseed.pm_arrow_export_array(batch._cols, batch._pointer + batch._base, owner, out):
            raise MemoryError("Cannot export miniSEED record batch")
        return 0
    except BaseException as e:  # nothing may propagate into C
        message = f"{type(e).__name__}: {e}".encode("utf-8", "replace")
        message = message[: clibmseed.PM_ARROW_ERROR_LEN - 1] + b"\0"
        ffi.memmove(error, message, len(message))
        return errno.EIO


@ffi.def_extern()  # type: ignore[untyped-decorator]
def pm_arrow_stream_release(source: Any) -> None:
    """Called from C, on any thread, when a stream is released."""
    _streams.pop(int(ffi.cast("uintptr_t", source)), None)


def _import_pyarrow() -> Any:
    """Import pyarrow for the `to_arrow()` conveniences, which need 14 or later."""
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError(
            "to_arrow() requires pyarrow 14 or later (pip install 'pyarrow>=14'). "
            "Other Arrow libraries can read batches and readers directly, e.g. "
            "polars.DataFrame(reader)."
        ) from e
    if int(pa.__version__.split(".")[0]) < 14:
        raise ImportError(
            f"to_arrow() requires pyarrow 14 or later, to import Arrow PyCapsules; "
            f"found {pa.__version__} (pip install 'pyarrow>=14')"
        )
    return pa


def _schema_capsule() -> Any:
    """The batch schema as an ``arrow_schema`` PyCapsule: a struct with a field per column.

    Not exported as ``__arrow_c_schema__``, which marks schema-like objects: batches
    and readers are data, and carry their schema in their array and stream exports.
    """
    _pycapsule_new()  # fail before allocating anything, outside CPython
    return _capsule(
        clibmseed.pm_arrow_new_schema(), _ARROW_SCHEMA, "pm_arrow_schema_capsule_destructor"
    )


def _capsule(pointer: Any, name: bytes, destructor: str) -> Any:
    """Wrap a C struct that the named C destructor frees in a PyCapsule."""
    if pointer == ffi.NULL:
        raise MemoryError("Cannot export miniSEED record batch")
    address = int(ffi.cast("uintptr_t", pointer))
    destructor_address = int(ffi.cast("uintptr_t", ffi.addressof(clibmseed, destructor)))
    return _pycapsule_new()(address, name, destructor_address)


# The capsule names must outlive their capsules, so they're module-level constants
_ARROW_SCHEMA = b"arrow_schema"
_ARROW_ARRAY = b"arrow_array"
_ARROW_ARRAY_STREAM = b"arrow_array_stream"


@functools.cache
def _pycapsule_new() -> Any:
    """CPython's PyCapsule_New, which only CPython provides."""
    pythonapi = getattr(ctypes, "pythonapi", None)
    if pythonapi is None:
        raise NotImplementedError("Arrow export requires CPython")
    new = pythonapi.PyCapsule_New
    new.restype = ctypes.py_object
    new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    return new


# Allocate column arrays without zeroing them: the scanner writes every element read
_new = ffi.new_allocator(should_clear_after_alloc=False)

# Column name -> (C type, memoryview format)
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

    Records' bytes are not copied: a batch references the buffer it was read
    from, keeping it alive. Don't modify a buffer passed to
    :meth:`MS3RecordBatchReader.from_buffer` while batches read from it are in use.
    """

    __slots__ = (
        "_data",
        "_pointer",
        "_cols",
        "_base",
        "_count",
        "_arrays",
        "_sourceids",
        "__weakref__",
    )

    def __init__(
        self,
        data: Any,
        pointer: Any,
        cols: Any,
        arrays: dict[str, Any],
        sourceids: list[str],
    ) -> None:
        # Internal: use from_buffer(), from_file() or from_filelike()
        self._data = data
        # Holds `data`'s buffer export, so a bytearray can't be resized under its records
        self._pointer = pointer
        self._cols = cols
        self._base = cols.base
        self._count = cols.count
        self._arrays = arrays
        self._sourceids = sourceids

    def __len__(self) -> int:
        return self._count

    def __repr__(self) -> str:
        return f"MS3RecordBatch({self._count} records, {len(self._sourceids)} source IDs)"

    def _column(self, name: str) -> memoryview:
        ctype, fmt = _COLUMNS[name]
        array = self._arrays[name]
        return memoryview(ffi.buffer(array, self._count * ffi.sizeof(ctype))).cast(fmt).toreadonly()

    @property
    def num_records(self) -> int:
        """Number of records in the batch."""
        return self._count

    @property
    def sourceids(self) -> list[str]:
        """The batch's distinct source identifiers, indexed by :attr:`sourceid_indices`."""
        return list(self._sourceids)

    @property
    def sourceid_indices(self) -> memoryview:
        """Each record's source identifier, as an index into :attr:`sourceids` (int32)."""
        return self._column("sid_index")

    @property
    def starttimes(self) -> memoryview:
        """Each record's first sample time, in nanoseconds since the epoch (int64)."""
        return self._column("starttime")

    @property
    def endtimes(self) -> memoryview:
        """Each record's last sample time, in nanoseconds since the epoch (int64)."""
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
        array = self._arrays["offset"]
        return memoryview(ffi.buffer(array, (self._count + 1) * 4)).cast("i").toreadonly()

    @property
    def records(self) -> memoryview:
        """The batch's raw records, back to back."""
        end = self._base + self._arrays["offset"][self._count]
        return memoryview(self._data)[self._base : end].toreadonly()

    def record(self, i: int) -> bytes:
        """Raw bytes of the batch's `i`-th record (copy)."""
        if not -self._count <= i < self._count:
            raise IndexError("record index out of range")
        i %= self._count
        offsets = self._arrays["offset"]
        return bytes(memoryview(self._data)[self._base + offsets[i] : self._base + offsets[i + 1]])

    def sourceid(self, i: int) -> str:
        """Source identifier of the batch's `i`-th record."""
        return self._sourceids[self.sourceid_indices[i]]

    def __arrow_c_array__(self, requested_schema: Any = None) -> tuple[Any, Any]:
        """Export the batch as ``arrow_schema`` and ``arrow_array`` PyCapsules.

        The Arrow PyCapsule interface: the array is a struct array with a field per
        column, referencing the batch's columns and records without copying them.
        The batch stays alive until the consumer releases the array.
        ``requested_schema`` is ignored; the batch is always exported as it is.
        """
        schema = _schema_capsule()
        owner = _export_handle(_exports, self)
        array = clibmseed.pm_arrow_new_array(self._cols, self._pointer + self._base, owner)
        return schema, _capsule(array, _ARROW_ARRAY, "pm_arrow_array_capsule_destructor")

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the batch as a one-batch ``arrow_array_stream`` PyCapsule.

        For consumers that only read streams, e.g. DuckDB.
        """
        return MS3RecordBatchReader(iter([self])).__arrow_c_stream__(requested_schema)

    def to_arrow(self) -> pa.RecordBatch:
        """Convert to a ``pyarrow.RecordBatch`` without copying the records or columns.

        Columns: ``sourceid`` (dictionary of string), ``starttime`` and ``endtime``
        (timestamp[ns, UTC]), ``samprate`` (float64), ``samplecnt`` (int64),
        ``encoding`` (int16), ``pubversion`` and ``formatversion`` (uint8), and
        ``record`` (binary, the raw record).

        Requires ``pyarrow`` 14 or later, which pymseed doesn't install. Other Arrow
        libraries can read a batch directly, e.g. ``polars.DataFrame(batch)``.
        """
        pa = _import_pyarrow()
        return pa.record_batch(self)


class MS3RecordBatchReader:
    """Reads miniSEED records as :class:`MS3RecordBatch` es; also an Arrow stream.

    Create one with :meth:`from_file`, :meth:`from_buffer` or :meth:`from_filelike`.
    Iterate over it for batches, or pass it to an Arrow library to read them all
    as one stream, e.g. ``pyarrow.RecordBatchReader.from_stream(reader)``,
    ``polars.DataFrame(reader)`` or a DuckDB query. Either way, each batch is read
    once: a stream continues from wherever iteration left off.
    """

    __slots__ = ("_batches",)

    def __init__(self, batches: Iterator[MS3RecordBatch]) -> None:
        # Internal: use from_buffer(), from_file() or from_filelike()
        self._batches = batches

    @classmethod
    def from_buffer(
        cls,
        buffer: Any,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records in a buffer.

        Args:
            buffer: A bytes-like object holding miniSEED records back to back.
            batch_size: Records per batch, at most. A batch may hold fewer at the
                end of a block.
            validate_crc: Validate CRCs when present (miniSEED 3).
            verbose: libmseed verbosity.

        Raises:
            MiniSEEDError: While reading, if a record can't be parsed, or the buffer
                ends partway through one. Batches already yielded are complete and valid.
        """
        scanner = _Scanner(batch_size, validate_crc, verbose)
        return cls(_read_buffer(scanner, buffer))

    @classmethod
    def from_filelike(
        cls,
        fh: Any,
        *,
        block_size: int = DEFAULT_BLOCK_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records read from a file-like object.

        Reads ``block_size`` bytes at a time with ``fh.read``. A batch never spans
        blocks, and references the block it was read from.

        Args:
            fh: An object with a ``read(n)`` method returning bytes.
            block_size: Bytes read at a time.
            batch_size, validate_crc, verbose: As for :meth:`from_buffer`.

        Raises:
            MiniSEEDError: As for :meth:`from_buffer`.
        """
        if block_size <= 0:
            raise ValueError("block_size must be greater than 0")
        scanner = _Scanner(batch_size, validate_crc, verbose)
        return cls(_read_filelike(scanner, fh, block_size))

    @classmethod
    def from_file(
        cls,
        filename: str | os.PathLike[str],
        *,
        block_size: int = DEFAULT_BLOCK_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        validate_crc: bool = True,
        verbose: int = 0,
    ) -> MS3RecordBatchReader:
        """Read batches of the records in a file. See :meth:`from_filelike`.

        The file is opened when reading starts, and closed when it ends or the
        reader is closed.
        """
        if block_size <= 0:
            raise ValueError("block_size must be greater than 0")
        scanner = _Scanner(batch_size, validate_crc, verbose)
        return cls(_read_file(scanner, filename, block_size))

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

        The Arrow PyCapsule interface. The consumer pulls batches as it reads,
        possibly from another thread. Each is a struct array referencing the
        batch's columns and records without copying them; see
        :meth:`MS3RecordBatch.to_arrow` for the columns. A batch that fails to read
        ends the stream with an error. ``requested_schema`` is ignored.
        """
        _pycapsule_new()  # fail before allocating anything, outside CPython
        source = _export_handle(_streams, self)
        stream = clibmseed.pm_arrow_new_stream(source)
        return _capsule(stream, _ARROW_ARRAY_STREAM, "pm_arrow_stream_capsule_destructor")

    def to_arrow(self) -> pa.RecordBatchReader:
        """The remaining batches as a ``pyarrow.RecordBatchReader``.

        Requires ``pyarrow`` 14 or later, which pymseed doesn't install.
        """
        pa = _import_pyarrow()
        return pa.RecordBatchReader.from_stream(self)


def _read_buffer(scanner: _Scanner, buffer: Any) -> Iterator[MS3RecordBatch]:
    begin_operation()
    yield from scanner.scan(buffer, 0, at_end=True)


def _read_filelike(scanner: _Scanner, fh: Any, block_size: int) -> Iterator[MS3RecordBatch]:
    begin_operation()
    tail = b""
    while True:
        chunk = fh.read(block_size)
        at_end = not chunk
        data = tail + chunk if tail else chunk
        if not data:
            return
        consumed = yield from scanner.scan(data, 0, at_end)
        tail = data[consumed:]
        if at_end:
            return


def _read_file(
    scanner: _Scanner, filename: str | os.PathLike[str], block_size: int
) -> Iterator[MS3RecordBatch]:
    with open(filename, "rb") as fh:
        yield from _read_filelike(scanner, fh, block_size)


class _Scanner:
    """Runs `pm_scan_records` over a buffer, a batch at a time."""

    def __init__(self, batch_size: int, validate_crc: bool, verbose: int) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        self.batch_size = batch_size
        self.flags = clibmseed.MSF_VALIDATECRC if validate_crc else 0
        self.verbose = verbose
        self.consumed = ffi.new("uint64_t *")
        # Scratch space for finding source IDs, reused for every batch: a hash
        # table at most half full, since a batch has no more source IDs than records
        slots = 1 << (2 * batch_size - 1).bit_length()
        self.sid_slots = _new("int32_t[]", slots)
        self.sid_slot_mask = slots - 1
        self.sid_hashes = _new("uint32_t[]", batch_size)

    def scan(self, data: Any, start: int, at_end: bool) -> Generator[MS3RecordBatch, None, int]:
        """Yield batches of the records in `data` from `start`; return where they end.

        At the end of the stream (`at_end`), raise if anything follows the last record.
        """
        flags = self.flags | (clibmseed.MSF_ATENDOFFILE if at_end else 0)
        ptr = ffi.from_buffer(data)
        size = len(ptr)
        self.consumed[0] = start
        while True:
            cols, arrays = self._columns()
            status = clibmseed.pm_scan_records(ptr, size, flags, self.verbose, cols, self.consumed)
            if status < 0:
                raise MiniSEEDError(
                    status, f"Error parsing miniSEED record at byte {self.consumed[0]}"
                )
            if cols.count:
                sourceids = [
                    ffi.string(cols.sids + i * clibmseed.LM_SIDLEN).decode("utf-8")
                    for i in range(cols.nsids)
                ]
                yield MS3RecordBatch(data, ptr, cols, arrays, sourceids)
            if status == clibmseed.PM_SCAN_NEEDMORE:
                break

        consumed = self.consumed[0]
        if at_end and consumed < size:
            code = (
                clibmseed.MS_NOTSEED
                if consumed == 0 and size < clibmseed.MINRECLEN
                else clibmseed.MS_ENDOFFILE
            )
            raise MiniSEEDError(
                code, f"Stream ends with {size - consumed} bytes of an incomplete record"
            )
        return consumed

    def _columns(self) -> tuple[Any, dict[str, Any]]:
        n = self.batch_size
        arrays = {name: _new(f"{ctype}[]", n) for name, (ctype, _) in _COLUMNS.items()}
        arrays["offset"] = _new("int32_t[]", n + 1)
        # Pages are only touched for the source IDs a batch actually holds
        arrays["sids"] = _new("char[]", n * clibmseed.LM_SIDLEN)
        cols = ffi.new("PMRecordColumns *")
        cols.capacity = n
        for name, array in arrays.items():
            setattr(cols, name, array)
        cols.sid_slots = self.sid_slots
        cols.sid_slot_mask = self.sid_slot_mask
        cols.sid_hashes = self.sid_hashes
        return cols, arrays
