"""
Core trace list implementation for pymseed

"""

from __future__ import annotations

from collections.abc import Sequence
from time import time
from typing import Any, Callable, Optional

from .clib import cdata_to_string, clibmseed, ffi
from .definitions import DataEncoding, SubSecond, TimeFormat
from .exceptions import MiniSEEDError
from .msrecord import MS3Record
from .util import encoding_sizetype, nstime2timestr


class MS3RecordPtr:
    """Wrapper around CFFI MS3RecordPtr structure"""

    def __init__(self, cffi_ptr: Any) -> None:
        self._ptr = cffi_ptr

    def __repr__(self) -> str:
        return (
            f"MS3RecordPtr(sourceid: {self.record.sourceid}\n"
            f"             filename: {cdata_to_string(self._ptr.filename)}\n"
            f"           fileoffset: {self._ptr.fileoffset}\n"
            f"            bufferptr: {self._ptr.bufferptr}\n"
            f"               reclen: {self._ptr.msr.reclen}\n"
            f"            starttime: {self.record.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_Z)}\n"
            f"              endtime: {self.record.endtime_str(timeformat=TimeFormat.ISOMONTHDAY_Z)}\n"
            ")"
        )

    def __str__(self) -> str:
        return (
            f"{self.record.sourceid}, "
            f"{cdata_to_string(self._ptr.filename)}, "
            f"byte offset: {self._ptr.fileoffset}"
        )

    @property
    def record(self) -> MS3Record:
        """Return a constructed MS3Record"""
        if not hasattr(self, "_msrecord"):
            self._msrecord = MS3Record(recordptr=self._ptr.msr)
        return self._msrecord

    @property
    def filename(self) -> Optional[str]:
        """Return filename as string"""
        result = cdata_to_string(self._ptr.filename)
        if result is None:
            return None
        return result

    @property
    def fileoffset(self) -> int:
        """Return file offset"""
        return self._ptr.fileoffset

    @property
    def endtime(self) -> int:
        """Return end time"""
        return self._ptr.endtime

    @property
    def dataoffset(self) -> int:
        """Return data offset"""
        return self._ptr.dataoffset


class MS3RecordList:
    """Wrapper around CFFI MS3RecordList structure

    This class supports list-like access to the record pointers:
    - len(record_list) returns the number of records
    - record_list[i] returns the i-th record pointer
    - record_list[start:end] returns a slice of record pointers
    - for record_ptr in record_list: iterates over all record pointers
    """

    def __init__(self, cffi_ptr: Any) -> None:
        self._list = cffi_ptr

    def __repr__(self) -> str:
        def indent_repr(thing):
            """Add two-space indentation to each line of repr(thing)"""
            return "\n".join("  " + line for line in repr(thing).split("\n"))

        # Create list of formatted strings
        if len(self) <= 5:
            formatted_lines = [indent_repr(recptr) for recptr in self]
        else:
            formatted_lines = [
                indent_repr(self[0]),
                indent_repr(self[1]),
                f"  ... {len(self) - 4} more",
                indent_repr(self[-2]),
                indent_repr(self[-1]),
            ]

        newline = "\n"
        return f"MS3RecordList(recordcnt: {len(self)}\n{newline.join(formatted_lines)}\n)"

    def __str__(self) -> str:
        def indent_str(thing):
            """Add two-space indentation to each line of str(thing)"""
            return "\n".join("  " + line for line in str(thing).split("\n"))

        # Create list of formatted strings
        if len(self) <= 5:
            formatted_lines = [indent_str(recptr) for recptr in self]
        else:
            formatted_lines = [
                indent_str(self[0]),
                indent_str(self[1]),
                f"  ... {len(self) - 4} more",
                indent_str(self[-2]),
                indent_str(self[-1]),
            ]

        newline = "\n"
        return f"Record list with {len(self)} records\n{newline.join(formatted_lines)}"

    def __len__(self) -> int:
        """Return number of records"""
        return self._list.recordcnt

    def __iter__(self) -> Any:
        """Return iterator over record pointers"""
        current_record = self._list.first
        while current_record != ffi.NULL:
            yield MS3RecordPtr(current_record)
            current_record = current_record.next

    def __getitem__(self, key: int | slice) -> Any:
        """Enable indexing and slicing access to record pointers"""
        if isinstance(key, slice):
            # Handle slice objects (e.g., record_list[1:3], record_list[::2])
            record_list = list(self)
            return record_list[key]
        elif isinstance(key, int):
            # Handle single integer index
            length = len(self)
            if length == 0:
                raise IndexError("list index out of range")

            # Handle negative indices
            if key < 0:
                key += length

            # Check bounds
            if key < 0 or key >= length:
                raise IndexError("list index out of range")

            # Find and return the record at the specified index
            for i, record in enumerate(self):
                if i == key:
                    return record

            # This shouldn't happen if our logic is correct
            raise IndexError("list index out of range")
        else:
            raise TypeError("indices must be integers or slices")

    @property
    def recordcnt(self) -> int:
        """Return record count"""
        return self._list.recordcnt

    def records(self) -> Any:
        """Return the records via a generator iterator"""
        current_record = self._list.first
        while current_record != ffi.NULL:
            yield MS3RecordPtr(current_record)
            current_record = current_record.next


class MS3TraceSeg:
    """Wrapper around CFFI MS3TraceSeg structure"""

    def __init__(
        self, cffi_ptr: Any, parent_id_ptr: Any = None, parent_tracelist: Any = None
    ) -> None:
        self._seg = cffi_ptr
        self._parent_id = parent_id_ptr  # Reference to parent MS3TraceID
        self._parent_tracelist = parent_tracelist  # Reference to parent MS3TraceList

    def __repr__(self) -> str:
        sample_preview = "[]"
        if self.numsamples > 0:
            if len(self.datasamples) > 5:
                # Create array representation with ellipsis inside: [1,2,3,4,5,...]
                first_samples = ", ".join(str(sample) for sample in list(self.datasamples[:5]))
                sample_preview = f"[{first_samples}, ...]"
            else:
                sample_preview = str(list(self.datasamples))

        return (
            f"MS3TraceSeg(start: {self.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}\n"
            f"              end: {self.endtime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}\n"
            f"         samprate: {self.samprate}\n"
            f"        samplecnt: {self.samplecnt}\n"
            f"      datasamples: {sample_preview}\n"
            f"         datasize: {self.datasize}\n"
            f"       numsamples: {self.numsamples}\n"
            f"       sampletype: {self.sampletype}\n"
            f"       recordlist: {'Record list of ' + str(len(self.recordlist)) + ' records' if self.recordlist else 'None'}\n"
            ")"
        )

    def __str__(self) -> str:
        return (
            f"start: {self.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}, "
            f"end: {self.endtime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}, "
            f"samprate: {self.samprate}, "
            f"samples: {self.samplecnt} "
        )

    @property
    def starttime(self) -> int:
        """Return start time as nanoseconds since Unix/POSIX epoch"""
        return self._seg.starttime

    @property
    def starttime_seconds(self) -> float:
        """Return start time as seconds since Unix/POSIX epoch"""
        return self._seg.starttime / clibmseed.NSTMODULUS

    def starttime_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return start time as formatted string"""
        result = nstime2timestr(self._seg.starttime, timeformat, subsecond)
        if result is None:
            return None
        return result

    @property
    def endtime(self) -> int:
        """Return end time as nanoseconds since Unix/POSIX epoch"""
        return self._seg.endtime

    @property
    def endtime_seconds(self) -> float:
        """Return end time as seconds since Unix/POSIX epoch"""
        return self._seg.endtime / clibmseed.NSTMODULUS

    def endtime_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return end time as formatted string"""
        result = nstime2timestr(self._seg.endtime, timeformat, subsecond)
        if result is None:
            return None
        return result

    @property
    def samprate(self) -> float:
        """Return sample rate"""
        return self._seg.samprate

    @property
    def samplecnt(self) -> int:
        """Return sample count"""
        return self._seg.samplecnt

    @property
    def recordlist(self) -> Optional[MS3RecordList]:
        """Return the record list structure"""
        if self._seg.recordlist:
            return MS3RecordList(self._seg.recordlist)
        else:
            return None

    @property
    def datasamples(self) -> memoryview:
        """Return data samples as a memoryview (no copy)

        A view of the data samples in an internal buffer owned by this MS3Record
        instance is returned.  If the data are needed beyond the lifetime of this
        instance, a copy must be made.

        The returned view can be used directly with slicing and indexing
        from `0` to `MS3TraceSeg.numsamples - 1`.

        The view can efficiently be copied to a _python list_ using:

            data_samples = MS3TraceSeg.datasamples[:]
        """
        if self._seg.numsamples <= 0:
            return memoryview(b"")  # Empty memoryview

        sampletype = self.sampletype

        if sampletype == "i":
            ptr = ffi.cast("int32_t *", self._seg.datasamples)
            buffer = ffi.buffer(ptr, self._seg.numsamples * ffi.sizeof("int32_t"))
            return memoryview(buffer).cast("i")
        elif sampletype == "f":
            ptr = ffi.cast("float *", self._seg.datasamples)
            buffer = ffi.buffer(ptr, self._seg.numsamples * ffi.sizeof("float"))
            return memoryview(buffer).cast("f")
        elif sampletype == "d":
            ptr = ffi.cast("double *", self._seg.datasamples)
            buffer = ffi.buffer(ptr, self._seg.numsamples * ffi.sizeof("double"))
            return memoryview(buffer).cast("d")
        elif sampletype == "t":
            ptr = ffi.cast("char *", self._seg.datasamples)
            buffer = ffi.buffer(ptr, self._seg.numsamples)
            return memoryview(buffer).cast("B")
        else:
            raise ValueError(f"Unknown sample type: {sampletype}")

    @property
    def sampletype(self) -> Optional[str]:
        """Return sample type code if available, otherwise None"""
        if self._seg.sampletype:
            return str(self._seg.sampletype.decode("ascii"))
        else:
            return None

    @property
    def numsamples(self) -> int:
        """Return number of samples"""
        return self._seg.numsamples

    @property
    def datasize(self) -> int:
        """Return data size in bytes"""
        return self._seg.datasize

    @property
    def sample_size_type(self) -> tuple[int, str]:
        """Return data sample size and type code from first record in list

        NOTE: This is a guesstimate based on the first record in the record list.
        It is not guaranteed to be correct for any other records in the list.
        """
        if self._seg.recordlist is None:
            raise ValueError("No record list available to determine sample size and type")

        # Get the first record
        first_record_ptr = self._seg.recordlist.first

        if first_record_ptr is None:
            raise ValueError("No records in record list")

        return encoding_sizetype(first_record_ptr.msr.encoding)

    @property
    def np_datasamples(self) -> Any:
        """Return data samples as a numpy array (no copy)

        A view of the data samples in an internal buffer owned by this MS3TraceSeg
        instance is returned. If the data are needed beyond the lifetime of this
        instance, a copy must be made.
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "numpy is not installed. Install numpy or this package with [numpy] optional dependency"
            ) from None

        if self._seg.numsamples <= 0:
            return np.array([])  # Empty array

        sampletype = self.sampletype

        # Translate libmseed sample type to numpy type
        nptype = {
            "i": np.int32,
            "f": np.float32,
            "d": np.float64,
            "t": "S1",  # 1-byte strings for text data
        }

        if sampletype not in nptype:
            raise ValueError(f"Unknown sample type: {sampletype}")

        # Create numpy array view from CFFI buffer
        return np.frombuffer(self.datasamples, dtype=nptype[sampletype])

    def create_numpy_array_from_recordlist(self) -> Any:
        """Return data samples as a numpy array unpacked from the record list

        The numpy array returned is an independent copy of the data samples.
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "numpy is not installed. Install numpy or this package with [numpy] optional dependency"
            ) from None

        if self.recordlist is None:
            raise ValueError(
                "Record list required, use record_list=True when populating MS3TraceList"
            )

        if self.samplecnt <= 0:
            return np.array([])  # Empty array

        (_, sample_type) = self.sample_size_type

        # Translate libmseed sample type to numpy type
        nptype = {
            "i": np.int32,
            "f": np.float32,
            "d": np.float64,
            "t": "S1",  # 1-byte strings for text data
        }

        if sample_type not in nptype:
            raise ValueError(f"Unknown sample type: {sample_type}")

        # Create numpy array of the correct type and size
        array = np.empty(self.samplecnt, dtype=nptype[sample_type])

        # Unpack data samples into the array
        self.unpack_recordlist(buffer=array)

        return array

    def unpack_recordlist(self, buffer: Any = None, verbose: int = 0) -> int:
        """Unpack data samples from miniSEED record list into accessible format

        This method decodes data samples from the original miniSEED records that were
        stored when reading with `record_list=True`. It's used for memory-efficient
        workflows where you delay data unpacking until needed.

        Args:
            buffer: Optional destination buffer for unpacked data. Must support the
                buffer protocol (e.g., numpy array, bytearray, memoryview). If provided,
                must be large enough to hold `self.samplecnt` samples of the appropriate
                data type. If None, data is unpacked into internal memory owned by this
                segment instance.
            verbose: Verbosity level for diagnostic output (0=quiet, 1-3=increasing
                detail). Default: 0

        Returns:
            Number of samples successfully unpacked

        Raises:
            ValueError: If no record list is available (requires `record_list=True` when
                reading), if data is already unpacked and a buffer is provided, or if
                the provided buffer doesn't support the buffer protocol
            MiniSEEDError: If unpacking fails due to corrupted or invalid record data

        Note:
            - Requires the segment to have been created with `record_list=True`
            - Can only be called once per segment if using internal memory (buffer=None)
            - If using a provided buffer, the buffer format must match the segment's
              sample type (int32 for "i", float32 for "f", float64 for "d", bytes for "t")
            - For performance, use memoryviews with matching dtype when providing buffers

        Examples:
            Basic workflow illustrating how to use unpack_recordlist():

            >>> from pymseed import MS3TraceList

            Basic unpacking to internal memory:
            >>> traces = MS3TraceList.from_file("examples/example_data.mseed", record_list=True)
            >>> len(traces)
            3
            >>> # Before unpacking, the data samples are not available
            >>> for traceid in traces:
            ...     for segment in traceid:
            ...         assert(segment.datasamples == memoryview(b''))
            ...         assert(segment.numsamples == 0)

            >>> # After unpacking, the data samples are available
            >>> for traceid in traces:
            ...     for segment in traceid:
            ...         count = segment.unpack_recordlist()
            ...         assert(segment.numsamples == segment.samplecnt)
            ...         assert(len(segment.datasamples) == segment.numsamples)

            Advanced example of unpacking data to a numpy array:
            Note: this example is for illustration only.  If numpy arrays are desired
            use the provided create_numpy_array_from_recordlist() or np_datasamples() instead.

            >>> # For doctest conditional skipping, unneeded for real code
            >>> try:
            ...     import numpy as np
            ...     HAS_NUMPY = True
            ... except ImportError:
            ...     HAS_NUMPY = False

            >>> if HAS_NUMPY:
            ...     traces = MS3TraceList.from_file("examples/example_data.mseed", record_list=True)
            ...     for traceid in traces:
            ...         for segment in traceid:
            ...             # Get the sample size and type from the first record in the record list
            ...             (size, sample_type) = segment.sample_size_type
            ...
            ...             if sample_type == "i":
            ...                 # Create a numpy array to hold the unpacked data
            ...                 numpy_array = np.zeros(segment.samplecnt, dtype=np.int32)
            ...
            ...                 # Unpack the data directly into our array
            ...                 count = segment.unpack_recordlist(numpy_array)
            ...
            ...                 # Check that the array is not all zeros
            ...                 assert not np.all(numpy_array == 0), "Numpy array is all zeros (should not happen)"
            ...
            ...             # Other sample types would need different numpy array types

            # Advanced example of unpacking data to an Apache Arrow array using pyarrow
            >>> # For doctest conditional skipping, unneeded for real code
            >>> try:
            ...     import pyarrow as pa
            ...     HAS_PYARROW = True
            ... except ImportError:
            ...     HAS_PYARROW = False

            >>> if HAS_PYARROW:
            ...     traces = MS3TraceList.from_file("examples/example_data.mseed", record_list=True)
            ...     for traceid in traces:
            ...         for segment in traceid:
            ...             # Get the sample size and type from the first record in the record list
            ...             (size, sample_type) = segment.sample_size_type
            ...
            ...             if sample_type == "i":
            ...                 # Create a pyarrow array to hold the unpacked data
            ...                 pyarrow_array = pa.array([0] * segment.samplecnt, type=pa.int32())
            ...
            ...                 # Get the data buffer for direct writing
            ...                 array_bitmap, array_buffer = pyarrow_array.buffers()
            ...
            ...                 # Unpack the data directly into our array buffer
            ...                 count = segment.unpack_recordlist(array_buffer)
            ...
            ...                 # Check that the array has no nulls (all values are valid)
            ...                 assert pyarrow_array.null_count == 0, "Pyarrow array has nulls (should not happen)"
            ...
            ...             # Other sample types would need different pyarrow array types

            The point of the advanced examples is to illustrate how to use the pattern
            to unpack data into a buffer provided by the caller in order to avoid
            copying the data.
        """
        if self.recordlist is None:
            raise ValueError("No record list available to unpack")

        if self.datasamples and buffer is not None:
            raise ValueError("Data samples already unpacked")

        # Handle buffer types that may not be compatible with memoryview
        buffer_ptr = ffi.NULL
        buffer_size = 0
        if buffer is not None:
            try:
                buffer_ptr = ffi.from_buffer(buffer)
                buffer_size = buffer.nbytes
            except (TypeError, AttributeError):
                # Try to get size through len() if nbytes is not available
                try:
                    buffer_ptr = ffi.from_buffer(buffer)
                    buffer_size = (
                        len(buffer) * buffer.itemsize
                        if hasattr(buffer, "itemsize")
                        else len(buffer)
                    )
                except (TypeError, AttributeError):
                    raise ValueError("Buffer must support the buffer protocol") from None

        status = clibmseed.mstl3_unpack_recordlist(
            self._parent_id,
            self._seg,
            buffer_ptr,
            buffer_size,
            verbose,
        )

        if status < 0:
            raise MiniSEEDError(status, "Error unpacking record list")
        else:
            return status

    def has_same_data(self, other: MS3TraceSeg) -> bool:
        """Compare trace segments for equivalent data

        Args:
            other: Another MS3TraceSeg to compare with

        Returns:
            True if segments have equivalent data, False otherwise
        """
        if not isinstance(other, MS3TraceSeg):
            return False

        return (
            self.starttime == other.starttime
            and self.endtime == other.endtime
            and self.samprate == other.samprate
            and self.samplecnt == other.samplecnt
            and self.datasamples == other.datasamples
            and self.datasize == other.datasize
            and self.numsamples == other.numsamples
            and self.sampletype == other.sampletype
        )


class MS3TraceID:
    """Wrapper around CFFI MS3TraceID structure

    This class supports list-like access to the trace segments:
    - len(traceid) returns the number of segments
    - traceid[i] returns the i-th segment
    - traceid[start:end] returns a slice of segments
    - for segment in traceid: iterates over all segments
    """

    def __init__(self, cffi_ptr: Any, parent_tracelist: Any = None) -> None:
        self._id = cffi_ptr
        self._parent_tracelist = parent_tracelist

    def __repr__(self) -> str:
        def indent_repr(thing):
            """Add two-space indentation to each line of repr(thing)"""
            return "\n".join("  " + line for line in repr(thing).split("\n"))

        # Create list of formatted strings
        if len(self) <= 5:
            formatted_lines = [indent_repr(traceid) for traceid in self]
        else:
            formatted_lines = [
                indent_repr(self[0]),
                indent_repr(self[1]),
                f"  ... {len(self) - 4} more",
                indent_repr(self[-2]),
                indent_repr(self[-1]),
            ]

        newline = "\n"
        return (
            f"MS3TraceID(sourceid: {self.sourceid}\n"
            f"         pubversion: {self.pubversion}\n"
            f"           earliest: {self.earliest_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}\n"
            f"             latest: {self.latest_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}\n"
            f"        numsegments: {len(self)}\n"
            f"{newline.join(formatted_lines)}"
            "\n)"
        )

    def __str__(self) -> str:
        return (
            f"{self.sourceid}, "
            f"v{self.pubversion}, "
            f"earliest: {self.earliest_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}, "
            f"latest: {self.latest_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}, "
            f"{len(self)} segments"
        )

    def __len__(self) -> int:
        """Return number of segments"""
        return self._id.numsegments

    def __iter__(self) -> Any:
        """Return iterator over segments"""
        current_segment = self._id.first
        while current_segment != ffi.NULL:
            yield MS3TraceSeg(current_segment, self._id, self._parent_tracelist)
            current_segment = current_segment.next

    def __getitem__(self, key: int | slice) -> Any:
        """Enable indexing and slicing access to segments"""
        if isinstance(key, slice):
            # Handle slice objects (e.g., traceid[1:3], traceid[::2])
            segment_list = list(self)
            return segment_list[key]
        elif isinstance(key, int):
            # Handle single integer index
            length = len(self)
            if length == 0:
                raise IndexError("list index out of range")

            # Handle negative indices
            if key < 0:
                key += length

            # Check bounds
            if key < 0 or key >= length:
                raise IndexError("list index out of range")

            # Find and return the segment at the specified index
            for i, segment in enumerate(self):
                if i == key:
                    return segment

            # This shouldn't happen if our logic is correct
            raise IndexError("list index out of range")
        else:
            raise TypeError("indices must be integers or slices")

    @property
    def sourceid(self) -> Optional[str]:
        """Return source ID as string"""
        result = cdata_to_string(self._id.sid)
        if result is None:
            return None
        return result

    @property
    def pubversion(self) -> int:
        """Return publication version"""
        return self._id.pubversion

    @property
    def earliest(self) -> int:
        """Return earliest time as nanoseconds since Unix/POSIX epoch"""
        return self._id.earliest

    @property
    def earliest_seconds(self) -> float:
        """Return earliest time as seconds since Unix/POSIX epoch"""
        return self._id.earliest / clibmseed.NSTMODULUS

    def earliest_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return earliest time as formatted string"""
        result = nstime2timestr(self._id.earliest, timeformat, subsecond)
        if result is None:
            return None
        return result

    @property
    def latest(self) -> int:
        """Return latest time as nanoseconds since Unix/POSIX epoch"""
        return self._id.latest

    @property
    def latest_seconds(self) -> float:
        """Return latest time as seconds since Unix/POSIX epoch"""
        return self._id.latest / clibmseed.NSTMODULUS

    def latest_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return latest time as formatted string"""
        result = nstime2timestr(self._id.latest, timeformat, subsecond)
        if result is None:
            return None
        return result


class MS3TraceList:
    """A container for a list of traces read from miniSEED

    If `file_name` is specified miniSEED will be read from the file.

    If `unpack_data` is True, the data samples will be decoded.

    If `skip_not_data` is True, bytes from the input stream will be skipped
    until a record is found.

    If `validate_crc` is True, the CRC will be validated if contained in
    the record (legacy miniSEED v2 contains no CRCs).  The CRC provides an
    internal integrity check of the record contents.

    The overall structure of the trace list list of trace IDs, each of which
    contains a list of trace segments illustrated as follows:
    - TraceList
      - TraceID
        - Trace Segment
        - Trace Segment
        - Trace Segment
        - ...
      - TraceID
        - Trace Segment
        - Trace Segment
        - ...
      - ...

    TraceIDs can be accessed via indexing and slicing:
    - `traces[0]` returns the first TraceID
    - `traces[1:3]` returns a slice of the TraceIDs
    - `for traceid in traces:` iterates over all TraceIDs

    Trace Segments can be accessed via indexing and slicing:
    - `traceid[0]` returns the first Trace Segment
    - `traceid[1:3]` returns a slice of the Trace Segments
    - `for segment in traceid:` iterates over all Trace Segments

    Example usage iterating over the trace list:
    ```
    >>> from pymseed import MS3TraceList

    >>> for traceid in MS3TraceList.from_file("examples/example_data.mseed"):
    ...     print(f"{traceid.sourceid}, {traceid.pubversion}")
    ...     for segment in traceid:
    ...         print(
    ...             f"  {segment.starttime_str()} - {segment.endtime_str()}, "
    ...             f"{segment.samprate} sps, {segment.samplecnt} samples"
    ...         )
    FDSN:IU_COLA_00_L_H_1, 4
      2010-02-27T06:50:00.069539Z - 2010-02-27T07:59:59.069538Z, 1.0 sps, 4200 samples
    FDSN:IU_COLA_00_L_H_2, 4
      2010-02-27T06:50:00.069539Z - 2010-02-27T07:59:59.069538Z, 1.0 sps, 4200 samples
    FDSN:IU_COLA_00_L_H_Z, 4
      2010-02-27T06:50:00.069539Z - 2010-02-27T07:59:59.069538Z, 1.0 sps, 4200 samples

    ```
    """

    def __init__(
        self,
        file_name=None,
        buffer=None,
        unpack_data=False,
        record_list=False,
        skip_not_data=False,
        validate_crc=True,
        split_version=False,
        verbose=0,
    ) -> None:
        # Initialize trace list - mstl3_init() returns an initialized pointer
        self._mstl = clibmseed.mstl3_init(ffi.NULL)

        if self._mstl == ffi.NULL:
            raise MiniSEEDError(clibmseed.MS_GENERROR, "Error initializing trace list")

        # Store filenames for record list functionality in C-compatible buffers
        self._c_file_names = []

        # Read specified file
        if file_name is not None:
            self.add_file(
                file_name,
                unpack_data,
                record_list,
                skip_not_data,
                validate_crc,
                split_version,
                verbose,
            )

        if buffer is not None:
            self.add_buffer(
                buffer,
                unpack_data,
                record_list,
                skip_not_data,
                validate_crc,
                split_version,
                verbose,
            )

    def __del__(self):
        """Destructor to ensure proper cleanup"""
        if self._mstl:
            mstl_ptr = ffi.new("MS3TraceList **")
            mstl_ptr[0] = self._mstl
            clibmseed.mstl3_free(mstl_ptr, 1)

    def __repr__(self) -> str:
        def indent_repr(thing):
            """Add two-space indentation to each line of repr(thing)"""
            return "\n".join("  " + line for line in repr(thing).split("\n"))

        # Create list of formatted strings
        if len(self) <= 5:
            formatted_lines = [indent_repr(traceid) for traceid in self]
        else:
            formatted_lines = [
                indent_repr(self[0]),
                indent_repr(self[1]),
                f"  ... {len(self) - 4} more",
                indent_repr(self[-2]),
                indent_repr(self[-1]),
            ]

        newline = "\n"
        return f"MS3TraceList(numtraceids: {len(self)}\n{newline.join(formatted_lines)}\n)"

    def __str__(self) -> str:
        def indent_str(thing):
            """Add two-space indentation to each line of str(thing)"""
            return "\n".join("  " + line for line in str(thing).split("\n"))

        # Create list of formatted strings
        if len(self) <= 5:
            formatted_lines = [indent_str(traceid) for traceid in self]
        else:
            formatted_lines = [
                indent_str(self[0]),
                indent_str(self[1]),
                f"  ... {len(self) - 4} more",
                indent_str(self[-2]),
                indent_str(self[-1]),
            ]

        newline = "\n"
        return f"Trace list with {len(self)} trace IDs\n{newline.join(formatted_lines)}\n"

    def __len__(self) -> int:
        """Return number of trace IDs in the list"""
        if self._mstl == ffi.NULL:
            return 0
        return int(self._mstl.numtraceids)

    def __iter__(self) -> Any:
        """Return iterator over trace IDs"""
        current_traceid = self._mstl.traces.next[0]
        while current_traceid != ffi.NULL:
            yield MS3TraceID(current_traceid, self)
            current_traceid = current_traceid.next[0]

    def __getitem__(self, key: int | slice) -> Any:
        """Enable indexing and slicing access to trace IDs"""
        if isinstance(key, slice):
            # Handle slice objects (e.g., traces[1:3], traces[::2])
            trace_list = list(self)
            return trace_list[key]
        elif isinstance(key, int):
            # Handle single integer index
            length = len(self)
            if length == 0:
                raise IndexError("list index out of range")

            # Handle negative indices
            if key < 0:
                key += length

            # Check bounds
            if key < 0 or key >= length:
                raise IndexError("list index out of range")

            # Find and return the trace ID at the specified index
            for i, traceid in enumerate(self):
                if i == key:
                    return traceid

            # This shouldn't happen if our logic is correct
            raise IndexError("list index out of range")
        else:
            raise TypeError("indices must be integers or slices")

    @property
    def numtraceids(self) -> int:
        """Return number of trace IDs in the list"""
        return len(self)

    def get_traceid(self, sourceid: str, version: int = 0) -> Optional[MS3TraceID]:
        """Get a specific trace ID from the list"""
        c_sourceid = ffi.new("char[]", sourceid.encode("utf-8"))

        traceid_ptr = clibmseed.mstl3_findID(self._mstl, c_sourceid, version, ffi.NULL)

        if traceid_ptr == ffi.NULL:
            return None

        return MS3TraceID(traceid_ptr, self)

    def sourceids(self) -> Any:
        """Return source IDs via a generator iterator"""
        for traceid in self:
            yield traceid.sourceid

    def print(
        self,
        details: int = 0,
        gaps: bool = False,
        versions: bool = False,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
    ) -> None:
        """Print trace list details"""
        clibmseed.mstl3_printtracelist(self._mstl, timeformat, details, gaps, versions)

    def add_file(
        self,
        file_name: str,
        unpack_data: bool = False,
        record_list: bool = False,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        split_version: bool = False,
        verbose: int = 0,
    ) -> None:
        """Read miniSEED data from file and add to existing trace list

        This method reads miniSEED records from a file and adds the data they contain
        to the current trace list. Data are organized by source ID and time, with
        overlapping or adjacent data automatically merged into continuous segments.

        Args:
            file_name: Path to the miniSEED file to read
            unpack_data: If True, decode data samples immediately. If False, data
                samples remain packed and must be unpacked later with
                `unpack_recordlist()`. Default: False
            record_list: If True, maintain a list of original records for each
                trace segment. Required for `unpack_recordlist()` and allows
                access to individual record metadata. Default: False
                NOTE: the files must remain accessible to unpack data with a record list
            skip_not_data: If True, skip non-data records in the file instead
                of raising an error. Useful for files with mixed content. Default: False
            validate_crc: If True, validate CRC checksums if present in records
                (miniSEED v3 only). Provides integrity verification. Default: True
            split_version: If True, treat different publication versions as
                separate trace IDs. Default: False (merge by source ID only)
            verbose: Verbosity level for diagnostic output (0=quiet, 1-3=increasing
                detail). Default: 0

        Raises:
            MiniSEEDError: If file cannot be read or contains invalid data

        Note:
            This method adds data to the existing trace list. It does not replace
            existing data. To start fresh, create a new MS3TraceList instance.

        Examples:
            Basic usage examples:

            >>> from pymseed import MS3TraceList
            >>> traces = MS3TraceList()
            >>> traces.add_file("examples/example_data.mseed")
            >>> len(traces)
            3

            Unpacking data while reading for immediate access

            >>> traces = MS3TraceList()
            >>> traces.add_file("examples/example_data.mseed", unpack_data=True)
            >>> traces[0].sourceid
            'FDSN:IU_COLA_00_L_H_1'
            >>> segment1 = traces[0][0]
            >>> segment1.starttime_str()
            '2010-02-27T06:50:00.069539Z'
            >>> segment1.endtime_str()
            '2010-02-27T07:59:59.069538Z'
            >>> segment1.samprate
            1.0
            >>> segment1.samplecnt
            4200
            >>> segment1.numsamples
            4200
            >>> segment1.sampletype
            'i'

            Read with record list for later unpacking:

            >>> traces = MS3TraceList()
            >>> traces.add_file("examples/example_data.mseed", record_list=True)
            >>> total_samples = 0
            >>> for traceid in traces:
            ...     for segment in traceid:
            ...         # Unpack when desired, can unpacked to designated buffer
            ...         samples_count = segment.unpack_recordlist()
            ...         total_samples += samples_count
            >>> total_samples
            12600

            Add multiple files to same trace list, in this trivial example the
            same file is read twice, so the data are duplicated in the trace list:

            >>> traces = MS3TraceList()
            >>> traces.add_file("examples/example_data.mseed")
            >>> traces.add_file("examples/example_data.mseed")  # Appends to existing data
            >>> len(traces)
            3
            >>> traceid = traces[0]
            >>> traceid[0].samplecnt
            4200
            >>> len(traceid) # Two segments (duplicate in this case)
            2
            >>> # Compare using explicit data comparison (fast)
            >>> traceid[0].has_same_data(traceid[1])
            True

        """

        # Store files names for reference and use in record lists
        self._c_file_names.append(ffi.new("char[]", file_name.encode("utf-8")))
        c_file_name = self._c_file_names[-1]

        # Request storing time of update in the trace list segment
        # This stores the update time as an nstime_t in the segment's private pointer (seg.prvtptr)
        flags = clibmseed.MSF_PPUPDATETIME

        if unpack_data:
            flags |= clibmseed.MSF_UNPACKDATA
        if record_list:
            flags |= clibmseed.MSF_RECORDLIST
        if skip_not_data:
            flags |= clibmseed.MSF_SKIPNOTDATA
        if validate_crc:
            flags |= clibmseed.MSF_VALIDATECRC

        # Create a reference to the current trace list pointer
        mstl_ptr = ffi.new("MS3TraceList **")
        mstl_ptr[0] = self._mstl

        status = clibmseed.ms3_readtracelist_selection(
            mstl_ptr,
            c_file_name,
            ffi.NULL,  # tolerance
            ffi.NULL,  # selections
            int(split_version),
            flags,
            verbose,
        )

        if status != clibmseed.MS_NOERROR:
            raise MiniSEEDError(status, f"Error reading file: {file_name}")

    def add_buffer(
        self,
        buffer: bytes,
        unpack_data: bool = False,
        record_list: bool = False,
        skip_not_data: bool = False,
        validate_crc: bool = True,
        split_version: bool = False,
        verbose: int = 0,
    ) -> None:
        """Read miniSEED data from a buffer and add to existing trace list

        This method reads miniSEED records from a bytes-like object and adds the
        data they contain to the current trace list. Data are organized by
        source ID and time, with overlapping or adjacent data automatically
        merged into continuous segments.

        Args:
            buffer: Bytes-like object containing miniSEED data
            unpack_data: If True, decode data samples immediately. If False, data
                samples remain packed and must be unpacked later with
                `unpack_recordlist()`. Default: False
            record_list: If True, maintain a list of original records for each
                trace segment. Required for `unpack_recordlist()` and allows
                access to individual record metadata. Default: False
                NOTE: the buffer must remain accessible to unpack data with a record list
                This can be quite tricky to achieve, for advanced use only
            skip_not_data: If True, skip non-data records in the buffer instead
                of raising an error. Useful for files with mixed content. Default: False
            validate_crc: If True, validate CRC checksums if present in records
                (miniSEED v3 only). Provides integrity verification. Default: True
            split_version: If True, treat different publication versions as
                separate trace IDs. Default: False (merge by source ID only)
            verbose: Verbosity level for diagnostic output (0=quiet, 1-3=increasing
                detail). Default: 0

        Raises:
            MiniSEEDError: If buffer cannot be read or contains invalid data

        Note:
            This method adds data to the existing trace list. It does not replace
            existing data. To start fresh, create a new MS3TraceList instance.
            To add data from a file, use `add_file()`.

        Examples:
            Read miniSEED data from a file into a buffer:

            >>> with open("examples/example_data.mseed", "rb") as f:
            ...     buffer = f.read()

            Basic usage examples:

            >>> from pymseed import MS3TraceList
            >>> traces = MS3TraceList()
            >>> traces.add_buffer(buffer)
            >>> len(traces)
            3

            Unpacking data while reading for immediate access

            >>> traces = MS3TraceList()
            >>> traces.add_buffer(buffer, unpack_data=True)
            >>> traces[0].sourceid
            'FDSN:IU_COLA_00_L_H_1'
            >>> segment1 = traces[0][0]
            >>> segment1.starttime_str()
            '2010-02-27T06:50:00.069539Z'
            >>> segment1.endtime_str()
            '2010-02-27T07:59:59.069538Z'
            >>> segment1.samprate
            1.0
            >>> segment1.samplecnt
            4200
            >>> segment1.numsamples
            4200
            >>> segment1.sampletype
            'i'

            Read with record list for later unpacking:

            >>> traces = MS3TraceList()
            >>> traces.add_buffer(buffer, record_list=True)
            >>> total_samples = 0
            >>> for traceid in traces:
            ...     for segment in traceid:
            ...         # Unpack when desired, can unpacked to designated buffer
            ...         samples_count = segment.unpack_recordlist()
            ...         total_samples += samples_count
            >>> total_samples
            12600

            Add multiple buffers to same trace list, in this trivial example the
            same file is read twice, so the data are duplicated in the trace list:

            >>> traces = MS3TraceList()
            >>> traces.add_buffer(buffer)
            >>> traces.add_buffer(buffer)  # Appends to existing data
            >>> len(traces)
            3
            >>> traceid = traces[0]
            >>> traceid[0].samplecnt
            4200
            >>> len(traceid) # Two segments (duplicate in this case)
            2

        """

        # Request storing time of update in the trace list segment
        # This stores the update time as an nstime_t in the segment's private pointer (seg.prvtptr)
        flags = clibmseed.MSF_PPUPDATETIME

        if unpack_data:
            flags |= clibmseed.MSF_UNPACKDATA
        if record_list:
            flags |= clibmseed.MSF_RECORDLIST
        if skip_not_data:
            flags |= clibmseed.MSF_SKIPNOTDATA
        if validate_crc:
            flags |= clibmseed.MSF_VALIDATECRC

        # Create a reference to the current trace list pointer
        mstl_ptr = ffi.new("MS3TraceList **")
        mstl_ptr[0] = self._mstl

        # Validate that the buffer supports the buffer protocol
        try:
            buffer_ptr = ffi.from_buffer(buffer)
            buffer_length = len(buffer)
        except (TypeError, AttributeError):
            raise ValueError("Buffer must support the buffer protocol") from None

        status = clibmseed.mstl3_readbuffer_selection(
            mstl_ptr,
            buffer_ptr,
            buffer_length,
            int(split_version),
            flags,
            ffi.NULL,  # tolerance
            ffi.NULL,  # selections
            verbose,
        )

        if status < 0:
            raise MiniSEEDError(status, f"Error reading buffer (status: {status})")

    def add_data(
        self,
        sourceid: str,
        data_samples: Sequence[Any],
        sample_type: str,
        sample_rate: float,
        start_time_str: Optional[str] = None,
        start_time: Optional[int] = None,
        start_time_seconds: Optional[float] = None,
        publication_version: int = 0,
    ) -> None:
        """Add data samples to the trace list

        A segment of regularly sampled data values for the given source ID of
        the specific type and sample rate are added to the trace list.

        Args:
            sourceid: Source identifier for the trace (e.g., "FDSN:XX_STA__BHZ").
                Should follow FDSN Source Identifier format.
            data_samples: Sequence of data samples. Can be a Python list, numpy array,
                or any buffer-like object. Data type must match `sample_type`.
            sample_type: Data sample type code:
                - "i": 32-bit signed integers (int32)
                - "f": 32-bit floating point (float32)
                - "d": 64-bit floating point (float64)
                - "t": Text/character data (single bytes)
            sample_rate: Sample rate in samples per second (Hz) or period (seconds).
                Use positive values for samples/second, and negative values for sample period in seconds.
            start_time_str: Start time as formatted string (e.g., "2023-01-01T12:00:00.000Z").
                Mutually exclusive with start_time and start_time_seconds.
            start_time: Start time as nanoseconds since Unix epoch.
                Mutually exclusive with start_time_str and start_time_seconds.
            start_time_seconds: Start time as seconds since Unix epoch (float).
                Mutually exclusive with start_time_str and start_time.
            publication_version: Publication version number for the trace. Default: 0

        Raises:
            ValueError: If sample_type is invalid, time parameters are conflicting,
                or data_samples format is incompatible with sample_type
            MiniSEEDError: If the data cannot be added to the trace list

        Note:
            Data is automatically merged with existing segments based on source ID,
            time continuity, and sample rate compatibility. Adjacent or overlapping
            segments are combined when possible.

        Performance:
            The method attempts zero-copy optimization when data_samples is a compatible
            buffer (correct type and format). Otherwise, data is converted with a copy.
            Use arrays (or any buffer with memoryviews) with matching types for best performance.

        Examples:
            Basic usage with integer data:

            >>> from pymseed import MS3TraceList
            >>> traces = MS3TraceList()
            >>> data_series = [100, 105, 98, 102, 99, 103, 97]
            >>> traces.add_data(
            ...     sourceid="FDSN:XX_STA__BHZ",
            ...     data_samples=data_series,
            ...     sample_type="i",
            ...     sample_rate=20.0,
            ...     start_time_str="2023-01-01T00:00:00.000Z"
            ... )
            >>> len(traces)
            1
            >>> traces[0].sourceid
            'FDSN:XX_STA__BHZ'

            Multiple segments that get merged:

            >>> traces = MS3TraceList()
            >>> traces.add_data("FDSN:XX_STA__BH1", [1, 2, 3], "i", 10.0,
            ...                 start_time_str="2023-01-01T00:00:00.000Z")
            >>> traces.add_data("FDSN:XX_STA__BH1", [4, 5, 6], "i", 10.0,
            ...                 start_time_str="2023-01-01T00:00:00.300Z")
            >>> len(traces) # One traceID
            1
            >>> len(traces[0]) # One trace segment
            1
        """

        # Create an MS3Record to hold the data
        msr = MS3Record()
        msr.sourceid = sourceid
        msr.samprate = sample_rate
        msr.pubversion = publication_version

        # Set start time
        if start_time_str is not None:
            msr.set_starttime_str(start_time_str)
        elif start_time is not None:
            msr.starttime = start_time
        elif start_time_seconds is not None:
            msr.starttime_seconds = start_time_seconds
        else:
            raise ValueError(
                "Must specify one of start_time_str, start_time, or start_time_seconds"
            )

        # Request storing time of update in the trace list segment
        # This stores the update time as an nstime_t in the segment's private pointer (seg.prvtptr)
        flags = clibmseed.MSF_PPUPDATETIME

        # Set data samples array, type, and counts temporarily for potential zero-copy operations
        with msr.with_datasamples(data_samples, sample_type):
            # Add the MS3Record to the trace list, setting auto-heal flag to 1 (true)
            segptr = clibmseed.mstl3_addmsr_recordptr(
                self._mstl, msr._msr, ffi.NULL, 0, 1, flags, ffi.NULL
            )

        if segptr == ffi.NULL:
            raise MiniSEEDError(clibmseed.MS_GENERROR, "Error adding data samples")

    def _record_handler_wrapper(self, record: Any, record_length: int, handlerdata: Any) -> None:
        """Callback function for mstl3_pack()"""
        # Convert CFFI buffer to bytes for the handler
        record_bytes = ffi.buffer(record, record_length)[:]
        self._record_handler(record_bytes, self._record_handler_data)

    def pack(
        self,
        handler: Callable[[bytes, Any], None],
        handlerdata: Any = None,
        flush_data: bool = True,
        flush_idle_seconds: int = 0,
        record_length: int = 4096,
        encoding: DataEncoding = DataEncoding.STEIM1,
        format_version: Optional[int] = None,
        extra_headers: Optional[str] = None,
        verbose: int = 0,
    ) -> tuple[int, int]:
        """Pack trace list data into miniSEED records and call handler function for each record.

        This method packages the time series data from all traces in the trace list
        into miniSEED format records. For each generated record, the provided handler
        function is called with the record as a bytes object.

        Args:
            handler: Callback function that will be called for each packed record.
                Must accept two arguments: (record_bytes: bytes, userdata: Any).
                The record_bytes contains the complete miniSEED record.
            handlerdata: Optional user data passed to the handler function as the second argument.
                Can be any Python object (file handle, list, etc.).
            flush_data: If True, forces packing of all available data, even if it doesn't
                fill a complete record. If False, partial records at the end of traces
                may be held in internal buffers. Default is True.
            flush_idle_seconds: If > 0, forces flushing of data segments that have not been
                updated within the specified number of seconds. Default is 0 (disabled).
            record_length: Length of each miniSEED record in bytes. Must be a power of 2
                between 128 and 65536. Common values are 512, 4096, and 8192. Default is 4096.
            encoding: Data encoding format for compression. Options include:
                - DataEncoding.STEIM1: Steim-1 compression (default, good general purpose for 32-bit ints)
                - DataEncoding.STEIM2: Steim-2 compression
                - DataEncoding.INT16: 16-bit integers (no compression)
                - DataEncoding.INT32: 32-bit integers (no compression)
                - DataEncoding.FLOAT32: 32-bit IEEE floats
                - DataEncoding.FLOAT64: 64-bit IEEE doubles
                - DataEncoding.TEXT: Text encoding (UTF-8)
            format_version: miniSEED format version (2 or 3). If None, uses library default.
                Version 2 is legacy format, version 3 is latest standard.
            extra_headers: Optional extra header fields to include.
                Must be valid JSON string.
            verbose: Verbosity level for libmseed output (0=quiet, 1=info, 2=detailed).

        Returns:
            tuple[int, int]: A tuple containing:
                - packed_samples: Total number of data samples that were packed
                - packed_records: Total number of miniSEED records generated

        Raises:
            ValueError: If format_version is not 2 or 3, or if record_length is invalid.
            MiniSEEDError: If the underlying libmseed library encounters an error during packing.

        Examples:
            Simple example writing to a file:

            >>> # Create a trace list with some data
            >>> traces = MS3TraceList()
            >>> traces.add_data("FDSN:XX_STA__BHZ", [1, 2, 3, 4, 5], "i", 100.0, start_time_str="2023-01-01T00:00:00.000Z")

            >>> # Pack to file using a simple handler
            >>> def write_to_file(record_bytes, file_handle):
            ...     file_handle.write(record_bytes)

            >>> with open("output.mseed", "wb") as f: # doctest: +SKIP
            ...     packed_samples, packed_records = traces.pack(write_to_file, f)
            ...     print(f"Packed {packed_samples} samples into {packed_records} records")
            Packed 5 samples into 1 records

        Note:
            - The handler function is called once for each complete record generated
            - For large datasets, consider using streaming approaches with multiple pack() calls

        See also:
            - to_file()
        """

        # Set handler function as CFFI callback function
        self._record_handler = handler
        self._record_handler_data = handlerdata

        # Create callback function type and instance
        RECORD_HANDLER = ffi.callback("void(char *, int, void *)", self._record_handler_wrapper)

        pack_flags = 0
        if flush_data:
            pack_flags |= clibmseed.MSF_FLUSHDATA

        if format_version is not None:
            if format_version not in [2, 3]:
                raise ValueError(f"Invalid miniSEED format version: {format_version}")
            if format_version == 2:
                pack_flags |= clibmseed.MSF_PACKVER2

        packed_samples = ffi.new("int64_t *")

        c_extra = ffi.new("char[]", extra_headers.encode("utf-8")) if extra_headers else ffi.NULL

        packed_records = clibmseed.mstl3_pack_ppupdate_flushidle(
            self._mstl,
            RECORD_HANDLER,
            ffi.NULL,
            record_length,
            encoding,
            packed_samples,
            pack_flags,
            verbose,
            c_extra,
            flush_idle_seconds,
        )

        if packed_records < 0:
            raise MiniSEEDError(packed_records, "Error packing miniSEED record(s)")

        return (packed_samples[0], packed_records)

    def to_file(
        self,
        filename: str,
        overwrite: bool = False,
        max_reclen: int = 4096,
        encoding: DataEncoding = DataEncoding.STEIM1,
        format_version: Optional[int] = None,
        verbose: int = 0,
    ) -> int:
        """Write trace list data to a miniSEED file.

        This method packages the time series data from all traces in the trace list
        into miniSEED format and writes them directly to a file. This is a convenience
        method that combines packing and file writing in a single operation.

        Args:
            filename: Path to the output miniSEED file. The file will be created if it
                doesn't exist. Directory must already exist.
            overwrite: If True, overwrites any existing file. If False and file exists,
                append data to the end of the file. Default is False for safety.
            max_reclen: Maximum record length in bytes. Must be a power of 2 between
                128 and 65536. Common values are 512, 4096, and 8192. Default is 4096.
            encoding: Data encoding format for compression. Options include:
                - DataEncoding.STEIM1: Steim-1 compression (default, good general purpose for 32-bit ints)
                - DataEncoding.STEIM2: Steim-2 compression
                - DataEncoding.INT16: 16-bit integers (no compression)
                - DataEncoding.INT32: 32-bit integers (no compression)
                - DataEncoding.FLOAT32: 32-bit IEEE floats
                - DataEncoding.FLOAT64: 64-bit IEEE doubles
                - DataEncoding.TEXT: Text encoding (UTF-8)
            format_version: miniSEED format version (2 or 3). If None, uses library default.
                Version 2 is legacy format, version 3 is latest standard.
            verbose: Verbosity level for libmseed output (0=quiet, 1=info, 2=detailed).

        Returns:
            int: Number of miniSEED records written to the file.

        Raises:
            ValueError: If format_version is not 2 or 3, or if max_reclen is invalid.
            MiniSEEDError: If the underlying libmseed library encounters an error during
                file writing (e.g., permission denied, disk full, invalid data).

        Examples:
            Simple file writing:

            >>> # Create a trace list with some data
            >>> traces = MS3TraceList()
            >>> traces.add_data("FDSN:XX_STA__BHZ", [1, 2, 3, 4, 5], "i", 100.0, start_time_str="2023-01-01T00:00:00Z")

            >>> # Write to file (basic usage)
            >>> records_written = traces.to_file("output.mseed") # doctest: +SKIP
            >>> print(f"Wrote {records_written} records to output.mseed") # doctest: +SKIP
            Wrote 1 records to output.mseed

            Writing with specific options:

            >>> # Write as miniSEED v2 with Steim-2 compression
            >>> records_written = traces.to_file( # doctest: +SKIP
            ...     "output.mseed",
            ...     overwrite=True,
            ...     format_version=2,
            ...     encoding=DataEncoding.STEIM2,
            ...     max_reclen=512
            ... )
            >>> print(f"Wrote {records_written} records to output.mseed") # doctest: +SKIP
            Wrote 1 records to output.mseed

        Note:
            - This method is more convenient than using pack() with a file handler
            - File permissions and disk space are checked during writing
            - All data is written in a single operation - use pack() for streaming
            - The output file contains all traces from the trace list
            - Record boundaries depend on max_reclen and data compression

        See also:
            - pack(): Lower-level method with custom record handlers
            - add_data(): Add time series data to the trace list
            - from_file(): Read miniSEED data from file
        """
        # Convert filename to bytes (C string)
        c_filename = ffi.new("char[]", filename.encode("utf-8"))

        pack_flags = 0
        if format_version is not None:
            if format_version not in [2, 3]:
                raise ValueError(f"Invalid miniSEED format version: {format_version}")
            if format_version == 2:
                pack_flags |= clibmseed.MSF_PACKVER2

        # Call the C function
        packed_records = clibmseed.mstl3_writemseed(
            self._mstl,
            c_filename,
            overwrite,
            max_reclen,
            encoding,
            pack_flags,
            verbose,
        )

        if packed_records < 0:
            raise MiniSEEDError(packed_records, "Error writing miniSEED file")

        return packed_records

    @classmethod
    def from_file(cls, filename, **kwargs):
        """Create MS3TraceList from a specified miniSEED file"""
        return cls(file_name=filename, **kwargs)

    @classmethod
    def from_buffer(cls, buffer: Any, **kwargs):
        """Create an MS3TraceList from miniSEED data in a memory buffer"""
        return cls(buffer=buffer, **kwargs)
