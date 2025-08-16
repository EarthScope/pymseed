"""
Core trace list implementation for pymseed

"""

from collections.abc import Sequence
from typing import Any, Optional, Callable

from .clib import clibmseed, ffi, cdata_to_string
from .definitions import DataEncoding, SubSecond, TimeFormat
from .exceptions import MiniSEEDError
from .msrecord import MS3Record
from .util import nstime2timestr, encoding_sizetype


class MS3RecordPtr:
    """Wrapper around CFFI MS3RecordPtr structure"""

    def __init__(self, cffi_ptr: Any) -> None:
        self._ptr = cffi_ptr

    def __repr__(self) -> str:
        return (
            f"Pointer to {self.msr.sourceid}, "
            f"{cdata_to_string(self._ptr.filename)}, "
            f"byte offset: {self._ptr.fileoffset}"
        )

    @property
    def msr(self) -> MS3Record:
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
    """Wrapper around CFFI MS3RecordList structure"""

    def __init__(self, cffi_ptr: Any) -> None:
        self._list = cffi_ptr

    def __repr__(self) -> str:
        return f"Record list of {self._list.recordcnt} records"

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
        return (
            f"start: {self.starttime_str()}, "
            f"end: {self.endtime_str()}, "
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
        """Unpack data samples from record list into a buffer-like object

        If a destination `buffer` is provided it must be a buffer-like object,
        and large enough to hold the data samples.

        If a destination `buffer` is not provided (None), unpacked data will be
        owned by this object instance and will be freed when the instance is
        destroyed. If you wish to keep the data, you must make a copy.

        Returns the number of samples unpacked.
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
                    raise ValueError("Buffer must support the buffer protocol")

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
        return (
            f"Source ID: {self.sourceid}, "
            f"earliest: {self.earliest_str()}, "
            f"latest: {self.latest_str()}, "
            f"segments: {len(self)}"
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
    from pymseed import MS3TraceList

    traces = MS3TraceList.from_file("input_file.mseed")
    for traceid in traces:
        print(f"{traceid.sourceid}, {traceid.pubversion}")
        for segment in traceid:
            print(
                f"  {segment.starttime_str()} - {segment.endtime_str()}, "
                f"{segment.samprate} sps, {segment.samplecnt} samples"
            )
    ```
    """

    def __init__(
        self,
        file_name=None,
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

        # Store filenames for record list functionality in C-like buffers
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

    def __del__(self):
        """Destructor to ensure proper cleanup"""
        if self._mstl:
            mstl_ptr = ffi.new("MS3TraceList **")
            mstl_ptr[0] = self._mstl
            clibmseed.mstl3_free(mstl_ptr, 1)

    def __repr__(self) -> str:
        return f"MS3TraceList with {len(self)} trace IDs"

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
        """

        # Store files names for reference and use in record lists
        self._c_file_names.append(ffi.new("char[]", file_name.encode("utf-8")))
        c_file_name = self._c_file_names[-1]

        flags = 0
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
        """Add data samples to the trace list"""

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

        # Set sample count and type
        msr._msr.samplecnt = len(data_samples)
        msr._msr.numsamples = len(data_samples)

        if sample_type == "i":
            msr._msr.sampletype = b"i"
            msr.encoding = DataEncoding.INT32
        elif sample_type == "f":
            msr._msr.sampletype = b"f"
            msr.encoding = DataEncoding.FLOAT32
        elif sample_type == "d":
            msr._msr.sampletype = b"d"
            msr.encoding = DataEncoding.FLOAT64
        elif sample_type == "t":
            msr._msr.sampletype = b"t"
            msr.encoding = DataEncoding.TEXT
        else:
            raise ValueError(f"Unknown sample type: {sample_type}")

        # Allocate and copy data samples based on type
        if sample_type == "i":
            try:
                mv = memoryview(data_samples)
                if mv.format == "i" and mv.itemsize == 4:
                    # Compatible format - safe to zero-copy
                    sample_array = ffi.cast("int32_t *", ffi.from_buffer(data_samples))
                else:
                    raise ValueError("Incompatible buffer format")
            except (TypeError, ValueError):
                # Not compatible or not a buffer - need conversion
                sample_array = ffi.new("int32_t[]", [int(sample) for sample in data_samples])
            msr._msr.datasamples = ffi.cast("void *", sample_array)
            msr._msr.datasize = len(data_samples) * 4
        elif sample_type == "f":
            try:
                mv = memoryview(data_samples)
                if mv.format == "f" and mv.itemsize == 4:
                    # Compatible format - safe to zero-copy
                    sample_array = ffi.cast("float *", ffi.from_buffer(data_samples))
                else:
                    raise ValueError("Incompatible buffer format")
            except (TypeError, ValueError):
                # Not compatible or not a buffer - need conversion
                sample_array = ffi.new("float[]", [float(sample) for sample in data_samples])
            msr._msr.datasamples = ffi.cast("void *", sample_array)
            msr._msr.datasize = len(data_samples) * 4
        elif sample_type == "d":
            try:
                mv = memoryview(data_samples)
                if mv.format == "d" and mv.itemsize == 8:
                    # Compatible format - safe to zero-copy
                    sample_array = ffi.cast("double *", ffi.from_buffer(data_samples))
                else:
                    raise ValueError("Incompatible buffer format")
            except (TypeError, ValueError):
                # Not compatible or not a buffer - need conversion
                sample_array = ffi.new("double[]", [float(sample) for sample in data_samples])
            msr._msr.datasamples = ffi.cast("void *", sample_array)
            msr._msr.datasize = len(data_samples) * 8
        elif sample_type == "t":
            try:
                mv = memoryview(data_samples)
                if mv.format in ("c", "b", "B") and mv.itemsize == 1:
                    # Compatible format - safe to zero-copy
                    sample_array = ffi.cast("char *", ffi.from_buffer(data_samples))
                else:
                    raise ValueError("Incompatible buffer format")
            except (TypeError, ValueError):
                # Not compatible or not a buffer - need conversion
                text_data = []
                for sample in data_samples:
                    if isinstance(sample, str):
                        text_data.append(sample.encode("utf-8")[0])
                    else:
                        text_data.append(
                            int(sample)
                            if isinstance(sample, (int, float))
                            else str(sample).encode("utf-8")[0]
                        )
                sample_array = ffi.new("char[]", text_data)
            msr._msr.datasamples = ffi.cast("void *", sample_array)
            msr._msr.datasize = len(data_samples)
        else:
            raise ValueError(f"Unknown sample type: {sample_type}")

        # Request storing time of update in the trace list segment (at seg.prvtptr)
        flags = clibmseed.MSF_PPUPDATETIME

        # Add the MS3Record to the trace list, setting auto-heal flag to 1 (true)
        segptr = clibmseed.mstl3_addmsr_recordptr(
            self._mstl, msr._msr, ffi.NULL, 0, 1, flags, ffi.NULL
        )

        # Disconnect data sample memory from the MS3Record
        msr._msr.datasamples = ffi.NULL

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
        record_length: int = 4096,
        encoding: DataEncoding = DataEncoding.STEIM1,
        format_version: Optional[int] = None,
        extra_headers: Optional[str] = None,
        verbose: int = 0,
    ) -> tuple[int, int]:
        """Pack data into miniSEED record(s) and call handler()

        Returns a tuple of (packed_samples, packed_records)
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

        packed_records = clibmseed.mstl3_pack(
            self._mstl,
            RECORD_HANDLER,
            ffi.NULL,
            record_length,
            encoding,
            packed_samples,
            pack_flags,
            verbose,
            c_extra,
        )

        if packed_records < 0:
            raise MiniSEEDError(packed_records, "Error packing miniSEED record(s)")

        return (packed_samples[0], packed_records)

    @classmethod
    def from_file(cls, filename, **kwargs):
        """Create MS3TraceList from file"""
        return cls(file_name=filename, **kwargs)
