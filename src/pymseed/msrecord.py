"""
Core MS3Record implementation for pymseed

"""

import json
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Callable, Optional, Union

from .clib import cdata_to_string, clibmseed, ffi
from .definitions import SubSecond, TimeFormat
from .exceptions import MiniSEEDError
from .util import encoding_string, nstime2timestr, timestr2nstime


class MS3Record:
    """A wrapper for miniSEED data records supporting formats v2 and v3.

    MS3Record provides a Python interface to individual miniSEED data records,
    which are the fundamental unit of the miniSEED time series data format.
    Each record contains metadata (timing, sample rate, encoding) and optionally,
    but commonly, data samples.

    miniSEED is a format optimized for continuous time series data. It's widely
    used in seismology, and related geophysical data for storing and exchanging
    time series data.

    Key Features:
        - Read and write miniSEED v2 and v3 formats
        - Access to all record metadata (timing, sample rates, encoding, etc.)
        - Efficient data sample access via memoryview (zero-copy) or numpy arrays
        - Support for all defined data encodings

    Common Usage Patterns:

        Reading records (from a file):
            >>> from pymseed import MS3Record
            >>> for msr in MS3Record.from_file('examples/example_data.mseed', unpack_data=True):
            ...     print(f"{msr.sourceid}: {msr.numsamples} samples")
            ...     break # only print the first record to limit testing output
            FDSN:IU_COLA_00_L_H_1: 135 samples

        Creating records:
            >>> from pymseed import MS3Record, DataEncoding
            >>> msr = MS3Record()
            >>> msr.sourceid = "FDSN:NET_STA_LOC_B_S_s"
            >>> msr.starttime_str = "2024-01-01T00:00:00Z"
            >>> msr.samprate = 100.0
            >>> msr.encoding = DataEncoding.STEIM2
            >>> msr.reclen = 512

            >>> for record in msr.generate(data_samples=[1,2,3,4], sample_type='i'):
            ...     print(f"Packed {len(record)} byte record")
            Packed 126 byte record

        Working with data samples:
            # Get data as memoryview (no copy)
            data_mv = msr.datasamples
            # Get data as numpy array (requires numpy, no copy)
            data_np = msr.np_datasamples
            # Get data as Python list (copy)
            data_list = msr.datasamples[:]

    Attributes:
        All miniSEED record fields are accessible as properties with both
        getters and setters where appropriate. Key properties include:
        - sourceid: FDSN Source Identifier (e.g., "FDSN:IU_COLA_00_B_H_Z")
        - starttime: Start time in nanoseconds since Unix epoch
        - samprate: Sample rate in Hz (positive) or interval in seconds (negative)
        - numsamples: Number of decoded data samples
        - datasamples: Access to the actual data samples
        - encoding: Data encoding format (Steim1/2, Float, etc.)

    See Also:
        MSTraceList: For working with collections of records as traces
    """

    def __init__(
        self,
        reclen: Optional[int] = None,
        encoding: Optional[int] = None,
        recordptr: Any = None,
    ) -> None:
        """
        Initialize MS3Record wrapper.

        Creates a new miniSEED record or wraps an existing one. When creating
        a new record, the structure is initialized with default values that
        can be overridden by the optional parameters.

        Args:
            reclen: Maximum record length in bytes. Common values are 512, 4096
                   (miniSEED v2 default) bytes. If None, uses library default.
            encoding: Data encoding format code. Common values:
                    DataEncoding.TEXT, DataEncoding.STEIM1, DataEncoding.STEIM2,
                    DataEncoding.FLOAT32, DataEncoding.FLOAT64. If None, uses library default.
            recordptr: Internal C structure pointer. Only used when wrapping
                      existing parsed records. Should not be set by users.

        Note:
            Most users should create records with MS3Record() and set properties
            like sourceid, starttime, and samprate before packing data.

        Example:
            >>> # Create empty record
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.sourceid = "FDSN:IU_COLA_00_B_H_Z"
            >>> msr.samprate = 20.0
            >>>
            >>> # Create with specific encoding for Steim2 compression
            >>> msr = MS3Record(encoding=11, reclen=4096)

        """
        if recordptr is not None:
            # Wrap an existing record structure
            self._msr = recordptr
            self._msr_allocated = False
        else:
            # Allocate a new record
            self._msr = clibmseed.msr3_init(ffi.NULL)
            self._msr_allocated = True

            # Set values if provided
            if reclen is not None:
                self._msr.reclen = reclen
            if encoding is not None:
                self._msr.encoding = encoding

    def __del__(self) -> None:
        if self._msr and self._msr_allocated:
            try:
                msr_ptr = ffi.new("MS3Record **")
                msr_ptr[0] = self._msr
                clibmseed.msr3_free(msr_ptr)
                self._msr = ffi.NULL
                self._msr_allocated = False
            except Exception:
                # Silently ignore errors during cleanup to avoid issues during interpreter shutdown
                pass

    def __repr__(self) -> str:
        sample_preview= "[]"
        if self._msr.numsamples > 0:
            if len(self.datasamples) > 5:
                # Create array representation with ellipsis inside: [1,2,3,4,5,...]
                first_samples = ', '.join(str(sample) for sample in list(self.datasamples[:5]))
                sample_preview = f"[{first_samples}, ...]"
            else:
                sample_preview = str(list(self.datasamples))

        return (
            f"MS3Record(sourceid: {self.sourceid}\n"
            f"        pubversion: {self._msr.pubversion}\n"
            f"            reclen: {self._msr.reclen}\n"
            f"     formatversion: {self._msr.formatversion}\n"
            f"         starttime: {self._msr.starttime} => {self.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}\n"
            f"         samplecnt: {self._msr.samplecnt}\n"
            f"          samprate: {self._msr.samprate}\n"
            f"             flags: {self._msr.flags} => {self.flags_dict()}\n"
            f"               CRC: {self._msr.crc} => {hex(self._msr.crc)}\n"
            f"          encoding: {self._msr.encoding} => {self.encoding_str()}\n"
            f"       extralength: {self._msr.extralength}\n"
            f"        datalength: {self._msr.datalength}\n"
            f"             extra: {self.extra}\n"
            f"        numsamples: {self._msr.numsamples}\n"
            f"       datasamples: {sample_preview}\n"
            f"          datasize: {self._msr.datasize}\n"
            f"        sampletype: {self.sampletype} => {self.sampletype_str()}\n"
            f"    record pointer: {self._msr})"
        )

    def __lt__(self, obj: "MS3Record") -> bool:
        return (self.sourceid, self.starttime) < (obj.sourceid, obj.starttime)

    def __gt__(self, obj: "MS3Record") -> bool:
        return (self.sourceid, self.starttime) > (obj.sourceid, obj.starttime)

    def __le__(self, obj: "MS3Record") -> bool:
        return (self.sourceid, self.starttime) <= (obj.sourceid, obj.starttime)

    def __ge__(self, obj: "MS3Record") -> bool:
        return (self.sourceid, self.starttime) >= (obj.sourceid, obj.starttime)

    def __str__(self) -> str:
        return (
            f"{self.sourceid}, "
            f"v{self.pubversion}, "
            f"{self.reclen} bytes, "
            f"{self.samplecnt} samples, "
            f"{self.samprate} Hz, "
            f"{self.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_DOY_Z)}"
        )

    @property
    def record(self) -> bytes:
        """Return raw, parsed miniSEED record as bytes (copy)"""
        if self._msr.record == ffi.NULL:
            raise ValueError("No raw record available")

        return bytes(ffi.buffer(self._msr.record, self._msr.reclen)[:])

    @property
    def record_mv(self) -> memoryview:
        """Return raw, parsed miniSEED record as memoryview (no copy)"""
        if self._msr.record == ffi.NULL:
            raise ValueError("No raw record available")

        return ffi.buffer(self._msr.record, self._msr.reclen)

    @property
    def reclen(self) -> int:
        """Return record length in bytes"""
        return self._msr.reclen

    @reclen.setter
    def reclen(self, value: int) -> None:
        """Set maximum record length in bytes"""
        self._msr.reclen = value

    @property
    def swapflag(self) -> int:
        """Return swap flags as raw integer"""
        return self._msr.swapflag

    def swapflag_dict(self) -> dict[str, bool]:
        """Return swap flags as dictionary"""
        swapflag = {}
        swapflag["header_swapped"] = bool(self._msr.swapflag & clibmseed.MSSWAP_HEADER)
        swapflag["payload_swapped"] = bool(self._msr.swapflag & clibmseed.MSSWAP_PAYLOAD)
        return swapflag

    @property
    def sourceid(self) -> Optional[str]:
        """Source identifier string identifying the data source.

        Returns:
            Source identifier string, or None if not set
        """
        return cdata_to_string(self._msr.sid)

    @sourceid.setter
    def sourceid(self, value: str) -> None:
        """Set source identifier string.

        The source identifier is limited to 63 characters and should follow
        FDSN Source Identifier conventions for interoperability.

        Examples:
            "FDSN:UW_BEER__L_H_Z"
            "FDSN:IU_COLA_00_B_H_1"

        Args:
            value: Source identifier string

        Raises:
            ValueError: If identifier exceeds 63 characters

        See Also:
            https://docs.fdsn.org/projects/source-identifiers
        """
        if len(value) >= clibmseed.LM_SIDLEN:
            raise ValueError(f"Source ID too long (max {clibmseed.LM_SIDLEN - 1} characters)")

        self._msr.sid = ffi.new(f"char[{clibmseed.LM_SIDLEN}]", value.encode("utf-8"))

    @property
    def formatversion(self) -> int:
        """Return format version"""
        return self._msr.formatversion

    @formatversion.setter
    def formatversion(self, value: int) -> None:
        """Set format version"""
        if value not in [2, 3]:
            raise ValueError(f"Invalid miniSEED format version: {value}")
        self._msr.formatversion = value

    @property
    def flags(self) -> int:
        """Return record flags as raw 8-bit integer"""
        return self._msr.flags

    @flags.setter
    def flags(self, value: int) -> None:
        """Set record flags as an 8-bit unsigned integer"""
        self._msr.flags = value

    def flags_dict(self) -> dict[str, bool]:
        """Record flags as a dictionary.

        Decodes the 8-bit flags field into named boolean indicators for
        data quality assessment.

        Returns:
            dict[str, bool]: Dictionary with flag names as keys:
                'calibration_signals_present': Calibration signals detected
                'time_tag_is_questionable': Timing accuracy uncertain
                'clock_locked': Clock was locked to reference

        Example:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> flags = msr.flags_dict()
            >>> if flags.get('time_tag_is_questionable'):
            ...     print("Warning: questionable timing")

        See Also:
            flags: Raw 8-bit flags value
        """
        flags = {}
        if self._msr.flags & 0x01:
            flags["calibration_signals_present"] = True
        if self._msr.flags & 0x02:
            flags["time_tag_is_questionable"] = True
        if self._msr.flags & 0x04:
            flags["clock_locked"] = True
        return flags

    @property
    def starttime(self) -> int:
        """Time of the first sample as nanoseconds since Unix epoch.

        Depending on the version of miniSEED, and the characteristics of the
        data source, the start time may not have nanosecond precision.  A
        precision of microseconds is common, along with .0001 second resolution.

        Returns:
            Nanoseconds since Unix epoch (1970-01-01T00:00:00Z)

        Example:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.starttime = 1609459200000000000  # 2021-01-01T00:00:00Z

        See Also:
            starttime_seconds: For working with floating-point seconds
            starttime_str(): For human-readable time strings
        """
        return self._msr.starttime

    @starttime.setter
    def starttime(self, value: int) -> None:
        """Set start time as nanoseconds since Unix/POSIX epoch"""
        self._msr.starttime = value

    @property
    def starttime_seconds(self) -> float:
        """Return start time as seconds since Unix/POSIX epoch"""
        return self._msr.starttime / clibmseed.NSTMODULUS

    @starttime_seconds.setter
    def starttime_seconds(self, value: float) -> None:
        """Set start time as seconds since Unix/POSIX epoch

        The value is limited to microsecond resolution and will be rounded to
        the nearest microsecond to ensure a consistent conversion to the
        internal representation. This is done to avoid floating point precision
        issues.  If you need nanosecond precision, use the starttime setter instead.
        """
        # Scale to microseconds, round to nearest integer, then scale to nanoseconds
        self._msr.starttime = round(value * 1000000) * 1000

    def starttime_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return start time as formatted string"""
        return nstime2timestr(self._msr.starttime, timeformat, subsecond)

    def set_starttime_str(self, value: str) -> None:
        """Set start time from formatted date-time string

        Args:
            value (str): Formatted date-time string
                A number of formats are supported, but the recommended form is
                YYYY-MM-DDTHH:MM:SS.ssssssZ (RFC 3339/ISO 8601).

        Raises:
            ValueError: If the string is not a valid date-time string

        Example:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.set_starttime_str("2021-01-01T00:00:00.123456789Z")
            >>> msr.starttime_str()
            '2021-01-01T00:00:00.123456789Z'

        See Also:
            starttime_str(): For formatting the start time as a string
        """
        self._msr.starttime = timestr2nstime(value)

    @property
    def samprate(self) -> float:
        """Nominal sample rate in samples per second (Hz)

        Examples:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.samprate = 100.0   # 100 Hz sampling
            >>> msr.samprate
            100.0
            >>> msr.samprate = -10.0   # 10 second intervals
            >>> msr.samprate
            0.1

        See Also:
            samprate_raw: Nominal sample rate in Hz or sample interval in seconds
        """
        return clibmseed.msr3_sampratehz(self._msr)

    @samprate.setter
    def samprate(self, value: float) -> None:
        """Set nominal sampling rate

        For sampling rates in samples/second (Hz), this value should be positive.
        For sampling rates in seconds/sample, this value should be negative.

        It is recommend to use the sample period notation (negative values) when
        the sampling rate is less than 1 sample/second to retain accuracy.

        Examples:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.samprate = 100.0   # 100 Hz sampling
            >>> msr.samprate = -10.0   # 10 second intervals
        """
        self._msr.samprate = value

    @property
    def samprate_raw(self) -> float:
        """Nominal sample rate in samples per second (Hz) or sample interval in seconds.

        When positive, this represents samples per second (Hz).
        When negative, this represents the sample period in seconds (-1/period).

        Returns:
            Sample rate in Hz (positive) or interval in seconds (negative)

        Examples:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.samprate = 100.0   # 100 Hz sampling
            >>> msr.samprate_raw
            100.0
            >>> msr.samprate = -10.0   # 10 second intervals
            >>> msr.samprate_raw
            -10.0

        See Also:
            samprate: Nominal sample rate in Hz
        """
        return self._msr.samprate

    @property
    def samprate_period_ns(self) -> int:
        """Nominal sample period in nanoseconds.

        Examples:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.samprate = 40.0    # 40 Hz sampling
            >>> msr.samprate_period_ns
            25000000
            >>> msr.samprate = -10.0   # 10 second intervals
            >>> msr.samprate_period_ns
            10000000000

        See Also:
            samprate_period_seconds: Nominal sample period in seconds
        """
        return clibmseed.msr3_nsperiod(self._msr)

    @property
    def samprate_period_seconds(self) -> float:
        """Nominal sample period in seconds.

        Examples:
            >>> from pymseed import MS3Record
            >>> msr = MS3Record()
            >>> msr.samprate = 40.0    # 40 Hz sampling
            >>> msr.samprate_period_seconds
            0.025
            >>> msr.samprate = -10.0   # 10 second intervals
            >>> msr.samprate_period_seconds
            10.0

        See Also:
            samprate_period_ns: Nominal sample period in nanoseconds (for accuracy)
        """
        return clibmseed.msr3_nsperiod(self._msr) / clibmseed.NSTMODULUS

    @property
    def encoding(self) -> int:
        """Data encoding format code specifying how data samples are compressed/stored.

        miniSEED supports various encoding formats optimized for different data types
        and compression requirements. Common encoding values:

        - DataEncoding.TEXT: UTF-8 text
        - DataEncoding.STEIM1: Steim1 32-bit integer compression
        - DataEncoding.STEIM2: Steim2 32-bit integer compression
        - DataEncoding.FLOAT32: IEEE Float32 (little-endian)
        - DataEncoding.FLOAT64: IEEE Float64 (little-endian)

        Returns:
            int: Encoding format code

        Examples:
            >>> from pymseed import MS3Record, DataEncoding
            >>> msr = MS3Record()
            >>> msr.encoding = DataEncoding.STEIM2
            >>> msr.encoding = DataEncoding.FLOAT32

        See Also:
            encoding_str(): Human-readable encoding description
        """
        return self._msr.encoding

    @encoding.setter
    def encoding(self, value: int) -> None:
        """Set data encoding format"""
        self._msr.encoding = value

    @property
    def pubversion(self) -> int:
        """Return publication version"""
        return self._msr.pubversion

    @pubversion.setter
    def pubversion(self, value: int) -> None:
        """Set publication version"""
        self._msr.pubversion = value

    @property
    def samplecnt(self) -> int:
        """Return number of samples specified in fixed header"""
        return self._msr.samplecnt

    @property
    def crc(self) -> int:
        """Return CRC of entire record"""
        return self._msr.crc

    @property
    def extralength(self) -> int:
        """Return length of extra headers in bytes"""
        return self._msr.extralength

    @property
    def datalength(self) -> int:
        """Return length of data payload in bytes"""
        return self._msr.datalength

    @property
    def extra(self) -> str:
        """Return extra headers as JSON string"""
        if self._msr.extra == ffi.NULL:
            return ""
        return cdata_to_string(self._msr.extra)

    @extra.setter
    def extra(self, value: str) -> None:
        """Set extra headers as JSON string, will be minified to reduce size"""
        if value:
            # Minify the JSON string to ensure valid JSON and minimize size
            minified = json.dumps(json.loads(value), separators=(",", ":"))

            c_value = ffi.new("char[]", minified.encode("utf-8"))
            status = clibmseed.mseh_replace(self._msr, c_value)
            if status < 0:
                raise ValueError(f"Error setting extra headers: {status}")

    @property
    def datasamples(self) -> memoryview:
        """Data samples as a memoryview (zero-copy access).

        Returns a memoryview of the decoded data samples. This provides direct
        access to the internal buffer without copying data.

        The view type depends on the data encoding:
        - Integer data (Steim1/2, int16/32): memoryview of 32-bit integers
        - Float32 data: memoryview of 32-bit floats
        - Float64 data: memoryview of 64-bit floats
        - Text data: memoryview of bytes

        Important:
            The returned view is only valid while this MS3Record exists.
            If data is needed beyond the record's lifetime, make a copy.

        Returns:
            memoryview: Direct view of sample data, indexed 0 to numsamples-1

        Examples:
            >>> from pymseed import MS3Record
            >>> reader = MS3Record.from_file("examples/example_data.mseed", unpack_data=True)
            >>> msr = reader.read()
            >>> # Direct indexing (no copy)
            >>> first_sample = msr.datasamples[0]
            >>> last_sample = msr.datasamples[-1]
            >>>
            >>> # Slicing (no copy)
            >>> first_ten = msr.datasamples[:10]
            >>>
            >>> # Copy to Python list
            >>> data_list = msr.datasamples[:]
            >>>
            >>> # Copy to new array
            >>> import array
            >>> data_array = array.array('f', msr.datasamples)

        See Also:
            np_datasamples: NumPy array view (requires numpy)
            numsamples: Number of available samples
            sampletype: Type indicator ('i', 'f', 'd', 't')
        """
        if self._msr.numsamples <= 0:
            return memoryview(b"")  # Empty memoryview

        sampletype = self.sampletype

        if sampletype == "i":
            ptr = ffi.cast("int32_t *", self._msr.datasamples)
            buffer = ffi.buffer(ptr, self._msr.numsamples * ffi.sizeof("int32_t"))
            return memoryview(buffer).cast("i")
        elif sampletype == "f":
            ptr = ffi.cast("float *", self._msr.datasamples)
            buffer = ffi.buffer(ptr, self._msr.numsamples * ffi.sizeof("float"))
            return memoryview(buffer).cast("f")
        elif sampletype == "d":
            ptr = ffi.cast("double *", self._msr.datasamples)
            buffer = ffi.buffer(ptr, self._msr.numsamples * ffi.sizeof("double"))
            return memoryview(buffer).cast("d")
        elif sampletype == "t":
            ptr = ffi.cast("char *", self._msr.datasamples)
            buffer = ffi.buffer(ptr, self._msr.numsamples)
            return memoryview(buffer).cast("B")
        else:
            raise ValueError(f"Unknown sample type: {sampletype}")

    @property
    def np_datasamples(self) -> Any:
        """Data samples as a NumPy array view (zero-copy access).

        Returns a NumPy array view of the decoded data samples without copying
        the underlying data.

        The array dtype depends on the data encoding:
        - Integer data (Steim1/2, int16/32): numpy.int32
        - Float32 data: numpy.float32
        - Float64 data: numpy.float64
        - Text data: numpy dtype 'S1' (1-byte strings)

        Returns:
            numpy.ndarray: 1D array view of the sample data

        Raises:
            ImportError: If NumPy is not installed
            ValueError: If sample type is unknown or unsupported

        Important:
            Requires NumPy to be installed. The returned array is only valid
            while this MS3Record exists. For permanent storage, make a copy.

        Examples:
            >>> from pymseed import MS3Record
            >>> reader = MS3Record.from_file("examples/example_data.mseed", unpack_data=True)
            >>> msr = reader.read()

            >>> # Direct NumPy operations (no copy)
            >>> import numpy as np
            >>> data = msr.np_datasamples
            >>> mean_value = np.mean(data)
            >>> max_value = np.max(data)

            >>> # Copy for permanent storage
            >>> data_copy = msr.np_datasamples.copy()

            >>> # Mathematical operations
            >>> filtered = data * 0.5  # Creates new array

        See Also:
            datasamples: Raw memoryview access
            numsamples: Number of available samples
            sampletype: Type indicator ('i', 'f', 'd', 't')
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "numpy is not installed. Install numpy or this package with [numpy] optional dependency"
            ) from None

        if self._msr.numsamples <= 0:
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

    @property
    def datasize(self) -> int:
        """Return size of decoded data payload in bytes"""
        return self._msr.datasize

    @property
    def numsamples(self) -> int:
        """Number of data samples that have been decoded and are available.

        This represents the actual number of samples accessible via datasamples
        or np_datasamples properties. May differ from samplecnt (which is the
        declared sample count in the record header) if data has not been
        decoded.

        Returns:
            int: Number of decoded samples (0 if no data decoded)

        See Also:
            samplecnt: Sample count from record header
            datasamples: Access to the actual sample data
        """
        return self._msr.numsamples

    @property
    def sampletype(self) -> Optional[str]:
        """Return sample type code if available, otherwise None"""
        if self._msr.sampletype == b"\x00":
            return None
        return self._msr.sampletype.decode("ascii")

    def sampletype_str(self) -> Optional[str]:
        """Return sample type as descriptive string"""
        sampletype = self.sampletype
        if sampletype == "i":
            return "int32"
        elif sampletype == "f":
            return "float32"
        elif sampletype == "d":
            return "float64"
        elif sampletype == "t":
            return "text"
        else:
            return None

    @property
    def endtime(self) -> int:
        """End time of the last sample as nanoseconds since Unix epoch.

        Calculated from starttime, sample rate, and number of samples.
        For regularly sampled data: endtime = starttime + (numsamples-1)/samprate

        Returns:
            int: Nanoseconds since Unix epoch (1970-01-01T00:00:00Z)

        See Also:
            starttime: Start time of first sample
            endtime_seconds: End time as floating-point seconds
            endtime_str(): Human-readable end time string
        """
        return clibmseed.msr3_endtime(self._msr)

    @property
    def endtime_seconds(self) -> float:
        """Return end time as seconds since Unix/POSIX epoch"""
        return clibmseed.msr3_endtime(self._msr) / clibmseed.NSTMODULUS

    def endtime_str(
        self,
        timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
        subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
    ) -> Optional[str]:
        """Return end time as formatted string"""
        return nstime2timestr(self.endtime, timeformat, subsecond)

    def encoding_str(self) -> Optional[str]:
        """Human-readable description of the data encoding format.

        Returns:
            str or None: Descriptive string like "Steim2", "Float32", "Integer32",
                        or None if encoding is unknown

        See Also:
            encoding: Numeric encoding code
        """
        return encoding_string(self._msr.encoding)

    def print(self, details: int = 0) -> None:
        """Print record information to stdout with configurable detail level.

        Useful for debugging and inspecting record contents. Output includes
        metadata, timing information, and optionally sample data.

        Args:
            details: Detail level for output:
                    0 = Basic record header information (default)
                    1 = All record header information
        """
        clibmseed.msr3_print(self._msr, details)

    def unpack_data(self, verbose: int = 0) -> int:
        """Unpack the record's data samples

        This method unpacks (decodes) the data samples associated with the
        record if they were not decoded during the parsing process.  This is
        useful when only specific records need to be unpacked or for delayed
        unpacking workflows.

        Args:
            verbose: Verbosity level for diagnostic output. Default: 0

        Returns:
            Number of samples unpacked

        Raises:
            MiniSEEDError: If unpacking fails
        """
        samples_unpacked = clibmseed.msr3_unpack_data(self._msr, verbose)

        if samples_unpacked < 0:
            raise MiniSEEDError(samples_unpacked, "Error unpacking data samples")

        return samples_unpacked

    @contextmanager
    def with_datasamples(self, data_samples: Sequence[Any], sample_type: str):
        """Context manager for temporarily setting data samples with automatic cleanup.

        This context manager temporarily sets data samples, counts, and type for the record
        and automatically restores the original state when exiting the context.

        A common use case is to set data samples for creating (packing) miniSEED records,
        but this can be used for any purpose that requires setting data samples for a
        short period of time.

        Args:
            data_samples: Sequence containing the data samples. Can be a list, numpy array,
                memoryview, or any object supporting the sequence protocol.  If the value
                supports a memoryview, it will be used directly without copying.
            sample_type: Single character string indicating the data type ('i', 'f', 'd', 't')

        Yields:
            MS3Record: The record with the temporary data samples set

        Examples:
            Setting data samples for packing:

            >>> from pymseed import MS3Record, DataEncoding
            >>> msr = MS3Record()
            >>> msr.sourceid = "FDSN:XX_TEST__L_S_X"
            >>> msr.reclen = 512
            >>> msr.formatversion = 3
            >>> msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
            >>> msr.samprate = 100.0
            >>> msr.pubversion = 1
            >>> msr.encoding = DataEncoding.STEIM2

            # A data array that can be used without copying (zero-copy).  This is
            # a common case for data that is already in a bytearray, numpy array,
            # or other object from which a memoryview can be created.
            >>> import array
            >>> data = array.array('i', [1, 2, 3, 4])

            >>> output_file = "output.mseed"
            >>> with msr.with_datasamples(data, 'i'):
            ...     print (f"Writing records for {msr.numsamples} samples of type {msr.sampletype}")
            ...     packed_records = msr.to_file(output_file) # doctest: +SKIP
            Writing records for 4 samples of type i

            # Setting data samples for packing from a simple list that will be copied

            >>> data = [1.1, 2.6, 3.2, 4.8] # A simple list will be copied (no memoryview)
            >>> with msr.with_datasamples(data, 'f'):
            ...     print (f"Record has {msr.numsamples} samples of type {msr.sampletype}")
            Record has 4 samples of type f

            # Setting text data can be a string, bytes, bytearray, or a sequenced
            # that is convergted to byte characters.  Text data is always copied.

            >>> text_samples = "This is a log entry"
            >>> msr.sourceid = "FDSN:XX_TEST__L_O_G"
            >>> msr.samperate = 0
            >>> with msr.with_datasamples(text_samples, 't'):
            ...     print (f"Record has {msr.numsamples} samples of type {msr.sampletype}")
            Record has 19 samples of type t

        Note:
            The original record state is completely restored when exiting the context,
            including datasamples pointer, data size, sample counts, and sample type.

        See Also:
            MS3TraceList.add_data(): Add data samples to a trace list
        """
        # Save original state
        orig_datasamples = self._msr.datasamples
        orig_datasize = self._msr.datasize
        orig_samplecnt = self._msr.samplecnt
        orig_numsamples = self._msr.numsamples
        orig_sampletype = self._msr.sampletype

        try:
            # Set temporary data directly (inlined implementation)
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

                self._msr.datasamples = sample_array
                self._msr.numsamples = len(data_samples)
                self._msr.datasize = len(data_samples) * 4
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

                self._msr.datasamples = sample_array
                self._msr.numsamples = len(data_samples)
                self._msr.datasize = len(data_samples) * 4
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

                self._msr.datasamples = sample_array
                self._msr.numsamples = len(data_samples)
                self._msr.datasize = len(data_samples) * 8
            elif sample_type == "t":
                # Convert everything to bytes for simplicity
                if isinstance(data_samples, str):
                    text_bytes = data_samples.encode("utf-8")
                elif isinstance(data_samples, (bytes, bytearray)):
                    text_bytes = bytes(data_samples)
                else:
                    # Handle sequence - convert each item to byte
                    byte_values = []
                    for sample in data_samples:
                        if isinstance(sample, str):
                            byte_values.append(sample.encode("utf-8")[0])
                        else:
                            byte_values.append(int(sample) & 0xFF)
                    text_bytes = bytes(byte_values)

                sample_array = ffi.new("char[]", text_bytes)
                self._msr.datasamples = sample_array
                self._msr.numsamples = len(text_bytes)
                self._msr.datasize = len(text_bytes)
            else:
                raise ValueError(f"Unknown sample type: {sample_type}")

            self._msr.samplecnt = self._msr.numsamples
            self._msr.sampletype = sample_type.encode("ascii")

            yield self
        finally:
            # Restore original state
            self._msr.datasamples = orig_datasamples
            self._msr.datasize = orig_datasize
            self._msr.samplecnt = orig_samplecnt
            self._msr.numsamples = orig_numsamples
            self._msr.sampletype = orig_sampletype

    def _record_handler_wrapper(self, record: Any, record_length: int, handlerdata: Any) -> None:
        """Callback function for msr3_pack()"""
        # Convert CFFI buffer to bytes for the handler
        record_bytes = ffi.buffer(record, record_length)[:]
        self._record_handler(record_bytes, self._record_handler_data)

    def pack(
        self,
        handler: Callable[[bytes, Any], None],
        handler_data: Any = None,
        data_samples: Optional[Union[list[int], list[float], list[str]]] = None,
        sample_type: Optional[str] = None,
        verbose: int = 0,
    ) -> tuple[int, int]:
        """Pack data samples into miniSEED record(s) using a custom handler.

        .. deprecated::
            The `pack()` method is deprecated in favor of the more Pythonic
            `generate()` method. Use `generate()` for most use cases as it
            provides a cleaner generator-based interface with equivalent functionality.

        This method encodes data samples into one or more miniSEED records according
        to the record's configuration (encoding, record length, etc.) and calls the
        provided handler function for each generated record.

        Args:
            handler: Callback function that receives each packed record.
                    Signature: handler(record: bytes, handler_data: Any)
            handler_data: Optional data passed to the handler function.
                         Commonly used for file handles, counters, or containers.
            data_samples: Data to pack. If None, uses existing record data.
                         Types supported: list of int/float/str, numpy arrays,
                         or any buffer-protocol compatible object.
            sample_type: Sample type indicator when providing data_samples:
                        'i' = 32-bit integer
                        'f' = 32-bit float
                        'd' = 64-bit float
                        't' = text (1 byte per sample)
                        Required when data_samples is provided.
            verbose: Verbosity level for diagnostic output (0=quiet, 1+=verbose)

        Returns:
            tuple[int, int]: (total_samples_packed, number_of_records_created)

        Raises:
            MiniSEEDError: If packing fails due to invalid configuration or data
            ValueError: If sample_type is invalid or data format is incompatible

        Examples:
            >>> from pymseed import MS3Record, DataEncoding
            >>> import warnings

            >>> # Write to file
            >>> def file_handler(record: bytes, file_handle: Any):
            ...     file_handle.write(record)

            >>> msr = MS3Record() # doctest: +SKIP
            >>> with open('output.mseed', 'wb') as f: # doctest: +SKIP
            ...     with warnings.catch_warnings():
            ...         warnings.simplefilter("ignore", DeprecationWarning)
            ...         samples, records = msr.pack(
            ...             file_handler, f,
            ...             data_samples=[1, 2, 3, 4, 5],
            ...             sample_type='i'
            ...         )

            >>> # Collect records in memory
            >>> records_list = []
            >>> def collect_handler(record: bytes, container: Any):
            ...     container.append(record)

            >>> msr = MS3Record()
            >>> msr.encoding = DataEncoding.FLOAT32
            >>> with warnings.catch_warnings():
            ...     warnings.simplefilter("ignore", DeprecationWarning)
            ...     msr.pack(collect_handler, records_list, data_samples=[1.0, 2.0], sample_type='f')
            (2, 1)

        Notes:
            The handler function must use or copy the record buffer as the memory
            may be reused on subsequent iterations.

        See Also:
            generate(): Recommended replacement - Pythonic generator-based interface
        """
        # Issue deprecation warning
        warnings.warn(
            "pack() is deprecated and will be removed in a future version. "
            "Use generate() instead for a more Pythonic generator-based interface.",
            DeprecationWarning,
            stacklevel=2
        )

        # Set handler function as CFFI callback function
        self._record_handler = handler
        self._record_handler_data = handler_data

        # Create callback function type and instance
        RECORD_HANDLER = ffi.callback("void(char *, int, void *)", self._record_handler_wrapper)

        packed_samples = ffi.new("int64_t *")
        flags = clibmseed.MSF_FLUSHDATA  # Always flush data when packing

        if data_samples is not None and sample_type is not None:
            with self.with_datasamples(data_samples, sample_type):
                packed_records = clibmseed.msr3_pack(
                    self._msr,
                    RECORD_HANDLER,
                    ffi.NULL,
                    packed_samples,
                    flags,
                    verbose,
                )
        else:
            packed_records = clibmseed.msr3_pack(
                self._msr,
                RECORD_HANDLER,
                ffi.NULL,
                packed_samples,
                flags,
                verbose,
            )

        if packed_records < 0:
            raise MiniSEEDError(packed_records, "Error packing miniSEED record(s)")

        return (packed_samples[0], packed_records)

    def generate(
        self,
        data_samples: Optional[Union[list[int], list[float], list[str]]] = None,
        sample_type: Optional[str] = None,
        verbose: int = 0,
    ) -> Iterator[bytes]:
        """Create miniSEED record(s) using parameters from the record.

        This method creates miniSEED records using the parameters (encoding,
        record length, etc.) in this MS3Record. Alternante data samples (and
        type) can be provided, otherwise the existing record data is used.

        This method is a generator that yields each miniSEED record as it is
        created as bytes.

        Args:
            data_samples: Data to pack. If None, uses existing record data.
                         Types supported: list of int/float/str, numpy arrays,
                         or any buffer-protocol compatible object.
            sample_type: Sample type indicator when providing data_samples:
                        'i' = 32-bit integer 'f' = 32-bit float 'd' = 64-bit
                        float 't' = text (1 byte per sample) Required when
                        data_samples is provided.
            verbose: Verbosity level for diagnostic output (0=quiet, 1+=verbose)

        Yields:
            bytes: Each miniSEED record as it is created

        Examples:
            >>> from pymseed import MS3Record, DataEncoding
            >>>
            >>> # Create record template with MS3Record()
            >>> msr = MS3Record()
            >>> msr.sourceid = "FDSN:XX_TEST__L_H_Z"
            >>> msr.starttime_str = "2024-01-01T00:00:00Z"
            >>> msr.samprate = 1
            >>>
            >>> # Generate miniSEED records and write to file
            >>> # (See `MS3Record.to_file()` for a more convenient way to write to a file)
            >>> with open('output.mseed', 'wb') as f:  # doctest: +SKIP
            ...     for record in msr.generate(
            ...         data_samples=[1, 2, 3, 4, 5],
            ...         sample_type='i'
            ...     ):
            ...         f.write(record)
            >>>
            >>> # Generate miniSEED records and collect in a list
            >>> record_list = []
            >>> msr.encoding = DataEncoding.FLOAT32
            >>> data_samples = [1.0, 2.1, 3.2, 4.3, 5.4]
            >>> for record in msr.generate(data_samples=data_samples, sample_type='f'):
            ...     record_list.append(record)
            >>> print(f"Generated {len(record_list)} records")
            Generated 1 records

        See Also:
            MS3Record: Full record documentation
        """
        flags = clibmseed.MSF_FLUSHDATA  # Always flush data when packing

        record_pp = ffi.new("char **")
        reclen_p = ffi.new("int32_t *")

        # Pack miniSEED records using data samples and type if provided
        if data_samples is not None and sample_type is not None:
            with self.with_datasamples(data_samples, sample_type):

                packer = clibmseed.msr3_pack_init(self._msr, flags, verbose)

                if packer is None:
                    raise MiniSEEDError(-1, "Error initializing packer")

                while clibmseed.msr3_pack_next(packer, record_pp, reclen_p) == 1:
                    yield ffi.buffer(record_pp[0], reclen_p[0])[:]
        # Otherwise, pack miniSEED records using the record's existing data
        else:
            packer = clibmseed.msr3_pack_init(self._msr, flags, verbose)

            if packer is None:
                raise MiniSEEDError(-1, "Error initializing packer")

            while clibmseed.msr3_pack_next(packer, record_pp, reclen_p) == 1:
                yield ffi.buffer(record_pp[0], reclen_p[0])[:]

        # Free packer
        packer_pp = ffi.new("MS3RecordPacker **")
        packer_pp[0] = packer
        clibmseed.msr3_pack_free(packer_pp, ffi.NULL)

    def to_file(
        self,
        filename: str,
        overwrite: bool = False,
        verbose: int = 0,
    ) -> int:
        """Write data contained in the record to a miniSEED file

        Args:
            filename: Path to the output miniSEED file. The file will be created if it
                doesn't exist. Directory must already exist.
            overwrite: If True, overwrites existing file. If False and file exists,
                append data to the end of the file. Default is False for safety.
            verbose: Verbosity level for libmseed output (0=quiet, 1=info, 2=detailed).

        Returns:
            int: Number of miniSEED records written to the file.

        Raises:
            MiniSEEDError: If the underlying libmseed library encounters an error during
                file writing (e.g., permission denied, disk full, invalid data).

        See Also:
            generate(): For creating record
            MS3Record: Full record documentation
        """
        # Convert filename to bytes (C string)
        c_filename = ffi.new("char[]", filename.encode("utf-8"))

        # Set flags based on record configuration
        pack_flags = clibmseed.MSF_FLUSHDATA  # Pack all available data
        if self._msr.formatversion == 2:
            pack_flags |= clibmseed.MSF_PACKVER2  # Force miniSEED version 2 format

        # Call the C function
        packed_records = clibmseed.msr3_writemseed(
            self._msr,
            c_filename,
            overwrite,
            pack_flags,
            verbose,
        )

        if packed_records < 0:
            raise MiniSEEDError(packed_records, "Error writing miniSEED file")

        return packed_records

    @classmethod
    def from_file(cls, filename, **kwargs):
        """Create a record reader for miniSEED files.

        This convenience method returns an MS3RecordReader that can iterate over
        all records in a miniSEED file.

        Note that the objects returned by this iterator are only valid during
        the lifetime of the iterator. Once the iterator is exhausted, the
        objects are no longer valid and should not be used.

        Args:
            filename: Path to miniSEED file
            **kwargs: Additional arguments passed to MS3RecordReader

        Returns:
            MS3RecordReader: Iterator over records in the file

        See Also:
            from_buffer(): Read from memory buffer
            MS3RecordReader: Full file reader documentation
        """
        # Lazy import to avoid circular dependency
        from .msrecord_reader import MS3RecordReader

        return MS3RecordReader(filename, **kwargs)

    @classmethod
    def from_buffer(cls, buffer, **kwargs):
        """Create a record reader for miniSEED data in memory.

        This convenience method returns an MS3RecordBufferReader that can
        iterate over records stored in a memory buffer (bytes-like object).

        Note that the objects returned by this iterator are only valid during
        the lifetime of the iterator. Once the iterator is exhausted, the
        objects are no longer valid and should not be used.

        Args:
            buffer: Bytes-like object containing miniSEED data
            **kwargs: Additional arguments passed to MS3RecordBufferReader

        Returns:
            MS3RecordBufferReader: Iterator over records in the buffer

        See Also:
            from_file(): Read from file
            MS3RecordBufferReader: Full buffer reader documentation
        """
        # Lazy import to avoid circular dependency
        from .msrecord_buffer_reader import MS3RecordBufferReader

        return MS3RecordBufferReader(buffer, **kwargs)
