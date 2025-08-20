from .__version__ import __version__
from .clib import clibmseed, ffi
from .definitions import (
    DataEncoding,
    SubSecond,
    TimeFormat,
)
from .exceptions import MiniSEEDError
from .msrecord import MS3Record
from .msrecord_buffer_reader import MS3RecordBufferReader
from .msrecord_reader import MS3RecordReader
from .mstracelist import MS3TraceList
from .util import (
    nslc2sourceid,
    nstime2timestr,
    sample_time,
    sourceid2nslc,
    timestr2nstime,
)

libmseed_version = ffi.string(clibmseed.LIBMSEED_VERSION).decode('utf-8')

# Re-export these constants from the CFFI interface
NSTERROR = clibmseed.NSTERROR
NSTUNSET = clibmseed.NSTUNSET
NSTMODULUS = clibmseed.NSTMODULUS

__all__ = [
    "__version__",
    "libmseed_version",
    "NSTERROR",
    "NSTUNSET",
    "NSTMODULUS",
    "DataEncoding",
    "TimeFormat",
    "SubSecond",
    "MiniSEEDError",
    "MS3Record",
    "MS3RecordReader",
    "MS3RecordBufferReader",
    "MS3TraceList",
    "nstime2timestr",
    "timestr2nstime",
    "sourceid2nslc",
    "nslc2sourceid",
    "sample_time",
]

