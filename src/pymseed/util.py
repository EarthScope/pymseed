"""
Core utility functions for pymseed

"""

from typing import Optional

from .clib import cdata_to_string, clibmseed, ffi
from .definitions import SubSecond, TimeFormat


def nstime2timestr(
    nstime: int,
    timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
    subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
) -> Optional[str]:
    """Convert a nanosecond timestamp to a date-time string"""
    # Create a buffer for the time string (40 chars should be enough)
    c_timestr = ffi.new("char[]", 50)

    status = clibmseed.ms_nstime2timestr_n(nstime, c_timestr, 50, timeformat, subsecond)

    if status != 0:  # Success check - differs from ctypes version
        return cdata_to_string(c_timestr)
    else:
        raise ValueError(f"Error converting timestamp: {nstime}")


def timestr2nstime(timestr: str) -> int:
    """Convert a date-time string to nanoseconds since Unix epoch"""
    c_timestr = ffi.new("char[]", timestr.encode("utf-8"))
    return clibmseed.ms_timestr2nstime(c_timestr)


def sourceid2nslc(
    sourceid: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Convert an FDSN source ID to a tuple of (net, sta, loc, chan)"""
    max_size = 11
    net = ffi.new("char[]", max_size)
    sta = ffi.new("char[]", max_size)
    loc = ffi.new("char[]", max_size)
    chan = ffi.new("char[]", max_size)

    c_sourceid = ffi.new("char[]", sourceid.encode("utf-8"))

    status = clibmseed.ms_sid2nslc_n(
        c_sourceid, net, max_size, sta, max_size, loc, max_size, chan, max_size
    )

    if status == 0:
        return (
            cdata_to_string(net),
            cdata_to_string(sta),
            cdata_to_string(loc),
            cdata_to_string(chan),
        )
    else:
        raise ValueError(f"Invalid source ID: {sourceid}")


def nslc2sourceid(net: str, sta: str, loc: str, chan: str) -> str:
    """Convert network, station, location, channel to FDSN source ID"""
    max_sid_len = 64
    sid = ffi.new("char[]", max_sid_len)

    c_net = ffi.new("char[]", net.encode("utf-8"))
    c_sta = ffi.new("char[]", sta.encode("utf-8"))
    c_loc = ffi.new("char[]", loc.encode("utf-8"))
    c_chan = ffi.new("char[]", chan.encode("utf-8"))

    flags = 0
    status = clibmseed.ms_nslc2sid(sid, max_sid_len, flags, c_net, c_sta, c_loc, c_chan)

    if status > 0:
        result = cdata_to_string(sid)
        if result is None:
            raise ValueError(f"Error creating source ID from {net}.{sta}.{loc}.{chan}")
        return result
    else:
        raise ValueError(f"Error creating source ID from {net}.{sta}.{loc}.{chan}")


def encoding_string(encoding: int) -> Optional[str]:
    """Get descriptive string for encoding format"""
    return cdata_to_string(clibmseed.ms_encodingstr(encoding))


def error_string(error_code: int) -> Optional[str]:
    """Get descriptive string for error code"""
    return cdata_to_string(clibmseed.ms_errorstr(error_code))


def sample_size(encoding: int) -> int:
    """Get sample size in bytes for given encoding"""
    return clibmseed.ms_samplesize(encoding)


def encoding_sizetype(encoding: int) -> tuple[int, str]:
    """Get sample size and type for given encoding"""
    sample_size = ffi.new("uint8_t *")
    sample_type = ffi.new("char [1]")

    status = clibmseed.ms_encoding_sizetype(encoding, sample_size, sample_type)

    if status >= 0:
        return (sample_size[0], sample_type[0].decode("utf-8"))
    else:
        raise ValueError(f"Error getting size/type for encoding {encoding}")


def sample_time(time: int, offset: int, samprate: float) -> int:
    """Calculate time for a sample at given offset"""
    return clibmseed.ms_sampletime(time, offset, samprate)


def system_time() -> int:
    """Get the current system time in nanoseconds"""
    return clibmseed.lmp_systemtime()
