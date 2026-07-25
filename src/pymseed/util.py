"""
Core utility functions for pymseed

"""

from typing import Any

from .clib import cdata_to_string, clibmseed, ffi
from .definitions import SubSecond, TimeFormat

# Maximum length of any time string libmseed produces with some margin
_TIMESTRING_BUFSIZE = 50


def check_encoding(encoding: int) -> None:
    """Raise ValueError if encoding is outside the range libmseed stores it in"""
    if not 0 <= encoding <= 255:
        raise ValueError(f"encoding must be in the range 0..255; got {encoding}")


def check_str(name: str, value: Any) -> None:
    """Raise TypeError if value is not a str, before it is encoded for C"""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be str; got {type(value).__name__}")


def nstime2timestr(
    nstime: int,
    timeformat: TimeFormat = TimeFormat.ISOMONTHDAY_Z,
    subsecond: SubSecond = SubSecond.NANO_MICRO_NONE,
) -> str:
    """Convert a nanosecond timestamp to a date-time string"""
    c_timestr = ffi.new("char[]", _TIMESTRING_BUFSIZE)

    result = clibmseed.ms_nstime2timestr_n(
        nstime, c_timestr, _TIMESTRING_BUFSIZE, timeformat, subsecond
    )

    if result == ffi.NULL:
        raise ValueError(f"Error converting timestamp: {nstime}")

    return ffi.string(c_timestr).decode("utf-8")


def timestr2nstime(timestr: str) -> int:
    """Convert a date-time string to nanoseconds since Unix epoch

    Raises:
        TypeError: If ``timestr`` is not a str.
        ValueError: If ``timestr`` cannot be parsed as a date-time string.
    """
    check_str("timestr", timestr)

    c_timestr = ffi.new("char[]", timestr.encode("utf-8"))
    nstime = clibmseed.ms_timestr2nstime(c_timestr)

    if nstime == clibmseed.NSTERROR:
        raise ValueError(f"Invalid time string: {timestr!r}")

    return nstime


# Per-SEED code buffer size for sourceid2nslc().  Plenty of headroom for
# larger-than-SEED codes and extended channels.
_NSLC_CODE_BUFSIZE = 16


def sourceid2nslc(sourceid: str) -> tuple[str, str, str, str]:
    """Convert an FDSN source ID to a tuple of (net, sta, loc, chan)

    Components that are empty in the source ID are returned as empty
    strings.  Raises ``TypeError`` if the source ID is not a str, and
    ``ValueError`` if it is malformed.
    """
    check_str("sourceid", sourceid)

    net = ffi.new("char[]", _NSLC_CODE_BUFSIZE)
    sta = ffi.new("char[]", _NSLC_CODE_BUFSIZE)
    loc = ffi.new("char[]", _NSLC_CODE_BUFSIZE)
    chan = ffi.new("char[]", _NSLC_CODE_BUFSIZE)

    c_sourceid = ffi.new("char[]", sourceid.encode("utf-8"))

    status = clibmseed.ms_sid2nslc_n(
        c_sourceid,
        net,
        _NSLC_CODE_BUFSIZE,
        sta,
        _NSLC_CODE_BUFSIZE,
        loc,
        _NSLC_CODE_BUFSIZE,
        chan,
        _NSLC_CODE_BUFSIZE,
    )

    if status != 0:
        raise ValueError(f"Invalid source ID: {sourceid}")

    return (
        ffi.string(net).decode("utf-8"),
        ffi.string(sta).decode("utf-8"),
        ffi.string(loc).decode("utf-8"),
        ffi.string(chan).decode("utf-8"),
    )


def nslc2sourceid(net: str, sta: str, loc: str, chan: str) -> str:
    """Convert network, station, location, channel to FDSN source ID

    Raises ``TypeError`` if a component is not a str, and ``ValueError`` if the
    components cannot be combined into a valid FDSN source ID.
    """
    for name, value in (("net", net), ("sta", sta), ("loc", loc), ("chan", chan)):
        check_str(name, value)

    sid = ffi.new("char[]", clibmseed.LM_SIDLEN)

    c_net = ffi.new("char[]", net.encode("utf-8"))
    c_sta = ffi.new("char[]", sta.encode("utf-8"))
    c_loc = ffi.new("char[]", loc.encode("utf-8"))
    c_chan = ffi.new("char[]", chan.encode("utf-8"))

    flags = 0
    status = clibmseed.ms_nslc2sid(sid, clibmseed.LM_SIDLEN, flags, c_net, c_sta, c_loc, c_chan)

    if status < 0:
        raise ValueError(f"Error creating source ID from {net}.{sta}.{loc}.{chan}")

    return ffi.string(sid).decode("utf-8")


def encoding_string(encoding: int) -> str:
    """Get descriptive string for encoding format.

    Returns ``"Unset"`` for -1, the unset value, and libmseed's default unknown
    value, e.g. ``"Unknown"``, for unrecognized encoding values.

    Raises:
        ValueError: If ``encoding`` is outside the ``uint8_t`` range (0-255)
            and is not -1.
    """
    if encoding == -1:
        return "Unset"

    check_encoding(encoding)

    return ffi.string(clibmseed.ms_encodingstr(encoding)).decode("utf-8")


def error_string(error_code: int) -> str | None:
    """Get descriptive string for error code"""
    return cdata_to_string(clibmseed.ms_errorstr(error_code))


def sample_size(sample_type: bytes | str) -> int:
    """Get sample size in bytes for given sample type.

    Raises:
        ValueError: If ``sample_type`` is not a single character, or is not a
            recognized sample type code.
    """

    if isinstance(sample_type, str):
        sample_type = sample_type.encode("ascii")

    # ms_samplesize takes a single C char
    if len(sample_type) != 1:
        raise ValueError(f"Invalid sample type: {sample_type!r}. Must be a single character.")

    size = clibmseed.ms_samplesize(sample_type)

    # A size of zero is libmseed's unrecognized-type return
    if size == 0:
        raise ValueError(f"Unknown sample type: {sample_type!r}")

    return size


def encoding_sizetype(encoding: int) -> tuple[int, str]:
    """Get sample size and type for given encoding.

    Raises:
        ValueError: If ``encoding`` is outside the ``uint8_t`` range (0-255)
            or is not a recognized encoding code.
    """
    check_encoding(encoding)

    samplesize_out = ffi.new("uint8_t *")
    sampletype_out = ffi.new("char [1]")

    status = clibmseed.ms_encoding_sizetype(encoding, samplesize_out, sampletype_out)

    if status < 0:
        raise ValueError(f"Error getting size/type for encoding {encoding}")

    return (samplesize_out[0], sampletype_out[0].decode("utf-8"))


def sample_time(time: int, offset: int, samprate: float) -> int:
    """Calculate time for a sample at given offset"""
    return clibmseed.ms_sampletime(time, offset, samprate)


def system_time() -> int:
    """Get the current system time in nanoseconds"""
    return clibmseed.lmp_systemtime()
