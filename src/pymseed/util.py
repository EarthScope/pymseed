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
    """Convert a nanosecond timestamp to a date-time string

    The returned string is a subset of the RFC 3339 (ISO 8601 profile) and
    always in UTC (Z suffix optional).

    ``timeformat`` selects the overall layout via :class:`~pymseed.TimeFormat`
    (ISO month-day with ``T`` or space, optional trailing ``Z``, SEED ordinal
    day-of-year, Unix epoch seconds, or nanosecond epoch).  The default is
    :attr:`~pymseed.TimeFormat.ISOMONTHDAY_Z`
    (``YYYY-MM-DDThh:mm:ss[.fraction]Z``).

    ``subsecond`` selects how fractional seconds are shown via
    :class:`~pymseed.SubSecond`: fixed none / micro / nano resolution, or
    conditional forms that omit trailing zero fractions.  The default,
    :attr:`~pymseed.SubSecond.NANO_MICRO_NONE`, uses nanoseconds when needed,
    otherwise microseconds when needed, otherwise no fractional part.

    Examples:
        >>> from pymseed import nstime2timestr, timestr2nstime, TimeFormat, SubSecond
        >>> t = timestr2nstime("2024-03-15T10:30:00.123456789Z")
        >>> nstime2timestr(t)
        '2024-03-15T10:30:00.123456789Z'
        >>> nstime2timestr(t, subsecond=SubSecond.MICRO)
        '2024-03-15T10:30:00.123456Z'
        >>> nstime2timestr(t, timeformat=TimeFormat.ISOMONTHDAY_SPACE_Z, subsecond=SubSecond.NONE)
        '2024-03-15 10:30:00Z'
    """
    c_timestr = ffi.new("char[]", _TIMESTRING_BUFSIZE)

    result = clibmseed.ms_nstime2timestr_n(
        nstime, c_timestr, _TIMESTRING_BUFSIZE, timeformat, subsecond
    )

    if result == ffi.NULL:
        raise ValueError(f"Error converting timestamp: {nstime}")

    return ffi.string(c_timestr).decode("utf-8")


def timestr2nstime(timestr: str) -> int:
    """Convert a date-time string to nanoseconds since Unix epoch

    The time string is parsed as subset of the RFC 3339 (ISO 8601 profile)
    format.  The time string must be in UTC (Z suffix optional), other
    time zones are not supported.

    Examples:
        >>> from pymseed import timestr2nstime
        >>> timestr2nstime("2024-03-15T10:30:00.123456789Z")
        1710498600123456789
        >>> timestr2nstime("1964-03-28T03:36:14Z")
        -181859026000000000
        >>> timestr2nstime("1970-01-01T00:00:00Z")
        0

        >>> timestr2nstime("not-a-time")
        Traceback (most recent call last):
            ...
        ValueError: Invalid time string: 'not-a-time'

        >>> timestr2nstime(42)
        Traceback (most recent call last):
            ...
        TypeError: timestr must be str; got int

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
    """Convert an FDSN Source ID to a tuple of (net, sta, loc, chan)

    Components that are empty in the Source ID are returned as empty
    strings.

    The individual codes in the Source ID are commonly compatible with
    SEED format codes.  However, they can also be extended codes allowed by
    the Source ID standard, such as long network, station, and location
    codes and extended channel codes including multiple-character band,
    source or subsource codes.

    Examples:
        >>> from pymseed import sourceid2nslc
        >>> sourceid2nslc("FDSN:IU_COLA_00_B_H_Z")
        ('IU', 'COLA', '00', 'BHZ')
        >>> sourceid2nslc("FDSN:IU_COLA__B_H_Z")
        ('IU', 'COLA', '', 'BHZ')
        >>> sourceid2nslc("FDSN:NETWORK_STATION_LOCATION_XX_YY_ZZ")
        ('NETWORK', 'STATION', 'LOCATION', 'XX_YY_ZZ')

        >>> sourceid2nslc("not-a-source-id")
        Traceback (most recent call last):
            ...
        ValueError: Invalid FDSN Source ID: not-a-source-id

        >>> sourceid2nslc(42)
        Traceback (most recent call last):
            ...
        TypeError: sourceid must be str; got int

    Raises ``TypeError`` if the Source ID is not a str, and ``ValueError``
    if it is malformed.
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
        raise ValueError(f"Invalid FDSN Source ID: {sourceid}")

    return (
        ffi.string(net).decode("utf-8"),
        ffi.string(sta).decode("utf-8"),
        ffi.string(loc).decode("utf-8"),
        ffi.string(chan).decode("utf-8"),
    )


def nslc2sourceid(net: str, sta: str, loc: str, chan: str) -> str:
    """Convert network, station, location, channel to FDSN Source ID

    The network, station, location, and channel inputs are commonly
    SEED format codes.  They can also be extended codes allowed by the Source ID
    standard, such as long network, station, and location codes and extended
    channel codes including multiple-character band, source or subsource codes.

    Examples:
        >>> from pymseed import nslc2sourceid
        >>> nslc2sourceid("IU", "COLA", "00", "BHZ")
        'FDSN:IU_COLA_00_B_H_Z'
        >>> nslc2sourceid("IU", "COLA", "", "BHZ")
        'FDSN:IU_COLA__B_H_Z'
        >>> nslc2sourceid("NETWORK", "STATION", "LOCATION", "XX_YY_ZZ")
        'FDSN:NETWORK_STATION_LOCATION_XX_YY_ZZ'

        >>> nslc2sourceid("IU", "COLA", "00", 42)
        Traceback (most recent call last):
            ...
        TypeError: chan must be str; got int

        >>> nslc2sourceid("A" * 30, "B" * 30, "00", "BHZ")
        Traceback (most recent call last):
            ...
        ValueError: Error creating Source ID from AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB.00.BHZ

    Raises ``TypeError`` if a component is not a str, and ``ValueError`` if the
    components cannot be combined into a valid FDSN Source ID.
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
        raise ValueError(f"Error creating Source ID from {net}.{sta}.{loc}.{chan}")

    return ffi.string(sid).decode("utf-8")


def encoding_string(encoding: int) -> str:
    """Get descriptive string for encoding format.

    Returns ``"Unset"`` for -1, the unset value, and libmseed's default unknown
    value, e.g. ``"Unknown"``, for unrecognized encoding values.

    Examples:
        >>> from pymseed import DataEncoding
        >>> from pymseed.util import encoding_string
        >>> encoding_string(DataEncoding.STEIM2)
        'STEIM-2 integer compression'
        >>> encoding_string(-1)
        'Unset'

    Raises:
        ValueError: If ``encoding`` is outside the ``uint8_t`` range (0-255)
            and is not -1.
    """
    if encoding == -1:
        return "Unset"

    check_encoding(encoding)

    return ffi.string(clibmseed.ms_encodingstr(encoding)).decode("utf-8")


def error_string(error_code: int) -> str | None:
    """Get descriptive string for error code

    Returns ``None`` for unrecognized error codes.

    Examples:
        >>> from pymseed import clibmseed
        >>> from pymseed.util import error_string
        >>> error_string(clibmseed.MS_GENERROR)
        'Generic error'
        >>> error_string(clibmseed.MS_INVALIDCRC)
        'Invalid CRC detected'
    """
    return cdata_to_string(clibmseed.ms_errorstr(error_code))


def sample_size(sample_type: bytes | str) -> int:
    """Get sample size in bytes for given sample type.

    Examples:
        >>> from pymseed.util import sample_size
        >>> sample_size("i")
        4
        >>> sample_size("d")
        8
        >>> sample_size(b"t")
        1

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

    Returns a ``(size, type)`` pair where ``size`` is the decoded sample size
    in bytes and ``type`` is a sample type code (``'i'``, ``'f'``, ``'d'``,
    or ``'t'``).

    Examples:
        >>> from pymseed import DataEncoding
        >>> from pymseed.util import encoding_sizetype
        >>> encoding_sizetype(DataEncoding.STEIM2)
        (4, 'i')
        >>> encoding_sizetype(DataEncoding.FLOAT64)
        (8, 'd')

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
    """Calculate time for a sample at given offset from a given time and sample rate

    The time is returned in nanoseconds since Unix epoch.

    Examples:
        >>> from pymseed import sample_time, timestr2nstime, nstime2timestr
        >>> start = timestr2nstime("2024-01-01T00:00:00Z")
        >>> next_start = sample_time(start, 448, 40.0)
        >>> nstime2timestr(next_start)
        '2024-01-01T00:00:11.200000Z'
    """
    return clibmseed.ms_sampletime(time, offset, samprate)


def system_time() -> int:
    """Get the current system time in nanoseconds"""
    return clibmseed.lmp_systemtime()
