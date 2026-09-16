"""
Definitions and constants for pymseed

Note: Most constants are imported directly from the CFFI interface (clibmseed).
This module provides enums for some of the grouped constants.
"""

from enum import IntEnum

from .clib import clibmseed


class DataEncoding(IntEnum):
    """Data encoding format codes"""

    #: Text encoding (UTF-8)
    TEXT = clibmseed.DE_TEXT
    #: 16-bit integer
    INT16 = clibmseed.DE_INT16
    #: 32-bit integer
    INT32 = clibmseed.DE_INT32
    #: 32-bit float (IEEE)
    FLOAT32 = clibmseed.DE_FLOAT32
    #: 64-bit float (IEEE)
    FLOAT64 = clibmseed.DE_FLOAT64
    #: Steim-1 compressed 32-bit integers
    STEIM1 = clibmseed.DE_STEIM1
    #: Steim-2 compressed 32-bit integers
    STEIM2 = clibmseed.DE_STEIM2
    #: Legacy GEOSCOPE 24-bit integer (decoding only)
    GEOSCOPE24 = clibmseed.DE_GEOSCOPE24
    #: Legacy GEOSCOPE 16-bit gain ranged, 3-bit exponent (decoding only)
    GEOSCOPE163 = clibmseed.DE_GEOSCOPE163
    #: Legacy GEOSCOPE 16-bit gain ranged, 4-bit exponent (decoding only)
    GEOSCOPE164 = clibmseed.DE_GEOSCOPE164
    #: Legacy CDSN 16-bit gain ranged (decoding only)
    CDSN = clibmseed.DE_CDSN
    #: Legacy SRO 16-bit gain ranged (decoding only)
    SRO = clibmseed.DE_SRO
    #: Legacy DWWSSN 16-bit gain ranged (decoding only)
    DWWSSN = clibmseed.DE_DWWSSN


class TimeFormat(IntEnum):
    """Time format codes for :func:`~pymseed.nstime2timestr`"""

    #: ``YYYY-MM-DDThh:mm:ss.sssssssss``, ISO 8601 in month-day format
    ISOMONTHDAY = clibmseed.ISOMONTHDAY
    #: ``YYYY-MM-DDThh:mm:ss.sssssssss``, ISO 8601 in month-day format with trailing ``Z``
    ISOMONTHDAY_Z = clibmseed.ISOMONTHDAY_Z
    #: ``YYYY-MM-DD hh:mm:ss.sssssssss (doy)``, ISOMONTHDAY with day-of-year
    ISOMONTHDAY_DOY = clibmseed.ISOMONTHDAY_DOY
    #: ``YYYY-MM-DD hh:mm:ss.sssssssss (doy)``, ISOMONTHDAY with day-of-year and trailing ``Z``
    ISOMONTHDAY_DOY_Z = clibmseed.ISOMONTHDAY_DOY_Z
    #: ``YYYY-MM-DD hh:mm:ss.sssssssss``, same as ISOMONTHDAY with space separator
    ISOMONTHDAY_SPACE = clibmseed.ISOMONTHDAY_SPACE
    #: ``YYYY-MM-DD hh:mm:ss.sssssssss``, same as ISOMONTHDAY with space separator and trailing ``Z``
    ISOMONTHDAY_SPACE_Z = clibmseed.ISOMONTHDAY_SPACE_Z
    #: ``YYYY,DDD,hh:mm:ss.sssssssss``, SEED day-of-year format
    SEEDORDINAL = clibmseed.SEEDORDINAL
    #: ``ssssssssss.sssssssss``, Unix epoch value
    UNIXEPOCH = clibmseed.UNIXEPOCH
    #: ``sssssssssssssssssss``, Nanosecond epoch value
    NANOSECONDEPOCH = clibmseed.NANOSECONDEPOCH


class SubSecond(IntEnum):
    """Subsecond resolution codes for :func:`~pymseed.nstime2timestr`"""

    #: No subseconds
    NONE = clibmseed.NONE
    #: Microsecond resolution
    MICRO = clibmseed.MICRO
    #: Nanosecond resolution
    NANO = clibmseed.NANO
    #: Microsecond resolution if subseconds are non-zero, otherwise no subseconds
    MICRO_NONE = clibmseed.MICRO_NONE
    #: Nanosecond resolution if subseconds are non-zero, otherwise no subseconds
    NANO_NONE = clibmseed.NANO_NONE
    #: Nanosecond resolution if there are sub-microseconds, otherwise microseconds resolution
    NANO_MICRO = clibmseed.NANO_MICRO
    #: Nanosecond resolution if present, microsecond if present, otherwise no subseconds
    NANO_MICRO_NONE = clibmseed.NANO_MICRO_NONE
