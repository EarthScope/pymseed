"""
Definitions and constants for pymseed

Note: Most constants are imported directly from the CFFI interface (clibmseed).
This module provides enums for some of the grouped constants.
"""

from enum import IntEnum

# Import the CFFI interface
try:
    from .clib import clibmseed
except ImportError:
    raise ImportError("CFFI interface not available. Please build the CFFI interface first.") from None


class DataEncoding(IntEnum):
    """Data encoding format codes"""

    TEXT = clibmseed.DE_TEXT  # Text encoding (UTF-8)
    INT16 = clibmseed.DE_INT16  # 16-bit integer
    INT32 = clibmseed.DE_INT32  # 32-bit integer
    FLOAT32 = clibmseed.DE_FLOAT32  # 32-bit float (IEEE)
    FLOAT64 = clibmseed.DE_FLOAT64  # 64-bit float (IEEE)
    STEIM1 = clibmseed.DE_STEIM1  # Steim-1 compressed 32-bit integers
    STEIM2 = clibmseed.DE_STEIM2  # Steim-2 compressed 32-bit integers


class TimeFormat(IntEnum):
    """Time format codes for ms_nstime2timestr() and ms_nstime2timestrz()"""

    ISOMONTHDAY = clibmseed.ISOMONTHDAY  # "YYYY-MM-DDThh:mm:ss.sssssssss", ISO 8601 in month-day format
    ISOMONTHDAY_Z = clibmseed.ISOMONTHDAY_Z  # "YYYY-MM-DDThh:mm:ss.sssssssss", ISO 8601 in month-day format with trailing Z
    ISOMONTHDAY_DOY = clibmseed.ISOMONTHDAY_DOY  # "YYYY-MM-DD hh:mm:ss.sssssssss (doy)", ISOMONTHDAY with day-of-year
    ISOMONTHDAY_DOY_Z = clibmseed.ISOMONTHDAY_DOY_Z  # "YYYY-MM-DD hh:mm:ss.sssssssss (doy)", ISOMONTHDAY with day-of-year and trailing Z
    ISOMONTHDAY_SPACE = clibmseed.ISOMONTHDAY_SPACE  # "YYYY-MM-DD hh:mm:ss.sssssssss", same as ISOMONTHDAY with space separator
    ISOMONTHDAY_SPACE_Z = clibmseed.ISOMONTHDAY_SPACE_Z  # "YYYY-MM-DD hh:mm:ss.sssssssss", same as ISOMONTHDAY with space separator and trailing Z
    SEEDORDINAL = clibmseed.SEEDORDINAL  # "YYYY,DDD,hh:mm:ss.sssssssss", SEED day-of-year format
    UNIXEPOCH = clibmseed.UNIXEPOCH  # "ssssssssss.sssssssss", Unix epoch value
    NANOSECONDEPOCH = clibmseed.NANOSECONDEPOCH  # "sssssssssssssssssss", Nanosecond epoch value


class SubSecond(IntEnum):
    """Subsecond resolution codes for ms_nstime2timestr() and ms_nstime2timestrz()"""

    NONE = clibmseed.NONE  # No subseconds
    MICRO = clibmseed.MICRO  # Microsecond resolution
    NANO = clibmseed.NANO  # Nanosecond resolution
    MICRO_NONE = clibmseed.MICRO_NONE  # Microsecond resolution if subseconds are non-zero, otherwise no subseconds
    NANO_NONE = clibmseed.NANO_NONE  # Nanosecond resolution if subseconds are non-zero, otherwise no subseconds
    NANO_MICRO = clibmseed.NANO_MICRO  # Nanosecond resolution if there are sub-microseconds, otherwise microseconds resolution
    NANO_MICRO_NONE = clibmseed.NANO_MICRO_NONE  # Nanosecond resolution if present, microsecond if present, otherwise no subseconds
