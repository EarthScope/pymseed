"""
Build libmseed selections from filter arguments.

Shared by the record readers (:mod:`pymseed.msrecord`,
:mod:`pymseed.msrecord_reader`) and the trace list
(:mod:`pymseed.mstracelist`).

This lives in its own module rather than in :mod:`pymseed.util` because
:mod:`pymseed.exceptions` imports from :mod:`pymseed.util`, and this helper
raises :class:`MiniSEEDError`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .clib import clibmseed, ffi
from .exceptions import MiniSEEDError
from .util import check_str, timestr2nstime


def build_selections(
    sourceid: str | None,
    starttime: str | None,
    endtime: str | None,
) -> tuple[Any, Callable[[], None] | None]:
    """Build a libmseed MS3Selections from optional filter arguments.

    Returns (selections_ptr, free_fn).  When all arguments are None, returns
    (ffi.NULL, None) so callers can pass the pointer directly without branching.

    The caller is responsible for invoking free_fn() after the selections are
    no longer needed (i.e. after the C read call).

    Args:
        sourceid: Source ID glob pattern, or None to match all (uses ``*``).
        starttime: Start time as a formatted string, or None for open start.
        endtime: End time as a formatted string, or None for open end.

    Raises:
        TypeError: If an argument is neither a str nor None.
        ValueError: If a time string cannot be parsed.
        MiniSEEDError: If ms3_addselect() returns an error.
    """
    if sourceid is None and starttime is None and endtime is None:
        return ffi.NULL, None

    if sourceid is not None:
        check_str("sourceid", sourceid)

    sidpattern = sourceid if sourceid is not None else "*"
    c_sidpattern = ffi.new("char[]", sidpattern.encode("utf-8"))

    def _time_value(time_string: str | None, name: str) -> int:
        if time_string is None:
            return int(clibmseed.NSTUNSET)
        try:
            return timestr2nstime(time_string)
        except ValueError as exc:
            raise ValueError(f"Invalid {name} time string: {time_string!r}") from exc

    start_ns = _time_value(starttime, "starttime")
    end_ns = _time_value(endtime, "endtime")

    ppselections = ffi.new("MS3Selections **")
    status = clibmseed.ms3_addselect(ppselections, c_sidpattern, start_ns, end_ns, 0)
    if status < 0:
        raise MiniSEEDError(status, "Error building selections")

    def _free() -> None:
        if ppselections[0] != ffi.NULL:
            clibmseed.ms3_freeselections(ppselections[0])
            ppselections[0] = ffi.NULL

    return ppselections[0], _free
