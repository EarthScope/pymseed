"""
Core library interface for pymseed using CFFI

"""

from typing import Any

__all__ = ["ffi", "clibmseed", "cdata_to_string", "owned_memoryview"]

try:
    # This is the correct pattern: import the ffi and lib objects
    # directly FROM the compiled _libmseed_cffi module.
    from ._libmseed_cffi import ffi
    from ._libmseed_cffi import lib as clibmseed

except ImportError as exc:
    # The friendly error message is still a good idea.
    # The 'from exc' part preserves the original traceback for debugging.
    raise ImportError(
        "Could not import the CFFI-based C extension module.\n"
        "This is likely because the package is not installed correctly.\n"
        "Please make sure the package is installed, for example by running:\n"
        "  pip install .\n"
        "or for development:\n"
        "  pip install -e ."
    ) from exc


def owned_memoryview(ptr: Any, nbytes: int, format: str, owner: Any) -> memoryview:
    """
    Return a memoryview of `nbytes` at `ptr` that keeps `owner` alive.

    ffi.buffer() keeps only the cdata alive, and a cast pointer does not own the
    memory it addresses, so a view built from one can outlive whatever frees
    that memory.  Holding `owner` in the destructor closure of an ffi.gc()
    handle gives the view a reference to it, deferring the release until the
    view is gone.

    Args:
        ptr: CFFI pointer to the first element
        nbytes: Length of the memory at `ptr`
        format: Element format for the returned view, as `memoryview.cast()`
        owner: Object whose lifetime governs the memory at `ptr`

    Returns:
        A memoryview of the elements at `ptr`.
    """
    pinned = ffi.gc(ptr, lambda _ptr, _owner=owner: None)

    return memoryview(ffi.buffer(pinned, nbytes)).cast(format)


def cdata_to_string(cdata: Any, encoding: str = "utf-8") -> str | None:
    """
    Convert C string to Python string.  If the C string is NULL, return None.

    Args:
        cdata: CFFI cdata char*
        encoding: String encoding

    Returns:
        Python string, or None if `cdata` is NULL.
    """
    if not cdata:
        return None
    else:
        return ffi.string(cdata).decode(encoding)
