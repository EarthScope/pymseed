"""
Core library interface for pymseed using CFFI

"""

from typing import Any

__all__ = [
    "ffi",
    "clibmseed",
    "buffer_pointer",
    "cdata_to_string",
    "owned_memoryview",
    "owning_memoryview",
]

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


def buffer_pointer(buffer: Any, *, writable: bool = False, context: str = "") -> Any:
    """
    Bind `buffer` for C access, rejecting one that cannot be used.

    The exception ffi.from_buffer() raises for an unusable buffer depends on the
    interpreter and on the exporter: CPython reports read-only storage as
    BufferError, PyPy as TypeError, and numpy raises ValueError of its own.
    memoryview classifies the buffer the same way everywhere, so ask it first and
    the contract does not vary.

    Args:
        buffer: Object supporting the buffer protocol
        writable: Require storage that C code may write into
        context: Prefix for the BufferError message, naming the operation

    Returns:
        CFFI char[] bound to the memory of `buffer`, whose len() is its size in
        bytes.

    Raises:
        ValueError: If `buffer` does not support the buffer protocol
        BufferError: If `buffer` is not C-contiguous, or is read-only when
            `writable` is set
    """
    try:
        with memoryview(buffer) as view:
            readonly, contiguous = view.readonly, view.c_contiguous
    except TypeError:
        raise ValueError("Buffer must support the buffer protocol") from None

    prefix = f"{context}: " if context else ""

    if not contiguous:
        raise BufferError(f"{prefix}buffer is not C-contiguous")

    if writable and readonly:
        raise BufferError(f"{prefix}buffer is read-only")

    return ffi.from_buffer(buffer, require_writable=writable)


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


def owning_memoryview(ptr: Any, nbytes: int) -> memoryview:
    """
    Return a memoryview of `nbytes` at `ptr` that frees the memory itself.

    Unlike `owned_memoryview()`, which keeps an existing owner alive, this is
    for a buffer that libmseed allocated and no longer tracks: `ptr` must have
    come from `libmseed_memory.malloc`/`realloc`, since the destructor releases
    it with `libmseed_memory.free`. Used to hand a decoded sample buffer to
    Python without copying it, once the caller has detached it from libmseed's
    own structures.

    Passing `nbytes` to `ffi.gc()` as its `size` hint lets interpreters that
    don't refcount, e.g. PyPy, weigh this allocation properly when deciding
    when to collect it.

    Args:
        ptr: CFFI pointer to the first element, from libmseed's allocator
        nbytes: Length of the memory at `ptr`

    Returns:
        A memoryview of the elements at `ptr`, owning that memory.
    """
    freed = ffi.gc(ptr, clibmseed.libmseed_memory.free, size=nbytes)

    return memoryview(ffi.buffer(freed, nbytes))


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
