"""
Logging capture for libmseed error and warning messages.

.. note::
    This module is **not** an adapter for the standard-library
    :mod:`logging` package. It is a thin wrapper over libmseed's own
    internal message registry (``ms_rloginit`` / ``ms_rlog_pop`` /
    ``ms_rlog_free``). The names overlap, but the APIs and concepts do
    not — ``configure_logging`` here does not configure a
    :class:`logging.Logger`, and the messages it captures are not
    :class:`logging.LogRecord` instances. Users wanting bridge libmseed
    messages into stdlib logging should drain
    :func:`get_error_messages` and forward each string themselves.

This module provides facilities to capture libmseed error and warning
messages using libmseed's logging registry, instead of having them printed
to stderr/stdout.

The three public entry points are also re-exported from the top-level
:mod:`pymseed` package, so the typical usage is::

    from pymseed import configure_logging, get_error_messages

rather than importing from ``pymseed.logging`` directly.
"""

import atexit
import threading

from .clib import clibmseed, ffi

# Maximum messages to store in the registry
DEFAULT_MAX_MESSAGES = 10

# Track whether atexit cleanup has been registered
_atexit_registered_clear_error_messages = False

# Thread-local storage for keeping prefix strings alive
_thread_local_prefixes = threading.local()

# Arguments of the most recent configure_logging() call, applied to threads that
# have not configured themselves.  Replaced as a whole tuple so readers on other
# threads always see a consistent set.
_inherited_config: tuple[str | None, str | None, int] = (None, None, DEFAULT_MAX_MESSAGES)

# Reused pop buffer per thread (ms_rlog_pop requires a stable char[] each call)
_thread_local_rlog_pop_buf = threading.local()

# MAX_LOG_MSG_LENGTH in libmseed (logging.c)
_MAX_RLOG_MSG_LEN = 200


def configure_logging(
    log_prefix: str | None = None,
    error_prefix: str | None = None,
    max_messages: int = DEFAULT_MAX_MESSAGES,
) -> None:
    """
    Configure libmseed logging for the current thread.

    This function can be called from any thread to configure its logging
    parameters. Each :class:`threading.Thread` can hold its own log prefixes
    and message registry; calls from different threads do not interfere.

    The arguments are also remembered as the configuration for threads that
    never call this function themselves; pymseed applies it to each thread on
    first use, so message capture works on any thread without setup.

    Per-thread isolation requires libmseed to be built with thread-local
    storage (the default — see ``logging.c`` ``lm_thread_local`` selection).
    When libmseed is built with ``LIBMSEED_NO_THREADING`` defined, all threads
    share a single global log registry and the last :func:`configure_logging`
    call wins process-wide.

    Args:
        log_prefix: Prefix for log messages. None uses libmseed default.
        error_prefix: Prefix for error/diagnostic messages. None uses libmseed default.
        max_messages: Maximum number of warning/error messages to store in
            message registry. When the registry is full, oldest messages
            are discarded.  A value of 0 disables the registry.

    Raises:
        ValueError: If ``max_messages`` is negative.

    Example:
        >>> from pymseed import configure_logging
        >>> configure_logging(log_prefix="[LOG] ", error_prefix="[ERR] ")
    """
    if max_messages < 0:
        raise ValueError(f"max_messages must be >= 0; got {max_messages}. ")

    global _atexit_registered_clear_error_messages, _inherited_config

    # Encode the new prefixes into local bytes objects first. libmseed stores
    # the prefix by pointer rather than by copy (see logging.c: `logp->logprefix
    # = logprefix;`), so we must keep these bytes alive for the lifetime of the
    # thread's logging configuration by pinning them in thread-local storage.
    #
    # IMPORTANT: do NOT swap the new bytes into TLS until AFTER ms_rloginit has
    # updated libmseed's pointer. Otherwise, the assignment would drop the only
    # remaining reference to the previous prefix bytes (CPython frees them
    # immediately) while libmseed still points at them — opening a brief
    # use-after-free window. By computing the new bytes in locals, calling
    # ms_rloginit to switch libmseed's pointer first, and only then rotating
    # TLS, the previous prefix bytes outlive libmseed's reference to them.
    new_log_prefix_bytes: bytes | None = None if log_prefix is None else log_prefix.encode("utf-8")
    new_error_prefix_bytes: bytes | None = (
        None if error_prefix is None else error_prefix.encode("utf-8")
    )

    c_log_prefix = ffi.NULL if new_log_prefix_bytes is None else new_log_prefix_bytes
    c_error_prefix = ffi.NULL if new_error_prefix_bytes is None else new_error_prefix_bytes

    # Initialize with NULL print functions to suppress console output.
    clibmseed.ms_rloginit(ffi.NULL, c_log_prefix, ffi.NULL, c_error_prefix, max_messages)

    # Safe to rotate TLS now: libmseed's pointer is on the NEW bytes; dropping
    # the previous TLS reference can no longer leave a dangling pointer.
    _thread_local_prefixes.log_prefix = new_log_prefix_bytes
    _thread_local_prefixes.error_prefix = new_error_prefix_bytes
    _thread_local_prefixes.configured = True

    _inherited_config = (log_prefix, error_prefix, max_messages)

    # Register cleanup at exit (only once)
    if not _atexit_registered_clear_error_messages:
        atexit.register(clear_error_messages)
        _atexit_registered_clear_error_messages = True


def ensure_thread_logging() -> None:
    """Configure libmseed logging for the calling thread if not already done.

    libmseed's message registry is thread-local: an unconfigured thread prints
    diagnostics to stderr instead of storing them, so ``get_error_messages()``
    returns nothing and callers relying on it (notably
    :class:`~pymseed.MS3RecordValidator`) lose all message text.  pymseed calls
    this at the entry point of every operation that can produce messages.

    Applies the arguments of the most recent :func:`configure_logging` call.
    """
    if getattr(_thread_local_prefixes, "configured", False):
        return

    configure_logging(*_inherited_config)


def clear_error_messages() -> int:
    """
    Clear all log messages from the registry without returning them.

    Returns:
        The number of messages that were cleared.
    """
    return clibmseed.ms_rlog_free(ffi.NULL)


def get_error_messages() -> list[str]:
    """
    Get all error/warning messages from the libmseed logging registry.

    Messages are popped from the registry and returned as a list of strings.
    After calling this function, the registry will be empty.

    Returns:
        A list of error/warning message strings. Empty list if no messages.
    """
    buf = getattr(_thread_local_rlog_pop_buf, "buf", None)
    if buf is None:
        buf = ffi.new("char[]", _MAX_RLOG_MSG_LEN)
        _thread_local_rlog_pop_buf.buf = buf

    messages: list[str] = []
    while True:
        length = clibmseed.ms_rlog_pop(ffi.NULL, buf, _MAX_RLOG_MSG_LEN, 0)
        if length <= 0:
            break
        messages.append(ffi.unpack(buf, length).decode("utf-8", errors="replace").rstrip("\n"))

    return messages
