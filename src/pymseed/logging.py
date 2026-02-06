"""
Logging capture for libmseed error and warning messages.

This module provides facilities to capture libmseed error and warning messages
using the logging registry, instead of having them printed to stderr/stdout.
"""

import atexit
import threading
from typing import Optional

from .clib import clibmseed, ffi

# Maximum messages to store in the registry
DEFAULT_MAX_MESSAGES = 10

# Track whether atexit cleanup has been registered
_atexit_registered_clear_error_messages = False

# Thread-local storage for keeping prefix strings alive
_thread_local_prefixes = threading.local()


def configure_logging(
    log_prefix: Optional[str] = None,
    error_prefix: Optional[str] = None,
    max_messages: int = DEFAULT_MAX_MESSAGES,
) -> None:
    """
    Configure libmseed logging for the current thread.

    This function can be called from any thread to configure its logging
    parameters. When called from threads forked from the main thread, each
    thread can have its own logging configuration.

    Args:
        log_prefix: Prefix for log messages. None uses libmseed default.
        error_prefix: Prefix for error/diagnostic messages. None uses libmseed default.
        max_messages: Maximum number of messages to store in the registry.
            When the registry is full, oldest messages are discarded.

    Example:
        >>> from pymseed import configure_logging
        >>> configure_logging(log_prefix="[LOG] ", error_prefix="[ERR] ")
    """
    global _atexit_registered_clear_error_messages

    # Convert prefixes to C strings or NULL and store in thread-local storage to prevent
    # Python's garbage collector from freeing them. The C library stores pointers to these
    # strings, so they must remain valid for the lifetime of the thread's logging configuration.
    if log_prefix is None:
        c_log_prefix = ffi.NULL
        _thread_local_prefixes.log_prefix = None
    else:
        _thread_local_prefixes.log_prefix = log_prefix.encode("utf-8")
        c_log_prefix = _thread_local_prefixes.log_prefix

    if error_prefix is None:
        c_error_prefix = ffi.NULL
        _thread_local_prefixes.error_prefix = None
    else:
        _thread_local_prefixes.error_prefix = error_prefix.encode("utf-8")
        c_error_prefix = _thread_local_prefixes.error_prefix

    # Initialize with NULL print functions to suppress console output
    clibmseed.ms_rloginit(ffi.NULL, c_log_prefix, ffi.NULL, c_error_prefix, max_messages)

    # Register cleanup at exit (only once)
    if not _atexit_registered_clear_error_messages:
        atexit.register(clear_error_messages)
        _atexit_registered_clear_error_messages = True


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
    messages = []
    # MAX_LOG_MSG_LENGTH is 200 in libmseed
    max_len = 200
    message_buf = ffi.new("char[]", max_len)

    while True:
        length = clibmseed.ms_rlog_pop(ffi.NULL, message_buf, max_len, 0)
        if length <= 0:
            break
        message = ffi.string(message_buf).decode("utf-8").rstrip("\n")
        messages.append(message)

    return messages
