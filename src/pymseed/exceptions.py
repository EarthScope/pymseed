from typing import Any

from .clib import clibmseed
from .logging import get_error_messages
from .util import error_string


class PymseedError(RuntimeError):
    """Base class for all pymseed exceptions.

    Inherits from :class:`RuntimeError` because most concrete pymseed errors
    describe runtime/I/O/data conditions — bad CRC, unexpected end of file,
    wrong record length, libmseed allocation failure — none of which are
    :class:`ValueError`\\ s in the Python-stdlib sense ("right type, wrong
    value").

    Concrete pymseed errors (:class:`MiniSEEDError`, …) all derive from
    this class. Future additions should also inherit from it so callers do
    not have to grow ``except (A, B, C, …)`` tuples.
    """


class MiniSEEDError(PymseedError):
    """Exception for libmseed return values.

    ``status_code`` is the raw libmseed return value. Negative values are error
    codes; the meaning of a positive value depends on the function that
    produced it, so callers passing one supply their own ``message``.
    """

    status_code: int
    message: str | None
    error_messages: list[str]
    _rendered: str

    def __init__(self, status_code: int, message: str | None = None) -> None:
        super().__init__(status_code, message)
        self.status_code = status_code
        self.message = message

        # Drain libmseed's per-thread log registry so the exception carries
        # the underlying diagnostic context. Every pymseed entry point clears
        # the registry before it runs (see logging.begin_operation()), so
        # what's drained here belongs to the operation that raised this error.
        # Must run BEFORE _render() because the renderer reads this for
        # MS_GENERROR. Skipped for MS_NOERROR, which is never raised as an
        # error but is a valid status_code value.
        if status_code != clibmseed.MS_NOERROR:
            self.error_messages = get_error_messages()
        else:
            self.error_messages = []

        # Cache the rendered description once.
        self._rendered = self._render()

    def __str__(self) -> str:
        return self._rendered

    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Support pickling without draining the unpickling thread's registry.

        The default reduction re-runs ``__init__(status_code, message)``,
        which would drain whatever happens to be in the registry on the
        thread that unpickles this exception. Restore the already-rendered
        state instead.
        """
        return (_restore_miniseed_error, (self.status_code, self.message, self.error_messages))

    def _render(self) -> str:
        library_message: str | None
        # For generic errors, use captured error messages if available
        if self.status_code == clibmseed.MS_GENERROR and self.error_messages:
            library_message = "; ".join(self.error_messages)
        elif self.status_code < 0:
            library_message = error_string(self.status_code)
        else:
            # A non-negative status has no error string: its meaning depends on
            # the producing function, and the values collide (MS_ENDOFFILE and
            # msr3_parse()'s "one more byte needed" are both 1).
            library_message = None

        if library_message is None:
            return self.message or f"Unknown status code: {self.status_code}"

        if self.message:
            return f"{library_message} :: {self.message}"
        return library_message


def _restore_miniseed_error(
    status_code: int, message: str | None, error_messages: list[str]
) -> MiniSEEDError:
    """Rebuild a :class:`MiniSEEDError` from pickled state without touching
    the current thread's libmseed log registry. See ``__reduce__``."""
    exc = MiniSEEDError.__new__(MiniSEEDError)
    Exception.__init__(exc, status_code, message)
    exc.status_code = status_code
    exc.message = message
    exc.error_messages = error_messages
    exc._rendered = exc._render()
    return exc
