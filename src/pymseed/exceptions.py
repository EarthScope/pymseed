from .clib import clibmseed
from .logging import get_error_messages
from .util import error_string


class PymseedError(RuntimeError):
    """Base class for all pymseed exceptions.

    Inherits from :class:`RuntimeError` because most concrete pymseed errors
    describe runtime/I/O/data conditions — bad CRC, unexpected end of file,
    wrong record length, libmseed allocation failure, missing trace ID — none
    of which are :class:`ValueError`\\ s in the Python-stdlib sense
    ("right type, wrong value").

    Concrete pymseed errors (:class:`MiniSEEDError`, :class:`NoSuchSourceID`,
    …) all derive from this class. Future additions should also inherit
    from it so callers do not have to grow ``except (A, B, C, …)`` tuples.
    """


class MiniSEEDError(PymseedError):
    """Exception for libmseed return values."""

    status_code: int
    message: str | None
    error_messages: list[str]
    _rendered: str

    def __init__(self, status_code: int, message: str | None = None) -> None:
        super().__init__(status_code, message)
        self.status_code = status_code
        self.message = message

        # Drain libmseed's per-thread log registry for generic errors so the
        # exception carries the underlying diagnostic context. Must run
        # BEFORE _render() because the renderer reads `self.error_messages`.
        if status_code == clibmseed.MS_GENERROR:
            self.error_messages = get_error_messages()
        else:
            self.error_messages = []

        # Cache the rendered description once.
        self._rendered = self._render()

    def __str__(self) -> str:
        return self._rendered

    def _render(self) -> str:
        # For generic errors, use captured error messages if available
        if self.status_code == clibmseed.MS_GENERROR and self.error_messages:
            library_message = "; ".join(self.error_messages)
        else:
            library_message = (
                error_string(self.status_code) or f"Unknown error code: {self.status_code}"
            )

        if self.message:
            return f"{library_message} :: {self.message}"
        return library_message


class NoSuchSourceID(PymseedError):
    """Exception for non-existent trace source IDs."""

    sourceid: str

    def __init__(self, sourceid: str) -> None:
        super().__init__(sourceid)
        self.sourceid = sourceid

    def __str__(self) -> str:
        return f"Source ID not found: {self.sourceid}"
