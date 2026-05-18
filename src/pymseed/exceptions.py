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
    """Exception for libmseed return values"""

    def __init__(self, status_code: int, message: str | None = None) -> None:
        super().__init__(status_code, message)
        self.status_code = status_code
        self.message = message

        # Capture error messages from libmseed registry for generic errors
        if status_code == clibmseed.MS_GENERROR:
            self.error_messages = get_error_messages()
        else:
            self.error_messages = []

    def __str__(self) -> str:
        # For generic errors, use captured error messages if available
        if self.status_code == clibmseed.MS_GENERROR and self.error_messages:
            library_message = "; ".join(self.error_messages)
        else:
            library_message = error_string(self.status_code)
            if library_message is None:
                library_message = f"Unknown error code: {self.status_code}"

        if self.message:
            return f"{library_message} :: {self.message}"
        return library_message


class NoSuchSourceID(PymseedError):
    """Exception for non-existent trace source IDs"""

    def __init__(self, sourceid: str) -> None:
        super().__init__(sourceid)
        self.sourceid = sourceid

    def __str__(self) -> str:
        return f"Source ID not found: {self.sourceid}"
