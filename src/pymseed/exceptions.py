from typing import Optional

from .util import error_string


class MiniSEEDError(ValueError):
    """Exception for libmseed return values"""

    def __init__(self, status_code: int, message: Optional[str] = None) -> None:
        self.status_code = status_code
        self.message = message

    def __str__(self) -> str:
        library_message = error_string(self.status_code)

        if library_message is None:
            library_message = f"Unknown error code: {self.status_code}"

        return f"{library_message} {':: ' + self.message if self.message else ''}"


class NoSuchSourceID(ValueError):
    """Exception for non-existent trace source IDs"""

    def __init__(self, sourceid: str) -> None:
        self.sourceid = sourceid

    def __str__(self) -> str:
        return f"Source ID not found: {self.sourceid}"
