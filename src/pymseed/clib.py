"""
Core library interface for pymseed using CFFI

"""

from typing import Any, Optional

try:
    # This is the correct pattern: import the ffi and lib objects
    # directly FROM the compiled _libmseed_cffi module.
    from ._libmseed_cffi import ffi
    from ._libmseed_cffi import lib as clibmseed  # noqa: F401

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


def cdata_to_string(cdata: Any, encoding: str = "utf-8") -> Optional[str]:
    """
    Convert C string to Python string.  If the C string is NULL, return None.

    Args:
        cdata: CFFI cdata char*
        encoding: String encoding

    Returns:
        Python string
    """
    if cdata == ffi.NULL:
        return None
    else:
        return str(ffi.string(cdata).decode(encoding))
