"""Helpers for the parts of finalization that vary by interpreter."""

import gc
import sys

import pytest

REFCOUNTED = sys.implementation.name == "cpython"

requires_refcounting = pytest.mark.skipif(
    not REFCOUNTED,
    reason="release without the cyclic collector needs reference counting",
)

requires_buffer_export_lock = pytest.mark.skipif(
    not REFCOUNTED,
    reason="refusing to resize a buffer while it is exported is CPython-specific",
)


def collect_until(predicate, attempts=5):
    """Collect until `predicate` holds, reporting whether it ever did.

    PyPy runs one level of finalizers per pass, so releasing a chain of them
    takes more than the single collection CPython needs.

    Args:
        predicate: Condition to test after each collection
        attempts: Most collections to run

    Returns:
        True if `predicate` held within `attempts` collections.
    """
    for _ in range(attempts):
        gc.collect()
        if predicate():
            return True

    return False


def assert_released(reference, attempts=5):
    """Assert that `reference`, a weakref to a released object, reads as None."""
    assert collect_until(lambda: reference() is None, attempts), (
        f"still referenced after {attempts} collections"
    )
