"""Test configuration for optional dependencies that doctests cannot guard.

Test functions skip themselves with ``pytest.importorskip()``; a doctest has no
such hook, so the ones needing an optional dependency are skipped from here.
"""

import importlib.util

import pytest

# Doctests that call the extra-header validation built on jsonschema-rs.  They
# are the primary examples for those methods, so they document the real call
# rather than wrapping it in the availability check used for the secondary
# numpy and pyarrow examples.
_NEEDS_JSONSCHEMA = {
    "pymseed.msrecord.MS3Record.valid_extra_headers",
    "pymseed.msrecord.MS3Record.validate_extra_headers",
    "pymseed.msrecord_validator.MS3RecordValidator",
}


def pytest_collection_modifyitems(items):
    """Skip the doctests that need jsonschema-rs when it is not installed."""
    if importlib.util.find_spec("jsonschema_rs") is not None:
        return

    skip = pytest.mark.skip(reason="jsonschema-rs not installed")

    for item in items:
        if item.name in _NEEDS_JSONSCHEMA:
            item.add_marker(skip)
