"""JSON Schema validation for miniSEED3 extra headers (optional jsonschema-rs)."""

from __future__ import annotations

import functools
from importlib.resources import files
from typing import Any

from ._json import json_loads

_IMPORT_ERROR_MESSAGE = (
    "jsonschema-rs is not installed. Install jsonschema-rs or this package "
    "with the [jsonschema] optional dependency"
)

# Returned by load_extra_headers_validator() when jsonschema-rs is unavailable,
# so callers can tell a missing optional dependency from a broken schema.
JSONSCHEMA_MISSING = "jsonschema-rs not installed"

# Maps supported schema IDs to their bundled JSON Schema filenames
KNOWN_SCHEMAS: dict[str, str] = {
    "FDSN-v1.0": "ExtraHeaders-FDSN-v1.0.schema-2020-12.json",
}


def validator_for_extra_headers_schema(schema: dict[str, Any]) -> Any:
    """Return a reusable validator for *schema* (Draft 2020-12 via ``$schema``)."""
    try:
        import jsonschema_rs
    except ImportError:
        raise ImportError(_IMPORT_ERROR_MESSAGE) from None
    return jsonschema_rs.validator_for(schema)


@functools.cache
def load_extra_headers_validator(schema_id: str) -> tuple[Any, str | None]:
    """Load (and cache) the validator for the bundled schema ``schema_id``.

    Returns ``(validator, None)`` on success, or ``(None, error_message)``
    on any failure. The result is cached at the module level via
    :func:`functools.cache` so the bundled-schema read + JSON parse +
    jsonschema-rs validator compile happens at most once per process per
    schema. Both success and failure outcomes are cached, so a broken
    install reports the same descriptive error without retrying the
    failing load on every record.

    Tests that need to exercise distinct loading outcomes for the same
    ``schema_id`` should call ``load_extra_headers_validator.cache_clear()``
    between scenarios.
    """
    try:
        schema_bytes = files("pymseed.schemas").joinpath(KNOWN_SCHEMAS[schema_id]).read_bytes()
        validator = validator_for_extra_headers_schema(json_loads(schema_bytes))
        return validator, None
    except ImportError:
        return None, JSONSCHEMA_MISSING
    except (FileNotFoundError, OSError) as e:
        return None, f"bundled schema file unavailable: {e}"
    except Exception as e:
        # Covers json_loads failure, jsonschema-rs validator-construction
        # errors, etc. — any of these means the install is broken.
        return None, f"failed to load JSON schema: {type(e).__name__}: {e}"
