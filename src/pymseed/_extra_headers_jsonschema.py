"""JSON Schema validation for miniSEED3 extra headers (optional jsonschema-rs)."""

from __future__ import annotations

from typing import Any

_IMPORT_ERROR_MESSAGE = (
    "jsonschema-rs is not installed. Install jsonschema-rs or this package "
    "with the [jsonschema] optional dependency"
)


def validator_for_extra_headers_schema(schema: dict[str, Any]) -> Any:
    """Return a reusable validator for *schema* (Draft 2020-12 via ``$schema``)."""
    try:
        import jsonschema_rs
    except ImportError:
        raise ImportError(_IMPORT_ERROR_MESSAGE) from None
    return jsonschema_rs.validator_for(schema)
