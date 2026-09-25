from typing import Any

try:
    import orjson

    def json_loads(s: str | bytes) -> Any:
        return orjson.loads(s)

    def json_dumps_minified(obj: Any) -> bytes:
        return orjson.dumps(obj)

except ImportError:
    import json

    def json_loads(s: str | bytes) -> Any:
        return json.loads(s)

    def json_dumps_minified(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")
