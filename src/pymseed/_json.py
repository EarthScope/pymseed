try:
    import orjson

    def json_loads(s):
        return orjson.loads(s)

    def json_dumps(obj):
        return orjson.dumps(obj)

    def json_dumps_minified(obj):
        return orjson.dumps(obj)

except ImportError:
    import json

    def json_loads(s):
        return json.loads(s)

    def json_dumps(obj):
        return json.dumps(obj).encode("utf-8")

    def json_dumps_minified(obj):
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")
