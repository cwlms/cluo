import hashlib
import json
from typing import Any


def _is_jsonable(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, OverflowError):
        return False


def _prep_for_hash(value: Any) -> Any:
    """
    Recursively sorts dictionaries and lists to ensure consistent hash.
    Makes reasonable efforts to ensure objects are serializable
    """
    if isinstance(value, list):
        return sorted([_prep_for_hash(v) for v in value], key=lambda x: str(x))
    elif isinstance(value, dict):
        unsorted = {k: _prep_for_hash(v) for k, v in value.items()}
        return dict(sorted(unsorted.items(), key=lambda kv_tup: str(kv_tup[0])))
    elif not _is_jsonable(value):
        return str(value)
    else:
        return value


def hash_nested_primitive(values: list[Any] | dict[Any, Any]) -> str:
    """Hashes (MD5) nested (dict, list) of primitives.

    Args:
        values (list[Any] | dict[Any, Any]): Object to hash.

    Returns:
        str: MD5 hash.
    """
    return str(
        hashlib.md5(  # noqa: S324
            json.dumps(_prep_for_hash(values)).encode("utf-8")
        ).hexdigest()
    )
