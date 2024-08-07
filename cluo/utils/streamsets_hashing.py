import hashlib
import re
from copy import deepcopy
from datetime import date
from decimal import Decimal
from typing import Any, List, Optional
from unicodedata import normalize

from cluo.core import Record


def _strip_leading_trailing_zeroes(dig_str: str) -> str:
    re_subs = [
        (
            # leading trailing zeroes non-zero number
            r"^(-?)0*(\d+(?:\.(?:(?!0+$)\d)+)?)0*",
            r"\1\2",
        ),
        (
            # leading trailing zeroes zero number
            r"(-?)(?:0+)?(0)(\.)(0?)(?:0+)?",
            r"\1\2\3\4",
        ),
    ]
    for re_sub in re_subs:
        dig_str = re.sub(re_sub[0], re_sub[1], dig_str)
    return dig_str


def _reformat_scientific_string(sci_str: str) -> str:
    re_sub = (
        r"^(-?[0-9]\.)([0-9]{1,}?)([1-9]?)(0*)(E)(\+?)(\-?)([0-9]+)$",
        r"\1\2\3e\7\8",
    )
    return re.sub(re_sub[0], re_sub[1], sci_str)


def _decimal_to_string(value: Decimal | float):
    if ((abs(Decimal(value)) < 1e7) and (abs(Decimal(value)) > 1e-7)) or (value == 0.0):
        conv = "{:8.16}".format(value).strip()
        conv = _strip_leading_trailing_zeroes(conv)
        return conv
    conv = "{:8.15E}".format(value).strip()
    # remove trailing zeros from scientific notation
    conv = _reformat_scientific_string(conv)
    return conv


def _normalize_string_value(value: str) -> str:
    # normalization is squashed by ascii filter
    value = normalize("NFD", value)
    # filter out ascii characters
    value = re.sub(r"[^\x00-\x7f]", "", value)
    return value


def _is_simple_data_type(value: Any) -> bool:
    return isinstance(value, (int, float, str, bool, bytes, Decimal)) or value is None


def _hash_list(
    value: Any,
    top_level: bool = True,
    from_dict: bool = False,
    top_only: bool = False,
    normalize_ascii: bool = False,
    simple_types_only: bool = False,
) -> str:
    if top_level:
        # top level list should NOT be sorted
        return "".join(
            [
                _prep_for_hash(
                    raw,
                    top_level=False,
                    normalize_ascii=normalize_ascii,
                    simple_types_only=simple_types_only,
                )
                for raw in value
            ]
        )
    elif top_only:
        raise ValueError("cannot hash complex data types")
    if all(_is_simple_data_type(v) for v in value) and len(value) > 0:
        # array fields must be sorted before appending
        # array fields must be all simple data type
        unsorted_array = [
            _prep_for_hash(
                raw,
                top_level=False,
                normalize_ascii=normalize_ascii,
                simple_types_only=simple_types_only,
            )
            for raw in value
        ]
        sorted_array_source = "".join(
            sorted(deepcopy(unsorted_array), key=lambda x: str(x))
        )
        unsorted_array_source = "".join(unsorted_array)
        if from_dict:
            return unsorted_array_source
        return sorted_array_source + unsorted_array_source
    elif len(value) == 0:
        # empty arrays are equivalent to empty strings in streamsets version
        return ""
    else:
        raise ValueError("list evaluated to unexpected value: cannot hash")


def _simple_type_prep(
    value: Any,
    normalize_ascii: bool = False,
) -> str:
    if value is None:
        # java null variable string repr
        return "null|"
    elif isinstance(value, Decimal):
        return f"{_decimal_to_string(value)}|"
    elif isinstance(value, float):
        return f"{_decimal_to_string(value)}|"
    elif isinstance(value, date):
        if hasattr(value, "timestamp"):
            # python timestamp
            return f"{value.strftime('%Y-%m-%d %H:%M:%S.%f')}|"
        else:
            # python date
            return f"{value.strftime('%m/%d/%Y')}|"
    elif _is_simple_data_type(value):
        # simple data types are ok at all levels
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        value = (
            _normalize_string_value(value)
            if normalize_ascii and isinstance(value, str)
            else value
        )
        return f"{str(value).lower()}|"
    else:
        # all other object types are nulled out
        return "null|"


def _prep_for_hash(
    value: Any,
    top_level: bool = True,
    simple_types_only: bool = False,
    normalize_ascii: bool = False,
) -> str:
    if isinstance(value, list):
        return _hash_list(
            value,
            top_level=top_level,
            top_only=simple_types_only,
            normalize_ascii=normalize_ascii,
            simple_types_only=simple_types_only,
        )
    elif isinstance(value, dict):
        # convert dict of values to array first
        return _hash_list(
            value.values(),
            top_level=False,
            from_dict=True,
            top_only=simple_types_only,
            normalize_ascii=normalize_ascii,
            simple_types_only=simple_types_only,
        )
    else:
        return _simple_type_prep(value, normalize_ascii)


def hash_nested_primitive(
    values: List[Any] | dict[Any, Any],
    simple_types_only: bool = False,
    normalize_ascii: bool = False,
) -> str:
    return str(
        hashlib.md5(
            _prep_for_hash(
                values,
                simple_types_only=simple_types_only,
                normalize_ascii=normalize_ascii,
            ).encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()
    )


def _get_hashable_values(
    record: Record, hash_fields: Optional[List[str]], allow_nulls: bool = True
) -> list[Any]:
    hash_fields = hash_fields or record.data.keys()
    # check for nulls
    if not allow_nulls:
        for k in hash_fields:
            if record.data.get(k) is None:
                raise ValueError(f"null value in field {k} not permitted")
    # hash all fields in the order presented in the hash_fields list
    return [record.data.get(k) for k in hash_fields]


def make_hash(
    record: Record,
    hash_fields: Optional[List[str]],
    simple_types_only: bool = False,
    encode: bool = True,
    normalize_ascii: bool = False,
    allow_nulls: bool = True,
) -> str:
    hashable_values = _get_hashable_values(record, hash_fields, allow_nulls)
    if encode:
        return hash_nested_primitive(
            hashable_values,
            simple_types_only=simple_types_only,
            normalize_ascii=normalize_ascii,
        )
    else:
        return _prep_for_hash(
            hashable_values,
            simple_types_only=simple_types_only,
            normalize_ascii=normalize_ascii,
        )


def make_hashes(
    record: Record,
    hash_field_sets: List[List[str]],
    simple_types_only: bool = True,
    normalize_ascii: bool = True,
    encode: bool = True,
    allow_nulls: bool = False,
) -> List[Optional[str]]:
    return [
        make_hash(
            record, hash_fields, simple_types_only, encode, normalize_ascii, allow_nulls
        )
        if hash_fields
        else None
        for hash_fields in hash_field_sets
    ]
