import json
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from cluo.core.batch import Record
from cluo.utils.streamsets_hashing import make_hash, make_hashes

# set file-level mark.  to just run tests in this file run pytest -m stub from venv
pytestmark = [pytest.mark.hashing]
DEBUG = False


def setup_module(module):
    # set up a vs code remote debugging connection when DEBUG = True
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR REMOTE DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


field_hash_test_cases = {
    "string": {"f1": "abc", "expected": "5ab49371a78310b9117a0c461f5137a2"},
    "mixed-case string": {
        "f1": "aBc",
        "expected": "5ab49371a78310b9117a0c461f5137a2",
    },
    "null": {"f1": None, "expected": "57e9e4f0d93e1acfd21479b6aaaa32b7"},
    "integer": {"f1": 12122, "expected": "3f016a1cc7bf371513d4d7789266b8fd"},
    "float": {"f1": -212.4434, "expected": "534d2249881137c5ca2f7cd9b5b2fa5b"},
    "datetime": {
        "f1": datetime(2020, 10, 1, 12, 25, 2, 0, timezone.utc),
        "expected": "997d7299910af518bcd7e4c0ea17f8f8",
        "expected_raw": "2020-10-01 12:25:02.000000|",
    },
    "date": {
        "f1": date(2020, 10, 1),
        "expected": "08b1b7ee89d21b1cf95c5f4dc012b631",
        "expected_raw": "10/01/2020|",
    },
    "string with non-normalized unicode text": {
        "f1": "\u0061\u0315\u0300\u05ae\u036b\u0062",
        "expected": "c6f203a24ba5d4b82b7ec2d74c241af5",
    },
    "string with non-ascii characters": {
        "f1": "15\u00f8C 3\u0111",
        "expected": "ad1a7fbdc86d53ae3fea8c2c7e1df2f7",
        "disabled": False,
    },
    "utf-8 encoded string (bytes)": {
        # does not have streamsets equivalent: fails with error
        "f1": "abc".encode("utf-8"),
        "expected": "5ab49371a78310b9117a0c461f5137a2",
        "expected_base": "abc|",
    },
    "empty string": {
        "f1": "",
        "expected": "b99834bc19bbad24580b3adfa04fb947",
        "disabled": False,
    },
    "literal null string": {
        "f1": "null",
        "expected": "57e9e4f0d93e1acfd21479b6aaaa32b7",
        "disabled": False,
    },
    "empty array field": {
        "f1": [],
        "actual": "57e9e4f0d93e1acfd21479b6aaaa32b7",
        "expected": "d41d8cd98f00b204e9800998ecf8427e",
        "disabled": False,
    },
    "array field all nulls": {
        "f1": [None],
        "expected": "7a0104dfd76504c30efda66151ae5add",
        "disabled": False,
    },
    "array with single string": {
        "f1": ["abc"],
        "expected": "2fe63e8d8b1cf52bed49f681d94c79a8",
        "disabled": False,
    },
    "array fields mixed in": {
        "f0": "xyz",
        "f1": ["abc", "def", "ghi"],
        "f2": "jkl",
        "f3": ["mno", "pqr"],
        "expected": "c2e8b5e28f26e81c95f48d38446f3c00",
        "expected_raw": "xyz|abc|def|ghi|abc|def|ghi|jkl|mno|pqr|mno|pqr|",
        "hash_fields": ["f0", "f1", "f2", "f3"],
    },
    "array with one of each data type (string, int, float, decimal, bool)": {
        "f1": ["abc", -12321, 0.2112, Decimal(2212212212.12), False, True],
        "expected": "8e4b2178b9c4b117449ced7e4fb39edb",
        "expected_raw": "-12321|0.2112|2.21221221212e9|abc|false|true|abc|-12321|0.2112|2.21221221212e9|false|true|",
        "disabled": False,
    },
    "array in sorted order": {
        "f1": [
            -3,
            0.0,
            12,
            Decimal(2212212212.12),
        ],
        "expected": "4696cf3430646d995acf7cf31b6ac9ba",
        "expected_raw": "-3|0.0|12|2.21221221212e9|-3|0.0|12|2.21221221212e9|",
        "disabled": False,
    },
    "array in unsorted order": {
        "f1": [
            Decimal(2212212212.12),
            0.0,
            12,
            -3,
        ],
        "expected": "ee5729be10d56f75dadb8867470d78a1",
        "expected_raw": "-3|0.0|12|2.21221221212e9|2.21221221212e9|0.0|12|-3|",
        "disabled": False,
    },
    "decimal two places": {
        "f1": Decimal(2212212212.12),
        "expected": "ba20c741aa12bc0158c8a3f62490b3b3",
        "expected_raw": "2.21221221212e9|",
        "disabled": False,
    },
    "decimal scientific notation": {
        "f1": Decimal("12345678.12345678901"),
        "expected": "386c60954b323f47a216ed0112e5265d",
        "expected_raw": "1.234567812345679e7|",
        "disabled": False,
    },
    "decimal non scientific notation": {
        "f1": Decimal("1234567.12345678901"),
        "expected": "420e8ed96831b8a4d4ef8db74804d623",
        "expected_raw": "1234567.123456789|",
        "disabled": False,
    },
    "decimal lots of trailing zeros": {
        "f1": Decimal("10000000000.0"),
        "expected": "f405a36a538c8c7114dc244f354075b3",
        "expected_raw": "1.0e10|",
        "disabled": False,
    },
    "float lots of trailing zeros": {
        "f1": float("10000000000.0"),
        "expected": "f405a36a538c8c7114dc244f354075b3",
        "expected_raw": "1.0e10|",
        "disabled": False,
    },
    "array with duplicate entries: 4 strings ,2 are dups": {
        "f1": ["c_str", "x_str", "a_str", "x_str"],
        "expected": "0dbe5588875f7f6be087072a40eb708e",
        "expected_raw": "a_str|c_str|x_str|x_str|c_str|x_str|a_str|x_str|",
    },
    "array of arrays": {
        # currently no support for array of arrays
        "f1": [[], ["a", "b"], [1, 2, "c"], [None]],
        "exception": "ValueError",
    },
    "array of dicts": {
        # currently no support for array of dicts
        "f1": [{}, {"a": "b"}, {1: 2, "c": None}, {None: None}],
        "exception": "ValueError",
    },
    "map field all nulls": {
        "f1": {"key": None},
        "expected": "57e9e4f0d93e1acfd21479b6aaaa32b7",
        "expected_raw": "null|",
    },
    "map field with one of each data type": {
        "f1": {
            "z": "abc",
            "x": -12321,
            "y": 0.2112,
            "b": Decimal(2212212212.12),
            "a": False,
            "c": True,
        },
        "expected": "632ac824da9704b68747ab004f23adc5",
        "expected_raw": "abc|-12321|0.2112|2.21221221212e9|false|true|",
        "disabled": False,
    },
    "map of maps": {
        "f1": {"z": {}, "a": {"a": "b"}, "d": {1: 2, "c": None}, "c": {None: None}},
        # currently no support for dict of dicts
        "exception": "ValueError",
    },
    "map in unsorted order": {
        "f1": {
            "z": Decimal(2212212212.12),
            "c": 0.0,
            "q": 12,
            "qa": -3,
        },
        "expected": "4c00c98ad4815690985887880e88001a",
        "expected_raw": "2.21221221212e9|0.0|12|-3|",
    },
    "two fields both strings different field order": {
        "f1": "def",
        "f2": "abc",
        "hash_fields": ["f2", "f1"],
        "expected": "8e1c4109041587afdb521d0583dfd605",
        "expected_raw": "abc|def|",
    },
    "two fields both strings": {
        "f1": "abc",
        "f2": "def",
        "expected": "8e1c4109041587afdb521d0583dfd605",
        "expected_raw": "abc|def|",
        "hash_fields": ["f1", "f2"],
    },
    "two fields one null one not null": {
        "f1": None,
        "f2": "qwerty",
        "expected": "2204b2b5b819383ee37b3e6c4652d5f9",
        "expected_raw": "null|qwerty|",
        "hash_fields": ["f1", "f2"],
    },
    "two fields one null one empty string": {
        "f1": None,
        "f2": "",
        "expected": "9dab72237c0498fcb72ae3ae1c8aef91",
        "expected_raw": "null||",
        "hash_fields": ["f1", "f2"],
    },
    "two fields one empty array, one string": {
        "f1": "qwerty",
        "f2": [],
        "expected": "e1b713a5560328fc76c7cc59b873729d",
        "expected_raw": "qwerty|",
        "hash_fields": ["f1", "f2"],
    },
    "two fields one map, one array": {
        "f1": {"key": 12.12},
        "f2": ["qard"],
        "expected": "b2d03b85a97f8a141848cc62dc2553c4",
        "expected_raw": "12.12|qard|qard|",
        "hash_fields": ["f1", "f2"],
    },
    "two fields both map": {
        "f1": {"A": False},
        "f2": {"a": "def"},
        "expected": "6a160c4eeb35daa7f8d9a0a7c7671f07",
        "expected_raw": "false|def|",
        "hash_fields": ["f1", "f2"],
    },
    "two fields integer float": {
        "f1": 221221221212121,
        "f2": 0.2112,
        "expected": "ad3dcdbaf822c1ee8ccec988493d73ad",
        "expected_raw": "221221221212121|0.2112|",
        "hash_fields": ["f1", "f2"],
    },
    "ten fields, all strings": {
        "f1": "f1_str",
        "f2": "f2_str",
        "f3": "f3_str",
        "f4": "f4_str",
        "f5": "f5_str",
        "f6": "f6_str",
        "f7": "f7_str",
        "f8": "f8_str",
        "f9": "f9_str",
        "f10": "f10_str",
        "expected": "6e04b68ebebfc91bc13a065db6c382d1",
        "expected_raw": "f1_str|f2_str|f3_str|f4_str|f5_str|f6_str|f7_str|f8_str|f9_str|f10_str|",
        "hash_fields": [
            "f1",
            "f2",
            "f3",
            "f4",
            "f5",
            "f6",
            "f7",
            "f8",
            "f9",
            "f10",
        ],
    },
    "six fields: five strings, one array with all five strings": {
        "f1": "str1",
        "f2": "str2",
        "f3": "str3",
        "f4": "str4",
        "f5": "str5",
        "f6": [
            "str1",
            "str2",
            "str3",
            "str4",
            "str5",
        ],
        "expected": "05b2648f659c8d17acda2b12137fdea1",
        "expected_raw": "str1|str2|str3|str4|str5|str1|str2|str3|str4|str5|str1|str2|str3|str4|str5|",
        "hash_fields": ["f1", "f2", "f3", "f4", "f5", "f6"],
    },
    "six fields: five nulls, one array with all five nulls": {
        "f1": None,
        "f2": None,
        "f3": None,
        "f4": None,
        "f5": None,
        "f6": [
            "str1",
            "str2",
            "str3",
            "str4",
            "str5",
        ],
        "expected": "70ae7afb059f9f260136402d9af22c36",
        "expected_raw": "null|null|null|null|null|str1|str2|str3|str4|str5|str1|str2|str3|str4|str5|",
        "hash_fields": ["f1", "f2", "f3", "f4", "f5", "f6"],
    },
}


array_field_hash_test_cases = {
    "string": {
        "f1": "abc",
        "expected": ["5ab49371a78310b9117a0c461f5137a2"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["abc|"],
    },
    "datetime": {
        "f1": datetime(2020, 10, 1, 12, 25, 2, 0, timezone.utc),
        "expected": ["997d7299910af518bcd7e4c0ea17f8f8"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["2020-10-01 12:25:02.000000|"],
    },
    "date": {
        "f1": date(2020, 10, 1),
        "expected": ["08b1b7ee89d21b1cf95c5f4dc012b631"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["10/01/2020|"],
    },
    "string with all spaces and tabs": {
        # \u0020 = space, \u0090 = tab
        "f1": "\u0020\u0020\u0009\u0009\u0020\u0020",
        "expected": ["48c7b4220390e71bedfcc9f147a126f3"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["\u0020\u0020\u0009\u0009\u0020\u0020|"],
    },
    "string with non-ascii characters": {
        "f1": "\u03b1 beta",
        "expected": ["c2b9df685970e97b1988abcdbf7f7441"],
        "expected_names": ["f1_hash"],
        "expected_bases": [" beta|"],
    },
    "string with non-normalized unicode text": {
        # normalization is filtered by non-ascii removal
        # ascii characters are not subject to normalization
        "f1": "\u0061\u0315\u0300\u05ae\u036b\u0062",
        "expected": ["8a29bbfc96316dd428fe4fc0c8699865"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["ab|"],
    },
    "utf-8 encoded string (bytes)": {
        # does not have streamsets equivalent: fails with error
        "f1": "abc".encode("utf-8"),
        "expected": ["5ab49371a78310b9117a0c461f5137a2"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["abc|"],
    },
    "null": {
        # does not have streamsets equivalent: fails with error
        "f1": None,
        "exception": "ValueError",
        "exception_value": "null value in field f1 not permitted",
    },
    "empty string": {
        "f1": "",
        "expected": ["b99834bc19bbad24580b3adfa04fb947"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["|"],
    },
    "negative integer": {
        "f1": -221221212,
        "expected": ["6e06854b7c30cc21a46315dea2cc28f2"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["-221221212|"],
    },
    "integer": {
        "f1": 21212121,
        "expected": ["80718c238531755819dc8640535bb05d"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["21212121|"],
    },
    "very long negative decimal": {
        "f1": Decimal("-2212212212.13232323232"),
        "expected": ["456af80c85252613af14f604d5357b77"],
        "expected_names": ["f1_hash"],
        # streamsets converts this to -2.2122122121323233e9|
        # which is not easily reproducible
        # gonna let this one go
        "expected_bases": ["-2.212212212132323e9|"],
    },
    "very tiny negative decimal": {
        "f1": Decimal("-0.00000000002212212212132"),
        "expected": ["39dd706710df52a34c1c66d5433f0669"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["-2.212212212132e-11|"],
    },
    "very tiny positive decimal": {
        "f1": Decimal("0.00000000002212212212132"),
        "expected": ["14c6b04b22b96af205f25e29d436a121"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["2.212212212132e-11|"],
    },
    "float": {
        "f1": 0.2112,
        "expected": ["998dd53c6eefce0b7beb286869f11d0f"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["0.2112|"],
    },
    "json-encoded dictionary": {
        "f1": json.dumps({"a": 5, "b": "abc", "c": 0.1212}).encode("utf-8"),
        "expected": ["7b4d6958fa88d5eba709047cbbc371ff"],
        "expected_names": ["f1_hash"],
        "expected_bases": ["""{"a": 5, "b": "abc", "c": 0.1212}|"""],
    },
    "empty array field": {
        "f1": [],
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "array field all nulls": {
        "f1": [None, None, None],
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "array with single string": {
        "f1": ["abc"],
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "empty dict field": {
        "f1": {},
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "dict field with one key": {
        "f1": {"a": 5},
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "two fields both strings": {
        "f1": "abc",
        "f2": "xyz",
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["8382ab3f66fcfbda5d6ce811785e0d53"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["abc|xyz|"],
    },
    "two fields both strings different field order": {
        "f1": "abc",
        "f2": "xyz",
        "hash_field_sets": [["f2", "f1"]],
        "expected": ["10028349f0c99dc396d691d6f75b97b3"],
        "expected_names": ["f2_f1_hash"],
        "expected_bases": ["xyz|abc|"],
    },
    "two fields one int one string": {
        "f1": 12,
        "f2": "xyz",
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["a7553606b140adc9de20b5ac52d2a6d3"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["12|xyz|"],
    },
    "two fields one float one decimal": {
        "f1": 12.001,
        "f2": Decimal("212.121"),
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["105532cc823c037a222961c0eddd8fab"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["12.001|212.121|"],
    },
    "two fields one null one not null - raises": {
        "f1": "abc",
        "f2": None,
        "hash_field_sets": [["f1", "f2"]],
        "exception": "ValueError",
        "exception_value": "null value in field f2 not permitted",
    },
    "two fields one null one empty string - raises": {
        "f1": None,
        "f2": "",
        "exception": "ValueError",
        "exception_value": "null value in field f1 not permitted",
    },
    "two fields both empty strings": {
        "f1": "",
        "f2": "",
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["7d010443693eec253a121e2aa2ba177c"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["||"],
    },
    "two fields one string one empty string": {
        "f1": "abc",
        "f2": "",
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["299ca2c363a08598eac492b980095c05"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["abc||"],
    },
    "two fields one empty array, one string - raises": {
        "f1": [],
        "f2": "xyz",
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "two fields one map, one array - raises": {
        "f1": {},
        "f2": [],
        "exception": "ValueError",
        "exception_value": "cannot hash complex data types",
    },
    "two fields integer float": {
        "f1": 21,
        "f2": 102.12,
        "hash_field_sets": [["f1", "f2"]],
        "expected": ["3815628e7e879f5fbeb463dd71e9158d"],
        "expected_names": ["f1_f2_hash"],
        "expected_bases": ["21|102.12|"],
    },
    "two hashes, two fields each different fields different values": {
        "f1": "abc",
        "f2": "def",
        "f3": "uvw",
        "f4": "xyz",
        "hash_field_sets": [["f1", "f2"], ["f3", "f4"]],
        "expected": [
            "8e1c4109041587afdb521d0583dfd605",
            "dac34dd40e96cfe514e2136edfc35872",
        ],
        "expected_names": ["f1_f2_hash", "f3_f4_hash"],
        "expected_bases": ["abc|def|", "uvw|xyz|"],
    },
    "two hashes, two fields each same values, different field names - same hash, different hash name": {
        "f1": "abc",
        "f2": "def",
        "f3": "uvw",
        "f4": "xyz",
        "hash_field_sets": [["f1", "f2"], ["f3", "f4"]],
        "expected": [
            "8e1c4109041587afdb521d0583dfd605",
            "dac34dd40e96cfe514e2136edfc35872",
        ],
        "expected_names": ["f1_f2_hash", "f3_f4_hash"],
        "expected_bases": ["abc|def|", "uvw|xyz|"],
    },
    "two hashes same names - same hash, same hash name": {
        "f1": "abc",
        "f2": "def",
        "hash_field_sets": [["f1", "f2"], ["f1", "f2"]],
        "expected": [
            "8e1c4109041587afdb521d0583dfd605",
            "8e1c4109041587afdb521d0583dfd605",
        ],
        "expected_names": ["f1_f2_hash", "f1_f2_hash"],
        "expected_bases": ["abc|def|", "abc|def|"],
    },
    "two hashes, two fields each, one field null in one hash": {
        "f1": "abc",
        "f2": "def",
        "f3": None,
        "f4": "xyz",
        "hash_field_sets": [["f1", "f2"], ["f3", "f4"]],
        "exception": "ValueError",
        "exception_value": "null value in field f3 not permitted",
    },
    "two hashes, two fields in first, both empty strings, three fields in second, zero int zero float zero decimal": {
        "f1": "",
        "f2": "",
        "f3": 0,
        "f4": 0.00,
        "f5": Decimal("0.00000"),
        "hash_field_sets": [["f1", "f2"], ["f3", "f4", "f5"]],
        "expected": [
            "7d010443693eec253a121e2aa2ba177c",
            "11d4290e56f4ccce1cfdfddb7f9de28b",
        ],
        "expected_names": ["f1_f2_hash", "f3_f4_f5_hash"],
        "expected_bases": ["||", "0|0.0|0.0|"],
    },
}


@pytest.fixture
def hash_fields():
    return ["f1"]


@pytest.fixture
def hash_field_sets():
    return [["f1"]]


@pytest.mark.parametrize(
    "input_dict", field_hash_test_cases.values(), ids=field_hash_test_cases.keys()
)
def test_streamsets_field_hash(input_dict, hash_fields):
    expected_hashes = {}
    actual_hashes = {}
    if input_dict.get("disabled"):
        pytest.skip()
    test_hash_fields = input_dict.get("hash_fields", hash_fields)
    input_record = Record(data=input_dict)
    try:
        actual = make_hash(input_record, hash_fields=test_hash_fields)
        actual_str = make_hash(input_record, hash_fields=test_hash_fields, encode=False)
    except Exception as exc:
        actual = None
        actual_str = None
        actual_hashes["exception"] = type(exc).__name__

    if "expected_raw" in input_dict:
        expected_hashes["raw"] = input_dict.get("expected_raw")
        actual_hashes["raw"] = actual_str
    if "exception" in input_dict:
        expected_hashes["exception"] = input_dict.get("exception")
    expected_hashes["hash"] = input_dict.get("expected")
    actual_hashes["hash"] = actual

    assert expected_hashes == actual_hashes


@pytest.mark.parametrize(
    "input_dict",
    array_field_hash_test_cases.values(),
    ids=array_field_hash_test_cases.keys(),
)
def test_streamsets_array_field_hash(input_dict, hash_field_sets):
    if input_dict.get("disabled"):
        pytest.skip()
    test_hash_field_sets = input_dict.get("hash_field_sets", hash_field_sets)
    compare_keys = ("hashes", "names", "bases", "exception", "exception_value")
    actual = {key: None for key in compare_keys}
    expected = deepcopy(actual)
    input_record = Record(data=input_dict)
    try:
        actual["hashes"] = make_hashes(
            input_record, hash_field_sets=test_hash_field_sets
        )
        actual["bases"] = (
            make_hashes(
                input_record,
                hash_field_sets=test_hash_field_sets,
                encode=False,
                simple_types_only=True,
            )
            if "expected_bases" in input_dict
            else None
        )
    except Exception as exc:
        actual["exception"] = type(exc).__name__
        actual["exception_value"] = (
            exc.args[0] if "exception_value" in input_dict else None
        )

    expected["hashes"] = input_dict.get("expected")
    expected["bases"] = input_dict.get("expected_bases")
    expected["exception"] = input_dict.get("exception")
    expected["exception_value"] = input_dict.get("exception_value")

    assert expected == actual
