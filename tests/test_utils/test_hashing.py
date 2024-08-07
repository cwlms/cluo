import pytest

from cluo.utils.hashing import _prep_for_hash, hash_nested_primitive


@pytest.mark.parametrize(
    "input_",
    [
        [{"a": "b", 1: ["d", "z"]}, {"e": "f"}],
        [
            {"e": "f"},
            {1: ["z", "d"], "a": "b"},
        ],
    ],
)
def test_prep_for_hash_list(input_):
    actual = _prep_for_hash(input_)
    expected = [{"e": "f"}, {1: ["d", "z"], "a": "b"}]
    assert actual == expected


@pytest.mark.parametrize(
    "input_",
    [
        {"key1": {"a": "b", 1: ["d", "z"]}, "key2": {"e": "f"}},
        {"key2": {"e": "f"}, "key1": {1: ["z", "d"], "a": "b"}},
    ],
)
def test_prep_for_hash_dict(input_):
    actual = _prep_for_hash(input_)
    expected = {"key1": {1: ["d", "z"], "a": "b"}, "key2": {"e": "f"}}
    assert actual == expected


@pytest.mark.parametrize(
    "input_",
    [
        [{"a": "b", 1: ["d", "z"]}, {"e": "f"}],
        [
            {"e": "f"},
            {1: ["z", "d"], "a": "b"},
        ],
    ],
)
def test_hash_nested_primitive_list(input_):
    actual = hash_nested_primitive(input_)
    expected = "c07fb3bebf4e487e96d3177194c20c93"
    assert actual == expected


@pytest.mark.parametrize(
    "input_",
    [
        {"key1": {"a": "b", 1: ["d", "z"]}, "key2": {"e": "f"}},
        {"key2": {"e": "f"}, "key1": {1: ["z", "d"], "a": "b"}},
    ],
)
def test_hash_nested_primitive_dict(input_):
    actual = hash_nested_primitive(input_)
    expected = "808c2d4d8348d7d3c44c0bd76ceca199"
    assert actual == expected
