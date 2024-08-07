import pytest

from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import ArrayFieldHasherStage
from tests.conftest import parametrize_processes

pytestmark = [pytest.mark.array_field_hasher]


@parametrize_processes(ArrayFieldHasherStage)
def test_call_defaults(processes):
    stage = ArrayFieldHasherStage(
        hash_fields=[
            ["float", "string"],
            ["int", "float"],
            ["string", "none"],
            ["none"],
        ],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "string",
                    "none": None,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    expected_hashes = [
        "7fc97fdec51b2f21bb1b113454b24563",
        "91b426a1fd2a95dad5d0f0ca17b86ae6",
    ]
    assert processed_batch.records[0].data["array_hash"] == expected_hashes
    assert processed_batch.records[0].data["array_hash_name"] == [
        "float_string_hash",
        "int_float_hash",
    ]


@parametrize_processes(ArrayFieldHasherStage)
def test_streamsets_hashes_no_nulls(processes):
    stage = ArrayFieldHasherStage(
        hash_fields=[
            ["float", "string"],
            ["int", "float"],
            ["string", "none"],
            ["none"],
        ],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
        streamsets_compatible=True,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "string",
                    "none": None,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    expected_hashes = [
        "21b8ceb502b7ad99fd67f78030dbdaeb",
        "44d71dde5d3586d690f74383660f9392",
    ]
    assert processed_batch.records[0].data["array_hash"] == expected_hashes
    assert processed_batch.records[0].data["array_hash_name"] == [
        "float_string_hash",
        "int_float_hash",
    ]


@parametrize_processes(ArrayFieldHasherStage)
def test_call_non_defaults(processes):
    hash_destination = "hash_destination"
    hash_name_destination = "hash_name_destination"
    stage = ArrayFieldHasherStage(
        hash_fields=[["float", "string"], ["int", "float"], ["int", "float", "none"]],
        hash_destination=hash_destination,
        hash_name_destination=hash_name_destination,
        hash_nulls=True,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "string",
                    "none": None,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    expected_hashes = [
        "7fc97fdec51b2f21bb1b113454b24563",
        "91b426a1fd2a95dad5d0f0ca17b86ae6",
        "88c253b4a04cbd6b2f8da70843d86d1a",
    ]
    assert processed_batch.records[0].data[hash_destination] == expected_hashes
    assert processed_batch.records[0].data[hash_name_destination] == [
        "float_string_hash",
        "int_float_hash",
        "int_float_none_hash",
    ]


@parametrize_processes(ArrayFieldHasherStage)
def test_streamsets_hashes_allow_nulls(processes):
    hash_destination = "hash_destination"
    hash_name_destination = "hash_name_destination"
    stage = ArrayFieldHasherStage(
        hash_fields=[
            ["float", "string"],
            ["int", "float"],
            ["int", "float", "none"],
            ["none"],
        ],
        hash_destination=hash_destination,
        hash_name_destination=hash_name_destination,
        hash_nulls=True,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
        streamsets_compatible=True,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "string",
                    "none": None,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    expected_hashes = [
        "21b8ceb502b7ad99fd67f78030dbdaeb",
        "44d71dde5d3586d690f74383660f9392",
        "fd30203771f246fbf22a241b713187f1",
        "57e9e4f0d93e1acfd21479b6aaaa32b7",
    ]
    assert processed_batch.records[0].data[hash_destination] == expected_hashes
    assert processed_batch.records[0].data[hash_name_destination] == [
        "float_string_hash",
        "int_float_hash",
        "int_float_none_hash",
        "none_hash",
    ]
