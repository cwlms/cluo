import pytest

from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import FieldHasherStage
from tests.conftest import parametrize_processes

pytestmark = [pytest.mark.field_hasher]


@parametrize_processes(FieldHasherStage)
def test_simple_all_fields(processes):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=None,
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
                }
            ),
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "diff_string",
                }
            ),
        ]
    )
    processed_batch = stage(test_batch)
    actual_hashes = [
        record.data[hash_destination] for record in processed_batch.records
    ]
    expected_hashes = [
        "e315bb2141c59393c90cf0aa0bb389ce",
        "c6d33855ba0e7d0981a7226dc1e6839a",
    ]
    assert actual_hashes == expected_hashes


@parametrize_processes(FieldHasherStage)
def test_simple_subset_of_fields_same_hash(processes):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=["int", "float"],
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
                }
            ),
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "diff_string",
                }
            ),
        ]
    )
    processed_batch = stage(test_batch)
    actual_hashes = [
        record.data[hash_destination] for record in processed_batch.records
    ]
    expected_hashes = [
        "91b426a1fd2a95dad5d0f0ca17b86ae6",
        "91b426a1fd2a95dad5d0f0ca17b86ae6",
    ]
    assert actual_hashes == expected_hashes


@parametrize_processes(FieldHasherStage)
def test_simple_subset_of_fields_diff_hash(processes):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=["int", "string"],
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
                }
            ),
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "string": "diff_string",
                }
            ),
        ]
    )
    processed_batch = stage(test_batch)
    actual_hashes = [
        record.data[hash_destination] for record in processed_batch.records
    ]
    expected_hashes = [
        "6469de0ccf82386a5e8de65f34faf02b",
        "ade0afeabfd068876438b06496115e1b",
    ]
    assert actual_hashes == expected_hashes


@parametrize_processes(FieldHasherStage)
def test_list_field(processes):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=None,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "list": ["string", 10],
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    assert (
        processed_batch.records[0].data[hash_destination]
        == "5fa4155f4b3a91410c9dc73baecc6f5b"
    )


@parametrize_processes(FieldHasherStage)
def test_nested(processes):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=None,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "nested": {"key": "value"},
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    assert (
        processed_batch.records[0].data[hash_destination]
        == "5917d0f6010a193fc868de444ce4962a"
    )


@parametrize_processes(FieldHasherStage)
def test_invalid(processes, capsys):
    hash_destination = "hash_dest"
    stage = FieldHasherStage(
        hash_destination=hash_destination,
        hash_fields=None,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )

    class SomeObj:
        def __repr__(self):
            raise TypeError()

    test_batch = Batch(
        records=[
            Record(
                data={
                    "int": 1,
                    "float": 10.0,
                    "invalid_data": SomeObj(),
                }
            )
        ]
    )
    with pytest.raises(TypeError):
        stage(test_batch)
