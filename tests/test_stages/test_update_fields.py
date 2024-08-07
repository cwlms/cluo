import pytest

from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import UpdateFieldsStage
from tests.conftest import parametrize_processes


@parametrize_processes(UpdateFieldsStage)
def test_call_with_values(processes):
    # all change
    stage = UpdateFieldsStage({"a": "b"}, processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"a": "b"})])
    assert new_batch.has_same_record_data(test_batch)

    # adding a field -- does not throw expection if key not found in record_data
    stage = UpdateFieldsStage({"a": "b", "c": "d"}, processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"a": "b", "c": "d"})])
    assert new_batch.has_same_record_data(test_batch)

    # none change
    stage = UpdateFieldsStage({}, processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"a": "c"})])
    assert new_batch.has_same_record_data(test_batch)


@parametrize_processes(UpdateFieldsStage)
def test_call_with_functions(processes):
    # all change
    stage = UpdateFieldsStage({"a": lambda x: 2 * x}, processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"a": "cc"})])
    assert new_batch.has_same_record_data(test_batch)

    # adding a field -- DOES throw expection if key not found in record_data when using callable
    stage = UpdateFieldsStage(
        {"a": lambda x: 2 * x, "c": lambda x: 2 * x},
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    with pytest.raises(KeyError):
        stage(Batch(records=[Record(data={"a": "c"})]))

    # none change
    stage = UpdateFieldsStage({}, processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"a": "c"})])
    assert new_batch.has_same_record_data(test_batch)
