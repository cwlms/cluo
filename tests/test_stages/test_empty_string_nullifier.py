from cluo.core.batch import Batch, Record
from cluo.stages import EmptyStringNullifierStage
from tests.conftest import parametrize_processes


@parametrize_processes(EmptyStringNullifierStage)
def test_call(processes):
    stage = EmptyStringNullifierStage(processes=processes)
    input_ = Batch(records=[Record(data={"a": "string", "b": "", "c": " "})])
    expected = Batch(records=[Record(data={"a": "string", "b": None, "c": None})])
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(EmptyStringNullifierStage)
def test_empty(processes):
    stage = EmptyStringNullifierStage(processes=processes)
    input_ = Batch(records=[Record(data={"a": "string", "b": None, "c": " "})])
    expected = Batch(records=[Record(data={"a": "string", "b": None, "c": None})])
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(EmptyStringNullifierStage)
def test_non_string(processes):
    stage = EmptyStringNullifierStage(processes=processes)
    input_ = Batch(records=[Record(data={"a": "string", "b": 1, "c": " "})])
    expected = Batch(records=[Record(data={"a": "string", "b": 1, "c": None})])
    actual = stage(input_)
    assert actual.has_same_record_data(expected)
