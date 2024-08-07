import pytest

from cluo.core import Batch, Record
from cluo.stages import TypecastStage
from tests.conftest import parametrize_processes


@parametrize_processes(TypecastStage)
def test_simple_cast(processes):
    input_ = Batch(records=[Record(data={"a": "1", "b": "2", "c": 3})])
    expected_output = Batch(records=[Record(data={"a": 1, "b": "2", "c": 3.0})])
    stage = TypecastStage({"a": int, "c": float}, processes=processes)
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)


@parametrize_processes(TypecastStage)
def test_error(processes):
    input_ = Batch(records=[Record(data={"a": "string"})])
    stage = TypecastStage({"a": int}, processes=processes)
    with pytest.raises(ValueError):
        stage(input_)


@parametrize_processes(TypecastStage)
def test_init_error(processes):
    with pytest.raises(ValueError):
        TypecastStage({"a": lambda x: x}, processes=processes)
