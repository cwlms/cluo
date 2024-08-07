from copy import deepcopy

import pytest

from cluo.core import Batch, Record
from cluo.stages import PythonCallableStage
from tests.conftest import parametrize_processes

batch1 = Batch(records=[Record(data={"a": 1, "b": 2})])
batch2 = Batch(records=[Record(data={"a": 1, "b": 2, "c": 3})])


def process_record(record: Record) -> Record:
    record.data["c"] = 3
    return record


def process_batch(batch: Batch) -> Batch:
    batch.records = [process_record(record) for record in batch.records]
    return batch


@parametrize_processes(PythonCallableStage)
def test_process_record(processes):
    s = PythonCallableStage(process_record=process_record, processes=processes)
    result_batch = s(deepcopy(batch1))
    for result, test_record in zip(result_batch.records, batch2.records):
        assert result.data == test_record.data


@parametrize_processes(PythonCallableStage)
def test_process_batch(processes):
    s = PythonCallableStage(process_batch=process_batch, processes=processes)
    result_batch = s(deepcopy(batch1))
    for result, test_record in zip(result_batch.records, batch2.records):
        assert result.data == test_record.data


@parametrize_processes(PythonCallableStage)
def test_two_callables(processes):
    with pytest.raises(ValueError):
        PythonCallableStage(
            process_record=process_record,
            process_batch=process_batch,
            processes=processes,
        )


@parametrize_processes(PythonCallableStage)
def test_no_callables(processes):
    with pytest.raises(ValueError):
        PythonCallableStage(processes=processes)
