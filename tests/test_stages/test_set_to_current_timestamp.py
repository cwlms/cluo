import datetime

from freezegun import freeze_time

from cluo.core.batch import Batch, Record
from cluo.stages import SetToCurrentTimestampStage
from tests.conftest import parametrize_processes

FROZEN_TIMESTAMP = datetime.datetime(2022, 1, 20, 16, 42, 27, 263843)


@freeze_time(FROZEN_TIMESTAMP)
@parametrize_processes(SetToCurrentTimestampStage)
def test_call_new_fields(processes):
    fields = ["b", "c"]

    stage = SetToCurrentTimestampStage(fields=fields, processes=processes)
    test_batch = Batch(records=[Record(data={"a": 1}), Record(data={"a": 2})])
    processed_batch = stage(test_batch)

    for record in processed_batch.records:
        for field in fields:
            assert record.data[field] == FROZEN_TIMESTAMP


@freeze_time(FROZEN_TIMESTAMP)
@parametrize_processes(SetToCurrentTimestampStage)
def test_call_existing_fields(processes):
    fields = ["a"]

    stage = SetToCurrentTimestampStage(fields=fields, processes=processes)
    test_batch = Batch(records=[Record(data={"a": 1}), Record(data={"a": 2})])
    processed_batch = stage(test_batch)

    for record in processed_batch.records:
        for field in fields:
            assert record.data[field] == FROZEN_TIMESTAMP
