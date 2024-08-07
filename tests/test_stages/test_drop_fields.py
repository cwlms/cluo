from cluo.core import Batch, Record
from cluo.stages import DropFieldsStage
from tests.conftest import parametrize_processes


@parametrize_processes(DropFieldsStage)
def test_call(processes):
    stage = DropFieldsStage(["a", "b"], processes=processes)
    new_batch = stage(Batch(records=[Record(data={"a": 1, "b": 2, "c": 3})]))
    ref_batch = Batch(records=[Record(data={"c": 3})])
    for test_record, ref_record in zip(new_batch.records, ref_batch.records):
        assert test_record.data == ref_record.data
