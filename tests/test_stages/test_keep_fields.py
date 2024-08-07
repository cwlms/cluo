from cluo.core import Batch, Record
from cluo.stages import KeepFieldsStage
from tests.conftest import parametrize_processes


@parametrize_processes(KeepFieldsStage)
def test_call(processes):
    s = KeepFieldsStage(["a", "b"], processes=processes)
    test_batch = Batch(records=[Record(data={"a": 1, "b": 2, "c": 3})])
    ref_batch = Batch(records=[Record(data={"a": 1, "b": 2})])
    processed_batch = s(test_batch)
    for processed_record, ref_record in zip(processed_batch.records, ref_batch.records):
        assert processed_record.data == ref_record.data
