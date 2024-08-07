from cluo.core.batch import Batch, Record
from cluo.stages import IdentityStage
from tests.conftest import parametrize_processes


@parametrize_processes(IdentityStage)
def test_call(processes):
    stage = IdentityStage(processes=processes)
    test_batch = Batch(records=[Record(data={"k": 1}), Record(data={"k2": 2})])
    ref_batch = Batch(records=[Record(data={"k": 1}), Record(data={"k2": 2})])
    processed_batch = stage(test_batch)
    for processed_record, ref_record in zip(processed_batch.records, ref_batch.records):
        assert processed_record.data == ref_record.data
