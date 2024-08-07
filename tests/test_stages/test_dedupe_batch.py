from cluo.core import Batch, Record
from cluo.stages import DedupeBatchStage


def test_dedupe():
    input_ = Batch(
        records=[
            Record(data={"a": "1", "b": "2", "c": 2}),
            Record(data={"a": "1", "b": "2", "c": 3}),
            Record(data={"a": "1", "b": "2", "c": 4}),
        ]
    )
    expected_output = Batch(records=[Record(data={"a": "1", "b": "2", "c": 4})])
    stage = DedupeBatchStage(["a", "b"])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)
