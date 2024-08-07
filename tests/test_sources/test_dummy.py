from cluo.core.batch import Batch, Record
from cluo.sources import DummySource
from tests.conftest import parametrize_processes


@parametrize_processes(DummySource)
def test_call(processes):
    dummy_source = DummySource(
        record=Record({"a": 1, "b": 2, "c": 3}), batch_size=2, processes=processes
    )
    with dummy_source:
        assert dummy_source(Batch([])).has_same_record_data(
            Batch([Record({"a": 1, "b": 2, "c": 3}), Record({"a": 1, "b": 2, "c": 3})])
        )
