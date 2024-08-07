from cluo.core import Batch, Record
from cluo.stages import RenameFieldsStage
from tests.conftest import parametrize_processes


@parametrize_processes(RenameFieldsStage)
def test_call(processes):
    # all change
    s = RenameFieldsStage({"a": "b"}, processes=processes)
    new_batch = s(Batch(records=[Record(data={"a": "c"})]))
    test_batch = Batch(records=[Record(data={"b": "c"})])
    assert new_batch.has_same_record_data(test_batch)

    # some change
    s = RenameFieldsStage({"a": "b"}, processes=processes)
    new_batch = s(Batch(records=[Record(data={"a": "c", "z": "d"})]))
    test_batch = Batch(records=[Record(data={"b": "c", "z": "d"})])
    assert new_batch.has_same_record_data(test_batch)

    # all change with swap
    s = RenameFieldsStage({"a": "b", "b": "c"}, processes=processes)
    new_batch = s(Batch(records=[Record(data={"a": "c", "b": "d"})]))
    test_batch = Batch(records=[Record(data={"b": "c", "c": "d"})])
    assert new_batch.has_same_record_data(test_batch)

    # no change
    s = RenameFieldsStage({}, processes=processes)
    new_batch = s(
        Batch(records=[Record(data={"a": "c", "b": "d"}), Record(data={"q": 15})])
    )
    test_batch = Batch(
        records=[Record(data={"a": "c", "b": "d"}), Record(data={"q": 15})]
    )
    assert new_batch.has_same_record_data(test_batch)
