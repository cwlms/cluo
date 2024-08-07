from cluo.core import Batch, Record
from cluo.stages import BranchStage

batch_1 = Batch(
    records=[
        Record(data={"key": 12}),
        Record(data={"key": 13}),
        Record(data={"key": 14}),
    ]
)


def test_fall_through_branch_call_is_identity():
    branch_stage = BranchStage(
        channel_mapping={
            "key_less_than_13": lambda x: x.data["key"] < 13,
            "key_is_13": lambda x: x.data["key"] == 13,
            "key_greater_than_13": lambda x: True,
        },
        single_channel=False,
        name="test_fall_through",
    )
    new_batch = branch_stage(batch_1)
    for in_record, out_record in zip(batch_1.records, new_batch.records):
        assert in_record.data == out_record.data


def test_branch_single_channel_map():
    branch_stage = BranchStage(
        channel_mapping={
            "key_less_than_13": lambda x: x.data["key"] < 13,
            "key_is_13": lambda x: x.data["key"] == 13,
            "key_greater_than_eq_13": lambda x: x.data["key"] >= 13,
        },
        single_channel=True,
        name="test_fall_through",
    )
    assert branch_stage.get_channels(batch_1.records[0]) == ["key_less_than_13"]
    assert branch_stage.get_channels(batch_1.records[1]) == ["key_is_13"]
    assert branch_stage.get_channels(batch_1.records[2]) == ["key_greater_than_eq_13"]


def test_branch_multi_channel_map():
    branch_stage = BranchStage(
        channel_mapping={
            "key_less_than_13": lambda x: x.data["key"] < 13,
            "key_is_13": lambda x: x.data["key"] == 13,
            "key_greater_than_eq_13": lambda x: x.data["key"] >= 13,
        },
        single_channel=False,
        name="test_fall_through",
    )
    assert branch_stage.get_channels(batch_1.records[0]) == ["key_less_than_13"]
    assert branch_stage.get_channels(batch_1.records[1]) == [
        "key_is_13",
        "key_greater_than_eq_13",
    ]
    assert branch_stage.get_channels(batch_1.records[2]) == ["key_greater_than_eq_13"]
