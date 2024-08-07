from cluo.core import Batch, Record


def test_batch_has_same_data_same_data():
    batch_1 = Batch(records=[Record(data={"a": 1})])
    batch_2 = Batch(records=[Record(data={"a": 1})])
    batch_3 = Batch(records=[Record(data={"a": 2})])
    batch_4 = Batch(records=[Record(data={"a": 1, "b": 2})])
    batch_5 = Batch(
        records=[
            Record(data={"a": 1}),
            Record(data={"a": 1}),
        ]
    )

    assert batch_1.has_same_record_data(batch_2)
    assert not batch_1.has_same_record_data(batch_3)
    assert not batch_1.has_same_record_data(batch_4)
    assert not batch_1.has_same_record_data(batch_5)


def test_batch_max_offset():
    offset_field = "id"
    batch_1 = Batch(
        records=[
            Record(data={offset_field: 1, "value": "test"}),
            Record(data={offset_field: 2, "value": 100}),
        ]
    )
    batch_2 = Batch(
        records=[
            Record(data={offset_field: 1}),
            Record(data={offset_field: 10}),
            Record(data={offset_field: 2}),
        ]
    )
    batch_3 = Batch(
        records=[
            Record(data={offset_field: "a"}),
            Record(data={offset_field: "b"}),
            Record(data={offset_field: "c"}),
        ]
    )
    batch_4 = Batch(
        records=[
            Record(data={offset_field: "2020-01-03T00:00:00"}),
            Record(data={offset_field: "2020-01-04T00:00:00"}),
            Record(data={offset_field: "2020-01-04T01:00:00"}),
        ]
    )
    batch_5 = Batch(records=[])

    assert batch_1.max_offset(offset_field) == 2
    assert batch_2.max_offset(offset_field) == 10
    assert batch_3.max_offset(offset_field) == "c"
    assert batch_4.max_offset(offset_field) == "2020-01-04T01:00:00"
    assert batch_5.max_offset(offset_field) is None


def test_batch_dedupe():
    batch_1 = Batch(
        records=[
            Record(data={"a": 1, "b": 2, "c": "Some text"}),
            Record(data={"a": 1, "b": 2, "c": "Some updated text"}),
            Record(data={"a": 2, "b": 2, "c": "Some text"}),
            Record(data={"a": 2, "b": 2, "c": "Some updated text"}),
        ]
    ).deduplicate(["a", "b"])
    batch_2 = Batch(
        records=[
            Record(data={"a": 1, "b": 2, "c": "Some text"}),
            Record(data={"a": 1, "b": 2, "c": "Some updated text"}),
            Record(data={"a": 2, "b": 2, "c": "Some text"}),
            Record(data={"a": 2, "b": 2, "c": "Some updated text"}),
        ]
    ).deduplicate(["a", "b"], True)
    batch_1_deduped = Batch(
        records=[
            Record(data={"a": 1, "b": 2, "c": "Some updated text"}),
            Record(data={"a": 2, "b": 2, "c": "Some updated text"}),
        ]
    )
    batch_2_deduped = Batch(
        records=[
            Record(data={"a": 1, "b": 2, "c": "Some text"}),
            Record(data={"a": 2, "b": 2, "c": "Some text"}),
        ]
    )

    assert batch_1.records[0].data == batch_1_deduped.records[0].data
    assert batch_1.records[1].data == batch_1_deduped.records[1].data
    assert batch_2.records[0].data == batch_2_deduped.records[0].data
    assert batch_2.records[1].data == batch_2_deduped.records[1].data
