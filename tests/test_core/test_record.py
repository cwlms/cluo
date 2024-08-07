from cluo.core import Record


def test_record_initialization():
    new_record = Record()

    assert hasattr(new_record, "data")
    assert isinstance(new_record.data, dict)
