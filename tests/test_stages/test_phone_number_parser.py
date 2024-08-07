from cluo.core import Batch, Record
from cluo.stages import PhoneNumberParserStage
from tests.conftest import parametrize_processes


@parametrize_processes(PhoneNumberParserStage)
def test_call_single_field(processes):
    stage = PhoneNumberParserStage({"a"}, processes=processes)
    input_ = Batch(records=[Record(data={"a": "+15555555555", "b": 2, "c": 3})])
    expected = Batch(records=[Record(data={"a": "+15555555555", "b": 2, "c": 3})])
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(PhoneNumberParserStage)
def test_call_multiple_fields(processes):
    stage = PhoneNumberParserStage({"a", "b"}, processes=processes)
    input_ = Batch(
        records=[Record(data={"a": "+15555555555", "b": "+1 (555) 555-5555", "c": 3})]
    )
    expected = Batch(
        records=[Record(data={"a": "+15555555555", "b": "+15555555555", "c": 3})]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(PhoneNumberParserStage)
def test_global(processes):
    phone_numbers = {
        "india": "+91 1234-123456",  # with country code
        "us1": "+1 (234) 123-4567",  # with country code
        "us2": "(234) 123-4567",  # without country code
        "uk": "+44 7911 123456",  # with country code
        "uk2": "7911 123456",  # without country code --> will default to US, even though wrong
    }

    stage = PhoneNumberParserStage(
        set(phone_numbers.keys()),
        on_error="default",
        processes=processes,
    )
    input_ = Batch(records=[Record(data=phone_numbers)])
    expected = Batch(
        records=[
            Record(
                data={
                    "india": "+911234123456",
                    "us1": "+12341234567",
                    "us2": "+12341234567",
                    "uk": "+447911123456",
                    "uk2": "+17911123456",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(PhoneNumberParserStage)
def test_different_input_types(processes):
    phone_numbers = {"us_string": "1234123456", "us_int": 1234123456}

    stage = PhoneNumberParserStage(
        set(phone_numbers.keys()),
        processes=processes,
    )
    input_ = Batch(records=[Record(data=phone_numbers)])
    expected = Batch(
        records=[Record(data={"us_int": "+11234123456", "us_string": "+11234123456"})]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(PhoneNumberParserStage)
def test_none_on_error(processes):
    stage = PhoneNumberParserStage(
        {"a", "b", "c"}, processes=processes, on_error="default"
    )
    input_ = Batch(records=[Record(data={"a": None, "b": "123", "c": 321})])
    expected = Batch(records=[Record(data={"a": None, "b": None, "c": None})])
    actual = stage(input_)
    assert actual.has_same_record_data(expected)
