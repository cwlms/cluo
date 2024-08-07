from cluo.core.batch import Batch, Record
from cluo.stages import ValidateUrlStage
from tests.conftest import parametrize_processes


@parametrize_processes(ValidateUrlStage)
def test_call(processes):
    input_data = {
        "good_fb1": "https://www.facebook.com/",
        "good_fb2": "https://facebook.com/",
        "bad_fb1": "facebook.com/",
        "bad_fb2": "facebook.asldasdoij",
        "bad_fb3": 10,
    }
    stage = ValidateUrlStage(fields=set(input_data.keys()), processes=processes)
    input_ = Batch(records=[Record(data=input_data)])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "https://www.facebook.com/",
                    "good_fb2": "https://facebook.com/",
                    "bad_fb1": None,
                    "bad_fb2": None,
                    "bad_fb3": None,
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)
