import pytest

from cluo.core.batch import Batch, Record
from cluo.stages import CleanUrlStage
from tests.conftest import parametrize_processes


@parametrize_processes(CleanUrlStage)
def test_default_format(processes):
    input_data = {
        "good_fb1": "     http://www.facebook.com/",
        "good_fb2": "https://facebook.com/path",
        "good_fb3": "https://facebook.com/profile/?mobile=true",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()),
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "http://www.facebook.com",
                    "good_fb2": "https://facebook.com/path",
                    "good_fb3": "https://facebook.com/profile?mobile=true",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_protocol_exclusion(processes):
    input_data = {
        "good_fb1": "http://www.facebook.com/?query=1",
        "good_fb2": "https://facebook.com/path?query=1",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()),
        include_protocol=False,
        include_query_string=True,
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "www.facebook.com?query=1",
                    "good_fb2": "facebook.com/path?query=1",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_subdomain_exclusion(processes):
    input_data = {
        "good_fb1": "http://www.facebook.com/",
        "good_fb2": "https://subdomain.facebook.com/path?query=1",
        "good_fb3": "https://facebook.com?query=1",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()), processes=processes, include_subdomain=False
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "http://facebook.com",
                    "good_fb2": "https://facebook.com/path?query=1",
                    "good_fb3": "https://facebook.com?query=1",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_path_exclusion(processes):
    input_data = {
        "good_fb1": "https://www.facebook.com/?query=1",
        "good_fb2": "https://facebook.com/path/",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()),
        include_path=False,
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "https://www.facebook.com",
                    "good_fb2": "https://facebook.com",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_null_option(processes):
    input_data = {
        "not_url": "not a url",
        "almost_url1": "http://almosturl",
        "almost_url2": "http://almosturl.",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()),
        null_if_invalid=True,
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(data={"not_url": None, "almost_url1": None, "almost_url2": None})
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_base_domain(processes):
    input_data = {
        "good_fb1": "https://www.subdomain.facebook.com/path?query=1",
        "good_fb2": "facebook.com",
        "good_fb3": "facebook.com/",
    }
    input_record = Record(data=input_data)
    stage = CleanUrlStage(
        fields=set(input_data.keys()),
        include_protocol=False,
        include_subdomain=False,
        include_path=False,
        include_query_string=False,
        null_if_invalid=False,
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "good_fb1": "facebook.com",
                    "good_fb2": "facebook.com",
                    "good_fb3": "facebook.com",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(CleanUrlStage)
def test_protocol_exception(processes):
    with pytest.raises(Exception):
        input_data = {"bad_fb1": "facebook.com"}
        input_record = Record(data=input_data)
        stage = CleanUrlStage(
            fields=set(input_data.keys()),
            include_protocol=True,
            null_if_invalid=False,
            processes=processes,
        )
        input_ = Batch(records=[input_record])
        stage.process_record(input_record)
        stage(input_)


@parametrize_processes(CleanUrlStage)
def test_domain_exception(processes):
    with pytest.raises(Exception):
        input_data = {"bad_fb1": "facebookcom"}
        input_record = Record(data=input_data)
        stage = CleanUrlStage(
            fields=set(input_data.keys()),
            include_protocol=False,
            null_if_invalid=False,
            processes=processes,
        )
        input_ = Batch(records=[input_record])
        stage.process_record(input_record)
        stage(input_)
