import pytest

from cluo.core import ErrorHandlingMethod
from cluo.core.batch import Batch, Record
from cluo.stages import SocialMediaUrlCleanerStage
from tests.conftest import parametrize_processes

pytestmark = [
    pytest.mark.social_media_url_cleaner,
]


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call_3(processes):
    input_data = {
        "linkedin_url": "http://www.linkedin.com/pub/rafael-barroso/20/56a/84b"
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={"linkedin_url": "linkedin"},
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[Record(data={"linkedin_url": "linkedin.com/pub/rafael-barroso"})]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call(processes):
    input_data = {
        "twitter_url": "https://twitter.com/rbarroso78",
        "linkedin_url": "http://www.linkedin.com/pub/rafael-barroso/20/56a/84b",
        "facebook_url": "https://www.facebook.com/rafabaruv",
        "angellist_url": "https://angel.co/rafael-barroso",
        "pitchbook_url": "https://my.pitchbook.com/profile/rafael-barroso",
        "crunchbase_url": "https://crunchbase.com/company/rafs-company",
        "paperstreet_url": "https://paperstreet.vc/rbarroso78",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={
            "twitter_url": "twitter",
            "linkedin_url": "linkedin",
            "facebook_url": "facebook",
            "angellist_url": "angel",
            "pitchbook_url": "pitchbook",
            "crunchbase_url": "crunchbase",
            "paperstreet_url": "paperstreet",
        },
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "twitter_url": "twitter.com/rbarroso78",
                    "linkedin_url": "linkedin.com/pub/rafael-barroso",
                    "facebook_url": "facebook.com/rafabaruv",
                    "angellist_url": "angel.co/rafael-barroso",
                    "pitchbook_url": "my.pitchbook.com/profile/rafael-barroso",
                    "crunchbase_url": "crunchbase.com/organization/rafs-company",
                    "paperstreet_url": "paperstreet.vc/rbarroso78",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call2(processes):
    input_data = {
        "facebook_url": "https://www.facebook.com/rafabaruv",
        "angellist_url": "https://angel.co/rafael-barroso",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={
            "facebook_url": "facebook",
            "angellist_url": "angel",
        },
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "facebook_url": "facebook.com/rafabaruv",
                    "angellist_url": "angel.co/rafael-barroso",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call__bad_cleaning_method(processes):
    with pytest.raises(ValueError):
        SocialMediaUrlCleanerStage(
            fields={"twitter_url": "twittter"},
            processes=processes,
            error_handling_method=ErrorHandlingMethod.RAISE,
        )


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call_fields_error(processes):
    input_data = {
        "twitter_url": "https://twitter.com/rbarroso78",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={"twitter_url": "linkedin"},
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[input_record])
    with pytest.raises(Exception):
        stage(input_)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_call_yahoo(processes):
    input_data = {
        "yahoo_url": "https://yahoo.com/rbarroso78",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={"yahoo_url": "linkedin"},
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[input_record])
    with pytest.raises(Exception):
        stage(input_)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_null_on_error(processes):
    input_data = {
        "linkedin_url": "https://linkedin.com/",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={"linkedin_url": "linkedin"},
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
        null_on_error=True,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "linkedin_url": None,
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_null(processes):
    input_data = {
        "twitter_url": None,
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={"twitter_url": "twitter"},
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "twitter_url": None,
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)


@parametrize_processes(SocialMediaUrlCleanerStage)
def test_case_insensitive(processes):
    input_data = {
        "twitter_url": "HTTPS://TWITTER.COM/rbarroso78",
        "linkedin_url": "HTTP://WWW.LINKEDIN.COM/pub/rafael-barroso/20/56a/84b",
        "facebook_url": "HTTPS://WWW.FACEBOOK.COM/rafabaruv",
        "angellist_url": "HTTPS://ANGEL.CO/rafael-barroso",
        "pitchbook_url": "HTTPS://MY.PITCHBOOK.COM/profile/rafael-barroso",
        "crunchbase_url": "HTTPS://CRUNCHBASE.COM/company/rafs-company",
        "paperstreet_url": "HTTPS://PAPERSTREET.VC/rbarroso78",
    }
    input_record = Record(data=input_data)
    stage = SocialMediaUrlCleanerStage(
        fields={
            "twitter_url": "twitter",
            "linkedin_url": "linkedin",
            "facebook_url": "facebook",
            "angellist_url": "angel",
            "pitchbook_url": "pitchbook",
            "crunchbase_url": "crunchbase",
            "paperstreet_url": "paperstreet",
        },
        processes=processes,
    )
    input_ = Batch(records=[input_record])
    expected = Batch(
        records=[
            Record(
                data={
                    "twitter_url": "twitter.com/rbarroso78",
                    "linkedin_url": "linkedin.com/pub/rafael-barroso",
                    "facebook_url": "facebook.com/rafabaruv",
                    "angellist_url": "angel.co/rafael-barroso",
                    "pitchbook_url": "my.pitchbook.com/profile/rafael-barroso",
                    "crunchbase_url": "crunchbase.com/organization/rafs-company",
                    "paperstreet_url": "paperstreet.vc/rbarroso78",
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.records[0].data == expected.records[0].data
