import copy
import datetime
from datetime import timezone

import pytest
from simple_mockforce import mock_salesforce
from simple_salesforce import Salesforce

from cluo import config
from cluo.config import RunMode
from cluo.core import Batch, Pipeline, Record
from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import InMemoryOffsetManager
from cluo.sources.salesforce_select import SalesforceSelectSource

pytestmark = [
    pytest.mark.salesforce,
]
DEBUG = False


def setup_module(module):
    # set up a vs code remote debugging connection when DEBUG = True
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR REMOTE DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


MOCK_CREDS = {
    "username": "test",
    "password": "test",
    "security_token": "123",
    "domain": "mock",
}


test_dates = {
    "1": "2000-01-01T00:00:00.000+0000",
    "2": "2000-01-02T00:00:00.000+0000",
    "3": "2000-01-03T00:00:00.000+0000",
}


test_records = {
    "ce_test_records": [
        [
            {
                "Id_c": "a403s000000lOsjAAE",
                "LastModifiedDate_c": datetime.datetime(
                    2020, 6, 19, 22, 50, 16
                ),  # "2020-06-19T22:50:16.000+0000",
                "Other_Offset": "2021-05-19T22:50:16.000+0000",
            },
            {
                "Id_c": "a403s000000lPdpAAE",
                "LastModifiedDate_c": datetime.datetime(
                    2020, 7, 12, 15, 6, 21
                ),  # "2020-07-12T15:06:21.000+0000",
                "Other_Offset": "2021-06-19T22:50:16.000+0000",
            },
        ]
    ],
    "w_same_offset": [
        [
            {"Name": "1", "DateModified": test_dates["1"]},
            {"Name": "2", "DateModified": test_dates["1"]},
        ],
        [
            {"Name": "1", "DateModified": test_dates["2"]},
            {"Name": "2", "DateModified": test_dates["2"]},
            {"Name": "3", "DateModified": test_dates["2"]},
        ],
        [
            {"Name": "1", "DateModified": test_dates["3"]},
            {"Name": "2", "DateModified": test_dates["3"]},
            {"Name": "3", "DateModified": test_dates["3"]},
            {"Name": "4", "DateModified": test_dates["3"]},
            {"Name": "5", "DateModified": test_dates["3"]},
            {"Name": "6", "DateModified": test_dates["3"]},
            {"Name": "7", "DateModified": test_dates["3"]},
            {"Name": "8", "DateModified": test_dates["3"]},
        ],
    ],
    "w_different_offset": [
        [
            {"Name": "1", "DateModified": test_dates["1"]},
            {"Name": "2", "DateModified": test_dates["2"]},
        ],
        [
            {"Name": "1", "DateModified": test_dates["1"]},
            {"Name": "2", "DateModified": test_dates["2"]},
            {"Name": "3", "DateModified": test_dates["3"]},
        ],
        [
            {"Name": "1", "DateModified": test_dates["1"]},
            {"Name": "2", "DateModified": test_dates["2"]},
            {"Name": "3", "DateModified": test_dates["2"]},
            {"Name": "4", "DateModified": test_dates["3"]},
            {"Name": "5", "DateModified": test_dates["3"]},
        ],
        [
            {"Name": "1", "DateModified": test_dates["1"]},
            {"Name": "2", "DateModified": test_dates["2"]},
            {"Name": "3", "DateModified": test_dates["2"]},
            {"Name": "4", "DateModified": test_dates["2"]},
            {"Name": "5", "DateModified": test_dates["2"]},
            {"Name": "6", "DateModified": test_dates["3"]},
            {"Name": "7", "DateModified": test_dates["3"]},
            {"Name": "8", "DateModified": test_dates["3"]},
        ],
    ],
}


def convert_dates(records, date_field):
    for record in records:
        record[date_field] = (
            record[date_field]
            .replace(microsecond=0)
            .replace(tzinfo=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    return records


@mock_salesforce
@pytest.mark.parametrize("records", test_records["ce_test_records"])
@pytest.mark.parametrize("run_mode", [RunMode.WRITE, RunMode.PREVIEW])
def test_select_with_variable_run_mode(run_mode, records):
    fields = ["Id", "Id_c", "LastModifiedDate_c", "Other_Offset"]
    # Initialize pipeline
    config.set_run_mode(run_mode)
    pipeline = Pipeline(name="test_pipeline")

    input_records = convert_dates(copy.deepcopy(records), "LastModifiedDate_c")

    salesforce = Salesforce(**MOCK_CREDS)
    # Load test records using simple-mockforce
    for record in input_records:
        salesforce.Company_Event__c.create(record)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set(offset_manager.initial_offset)

    source = SalesforceSelectSource(
        entity="Company_Event__c",
        fields=fields,
        **MOCK_CREDS,
        offset_field="LastModifiedDate_c",
        batch_size=10,
        offset_manager=offset_manager,
        offset_nonunique=False,
    )

    pipeline.add_source(source)

    expected_batch = Batch(
        records=[
            Record(data=records[0]),
            Record(data=records[1]),
        ]
    )

    with source:
        result = source.process_batch(Batch())

    expected_record_ids = {rec.data.get("Id_c") for rec in expected_batch.records}
    actual_record_ids = {rec.data.get("Id_c") for rec in result.records}
    assert (
        expected_record_ids == actual_record_ids
    ), f"actual ids {actual_record_ids} match expected ids {expected_record_ids}"


@mock_salesforce
@pytest.mark.parametrize("records", test_records["ce_test_records"])
@pytest.mark.parametrize("run_mode", [RunMode.WRITE, RunMode.PREVIEW])
def test_select_past_current_offset(run_mode, records):
    # this date is past the latest record timestamps in the test data set
    test_offset = datetime.datetime(2020, 7, 12, 15, 6, 21)
    fields = ["Id", "Id_c", "LastModifiedDate_c", "Other_Offset"]
    config.set_run_mode(run_mode)
    pipeline = Pipeline(name="test_pipeline")

    input_records = convert_dates(copy.deepcopy(records), "LastModifiedDate_c")

    salesforce = Salesforce(**MOCK_CREDS)

    # Load test records using simple-mockforce
    for record in input_records:
        salesforce.Company_Event__c.create(record)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set(test_offset)
    source = SalesforceSelectSource(
        entity="Company_Event__c",
        fields=fields,
        **MOCK_CREDS,
        offset_field="LastModifiedDate_c",
        batch_size=2,
        offset_manager=offset_manager,
        offset_nonunique=True,
    )

    pipeline.add_source(source)

    expected_batch = Batch(records=[])

    with source:
        actual_batch = source.process_batch(Batch())

    expected_record_ids = {rec.get("Id_c") for rec in expected_batch.records}
    actual_record_ids = {rec.get("Id_c") for rec in actual_batch.records}
    assert (
        expected_record_ids == actual_record_ids
    ), f"actual ids {actual_record_ids} match expected ids {expected_record_ids}"


@mock_salesforce
@pytest.mark.parametrize("records", test_records["w_same_offset"])
def test_batch_w_same_offset_values(records):
    """All records collected have the same offset_field value."""

    # TEST INPUTS:
    batch_size = len(records)
    # Using DateModified as offset to mock LastModifiedDate seen in Salesforce
    offset_field = "DateModified"
    field_names = ["Id", "Name", offset_field]

    # EXPECTED VALUES:
    expected_record_names = [x["Name"] for x in records]
    expected_offset = datetime.datetime.fromisoformat(records[0][offset_field])
    # All offset values are the same in the batch:
    # - offset_value stays the same
    # Explanation:
    # - Given that we cannot expect batches to contain unique
    #   offset values, we will page through and process all
    #   results with an offset greater than the previously stored
    #   value. Then persist the max processed offset value on the
    #   final page (first partial batch or first empty page).

    # Initialize pipeline
    config.set_run_mode(RunMode.WRITE)
    pipeline = Pipeline(name="test_pipeline")

    # Initialize mock Salesforce connection
    salesforce = Salesforce(**MOCK_CREDS)

    # Load test records using simple-mockforce
    for record in records:
        salesforce.Account.create(record)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    source = SalesforceSelectSource(
        entity="Account",
        fields=field_names,
        **MOCK_CREDS,
        batch_size=batch_size,
        offset_field=offset_field,
        offset_manager=offset_manager,
        offset_nonunique=True,
    )
    pipeline.add_source(source)

    with source:
        batch_records = source.process_batch(Batch())
        returned_record_names = [dict(x.data)["Name"] for x in batch_records.records]

    # Test page increment, no offset change
    assert set(returned_record_names) == set(expected_record_names)
    assert offset_manager.get() == expected_offset
    assert source.max_id is not None

    with source:
        batch_records = source.process_batch(Batch())

    # Test page reset, offset change
    assert batch_records == Batch()
    assert offset_manager.get() == expected_offset
    assert source.max_id is None


@mock_salesforce
@pytest.mark.parametrize("records", test_records["w_different_offset"])
def test_batch_w_unique_offset_values(records):
    """Each record collected has a unique offset_field value."""

    # EXPECTED VALUES:
    expected_record_names = [x["Name"] for x in records]
    # Each offset_value in records collected is different:
    # - offset_value is set to max timestamp of records collected
    # Explaination:
    # - when unique offsets are expected paging isn't necessary

    # offset batch is always zero for these test cases - different timestamps in every batch
    expected_offset = datetime.datetime.fromisoformat(records[-1]["DateModified"])

    # Initialize pipeline
    config.set_run_mode(RunMode.WRITE)
    pipeline = Pipeline(name="test_pipeline")
    # Initialize mock Salesforce connection
    salesforce = Salesforce(**MOCK_CREDS)
    # Load test records using simple-mockforce
    for record in records:
        salesforce.Account.create(record)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    source = SalesforceSelectSource(
        entity="Account",
        fields=["Id", "Name", "DateModified"],
        **MOCK_CREDS,
        batch_size=len(records),
        offset_field="DateModified",
        offset_manager=offset_manager,
        offset_nonunique=False,
    )
    pipeline.add_source(source)

    with source:
        batch_records = source.process_batch(Batch())
        returned_record_names = [dict(x.data)["Name"] for x in batch_records.records]

    assert set(returned_record_names) == set(expected_record_names)
    assert offset_manager.get() == expected_offset


@mock_salesforce
@pytest.mark.parametrize(
    "records", [test_records["w_same_offset"][0], test_records["w_different_offset"][0]]
)
def test_batch_size_greater_than_source_data(records):
    """Count of records collected is less than batch_size."""

    # TEST INPUTS:
    # batch_size is greater than the length of records
    batch_size = len(records) + 1

    # EXPECTED VALUES:
    expected_record_names = [x["Name"] for x in records]
    # Length of records collected is less than batch_size:
    # - offset_value is set to max offset_value of records collected
    # - offset_batch is set to 0
    # Explaination:
    # - Given that the batch collected is smaller than the batch_size,
    #   the assumption is that these are the last records being ingested
    #   from the source. To prevent looping through the last few records of
    #   this batch, we set the offset_value to the max timestamp of these
    #   collected records.
    expected_record_dates = [
        datetime.datetime.strptime(x["DateModified"], "%Y-%m-%dT%H:%M:%S.%f%z")
        for x in records
    ]
    expected_offset = max(expected_record_dates)

    # Initialize pipeline
    config.set_run_mode(RunMode.WRITE)
    pipeline = Pipeline(name="test_pipeline")
    # Initialize mock Salesforce connection
    salesforce = Salesforce(**MOCK_CREDS)
    # Load test records using simple-mockforce
    for record in records:
        salesforce.Account.create(record)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    source = SalesforceSelectSource(
        entity="Account",
        fields=["Id", "Name", "DateModified"],
        **MOCK_CREDS,
        batch_size=batch_size,
        offset_field="DateModified",
        offset_manager=offset_manager,
        offset_nonunique=True,
    )
    pipeline.add_source(source)

    with source:
        batch_records = source.process_batch(Batch())
        returned_record_names = [dict(x.data)["Name"] for x in batch_records.records]

    assert set(returned_record_names) == set(expected_record_names)
    assert offset_manager.get() == expected_offset


"""
    multi_batch_test_cases are run with a batch size of two records unless otherwise indicated
    each set of numbers below, represents a record set to feed through the source
    test records are loaded with a last modified timestamp of midnight UTC on january 1st, 2020.
    the numbers in the test cases below represent seconds of offset past midnight for the test record.
    test cases pass if all the records in the group are ingested.
"""

multi_batch_test_cases = {
    "one record": {"data": [1]},
    "two records different timestamps": {"data": [1, 2]},
    "two records same timestamps": {"data": [1, 1]},
    "two batches different timestamps": {"data": [1, 2, 3, 4]},
    "first batch same offset": {"data": [1, 1, 2, 3, 4]},
    "multiple timestamps on batch boundary": {"data": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]},
    "multiple timestamps on batch boundary - one not": {
        "data": [1, 1, 2, 2, 3, 3, 3, 4, 4, 5, 5]
    },
    "second batch same timestamp offset two": {"data": [1, 1, 1, 2, 3]},
    "duplicate dates span multiple batches": {
        "data": [1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 6, 6, 6, 6, 6, 7]
    },
    "second batch will reprocess one record": {"data": [1, 2, 2, 2]},
    "multiple spans": {"data": [1, 2, 3, 3, 3, 4, 4, 5]},
    "batch size four, reprocess several records": {
        "data": [1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4],
        "batch_size": 4,
    },
}


@mock_salesforce
def test_batch_size_one():
    """salesforce source should raise exception with batch size 1"""
    with pytest.raises(ValueError) as ve:
        offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
        SalesforceSelectSource(
            entity="TestRecord",
            fields=["Id", "TestId", "DateModified"],
            **MOCK_CREDS,
            batch_size=1,
            offset_manager=offset_manager,
        )
        assert "Salesforce batch_size must be greater than 1" in str(
            ve
        ), "raises expected exception"


@mock_salesforce
def test_offset_field_in_select():
    """SalesforceSelectSource should throw and exception when offset field not in select list"""
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    with pytest.raises(ValueError) as ve:
        SalesforceSelectSource(
            entity="TestRecord",
            fields=["Id", "TestId"],
            **MOCK_CREDS,
            batch_size=1,
            offset_field="LastModifiedDate_c",
            offset_manager=offset_manager,
        )
        assert "offset field must be in fields list" in str(
            ve
        ), "raises expected exception"


@mock_salesforce
@pytest.mark.parametrize("include_deleted", [True, False])
def test_ingest_deleted_records(include_deleted):
    last_modified_date = datetime.datetime(2000, 1, 2)
    record_count = 8
    # every other record is deleted
    records = [
        {
            "Id__c": f"record {idx}",
            "LastModifiedDate__c": last_modified_date,
            "IsDeleted": idx % 2 == 0,
        }
        for idx in range(record_count)
    ]
    fields = ["Id", "Id__c", "LastModifiedDate__c"]
    # Initialize pipeline
    config.set_run_mode(RunMode.WRITE)
    pipeline = Pipeline(name="test_pipeline")

    input_records = convert_dates(copy.deepcopy(records), "LastModifiedDate__c")

    salesforce = Salesforce(**MOCK_CREDS)

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set(offset_manager.initial_offset)
    # Load test records using simple-mockforce
    for record in input_records:
        create_response = salesforce.test_source.create(record)
        if record.get("IsDeleted"):
            salesforce.test_source.delete(create_response.get("id"))

    source = SalesforceSelectSource(
        entity="test_source",
        fields=fields,
        **MOCK_CREDS,
        batch_size=10,
        offset_field="LastModifiedDate__c",
        include_deleted=include_deleted,
        offset_manager=offset_manager,
        offset_nonunique=True,
    )
    pipeline.add_source(source)

    expected_records = sorted(
        [
            {key: value for key, value in record.items() if key != "IsDeleted"}
            for record in records
            if not record.get("IsDeleted") or include_deleted
        ],
        key=lambda k: k.get("Id__c"),
    )
    with source:
        processed_batch = source.process_batch(Batch())

    actual_records = sorted(
        [dict(x.data) for x in processed_batch.records], key=lambda k: k.get("Id__c")
    )

    expected_record_ids = {rec.get("Id__c") for rec in expected_records}
    actual_record_ids = {rec.get("Id__c") for rec in actual_records}
    assert (
        expected_record_ids == actual_record_ids
    ), f"actual ids {actual_record_ids} match expected ids {expected_record_ids}"
