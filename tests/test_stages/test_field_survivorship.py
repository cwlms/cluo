import datetime
import decimal
from copy import copy

import pytest

from cluo.core.batch import Batch, Record
from cluo.core.exceptions import ErrorHandlingMethod
from cluo.stages import FieldSurvivorshipStage
from tests.conftest import parametrize_processes
from tests.helpers import batch_from_dicts, dicts_from_batch

from .facebook_matches import matches

pytestmark = [
    pytest.mark.field_survivorship,
    pytest.mark.survivorship,
]


@parametrize_processes(FieldSurvivorshipStage)
def test_call_source_priority(processes):
    config = {
        "company_name": {  # exists in input record
            "rule": "source_priority",
            "order": ["aim", "salesforce", "crunchbase"],
        },
        "data_source_key": {
            "rule": "source_priority",
            "order": ["affinity", "aim", "salesforce", "crunchbase"],
        },  # does not exist in input record
        "investor_type": {"rule": "aggregate"},  # aggregating strings
        "all_categories": {"rule": "aggregate"},  # aggregating lists
        "state_province": {"rule": "recency"},
        "point_of_contact": {"rule": "frequency"},
    }
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    input_ = Batch(
        records=[
            Record(
                data={
                    "company_name": "Face-error",
                    "other": "other",
                    "matches": matches,
                }
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "company_name": "Facebook",
                    "other": "other",
                    "data_source_key": "affinity_260472098",
                    "investor_type": ["Angel", "Corporate Venture Capital"],
                    "all_categories": [
                        "Artificial Intelligence & Machine Learning",
                        "SaaS",
                        "Developer Tools",
                        "Fashion",
                        "Insurtech",
                        "Professional Services",
                        "Fintech",
                        "Gaming",
                        "Education",
                        "Transportation",
                        "Energy",
                        "E-Commerce",
                        "Consumer Software",
                        "Payments",
                        "Marketplace",
                        "Advertising",
                        "Finance",
                        "Enterprise Software",
                        "Lending",
                    ],
                    "state_province": "Minnesota",
                    "point_of_contact": "sw.saurabhw@gmail.com",
                    "matches": matches,
                }
            )
        ]
    )

    actual = stage(input_)

    # list comparisons might have an issue
    actual.records[0].data["all_categories"] = set(
        actual.records[0].data["all_categories"]
    )

    expected.records[0].data["all_categories"] = set(
        expected.records[0].data["all_categories"]
    )

    assert actual.has_same_record_data(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_call_source_priority_missing_source(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1", "source_2"],
        },
    }
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    input_ = Batch(
        records=[
            Record(
                data={
                    "some_field": "incoming_value",
                    "data_source": "source_3",
                    "matches": [
                        {"some_field": "some_value", "data_source": "source_3"}
                    ],
                }
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "some_field": None,
                    "data_source": "source_3",
                    "matches": [
                        {"some_field": "some_value", "data_source": "source_3"}
                    ],
                }
            )
        ]
    )

    actual = stage(input_)

    assert actual.has_same_record_data(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_call_source_priority_null_field(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1"],
        },
    }
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    input_ = Batch(
        records=[
            Record(
                data={
                    "some_field": None,
                    "data_source": "source_1",
                    "matches": [{"some_field": None, "data_source": "source_1"}],
                }
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "some_field": None,
                    "data_source": "source_1",
                    "matches": [{"some_field": None, "data_source": "source_1"}],
                }
            )
        ]
    )

    actual = stage(input_)

    assert actual.has_same_record_data(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_call_source_priority_list_field(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1"],
        },
    }
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    source_value = ["value"]
    input_ = Batch(
        records=[
            Record(
                data={
                    "some_field": source_value,
                    "data_source": "source_1",
                    "matches": [
                        {"some_field": source_value, "data_source": "source_1"}
                    ],
                }
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "some_field": source_value,
                    "data_source": "source_1",
                    "matches": [
                        {"some_field": source_value, "data_source": "source_1"}
                    ],
                }
            )
        ]
    )

    actual = stage(input_)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_call_source_priority_timestamp_field(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1", "source_2"],
        },
    }
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    source_value_1 = datetime.datetime(2020, 1, 1, 1, 1, 1, 1, datetime.timezone.utc)
    source_value_2 = datetime.datetime(
        2021, 1, 1, 1, 1, 1, 123450, datetime.timezone.utc
    )
    input_ = Batch(
        records=[
            Record(
                data={
                    "some_field": source_value_1,
                    "data_source": "source_2",
                    "matches": [
                        {
                            "some_field": datetime.datetime(
                                2021, 1, 1, 1, 1, 1, 123450, datetime.timezone.utc
                            ),
                            "data_source": "source_1",
                        }
                    ],
                }
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "some_field": source_value_2,
                    "data_source": "source_2",
                    "matches": [
                        {
                            "some_field": datetime.datetime(
                                2021, 1, 1, 1, 1, 1, 123450, datetime.timezone.utc
                            ),
                            "data_source": "source_1",
                        }
                    ],
                }
            )
        ]
    )

    actual = stage(input_)

    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_frequency(processes):
    config = {"some_field": {"rule": "frequency"}}
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    matches_by_frequency = [
        ("string", 1),
        (("tuple", "list", 1), 2),
        ({"key": "dict", "key2": {1: "nested"}, "key3": ("tuple",)}, 3),
        (decimal.Decimal(100.5), 4),
        (Batch([Record({"a": "b"})]), 4),
        (datetime.datetime.now(), 5),
        ("most frequent", 6),
        # more frequent but none: excluded
        (None, 7),
    ]
    matches = []
    for field, frequency in matches_by_frequency:
        matches.extend({"some_field": field} for _ in range(frequency))
    input_dicts = [
        {
            "some_field": "current_record_value",
            "matches": matches,
        }
    ]
    output_dicts = [
        {
            "some_field": "most frequent",
            "matches": matches,
        }
    ]
    input_ = batch_from_dicts(input_dicts)
    expected = batch_from_dicts(output_dicts)
    actual = stage(input_)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_frequency_empty(processes):
    config = {"some_field": {"rule": "frequency"}}
    stage = FieldSurvivorshipStage(
        config, processes=processes, error_handling_method=ErrorHandlingMethod.RAISE
    )
    matches_by_frequency = [
        # more frequent but none: excluded
        (None, 7),
    ]
    matches = []
    for field, frequency in matches_by_frequency:
        for _ in range(frequency):
            matches.append({"some_field": field})
    input_dicts = [
        {
            "some_field": None,
            "matches": matches,
        }
    ]
    output_dicts = [
        {
            "some_field": None,
            "matches": matches,
        }
    ]
    input_ = batch_from_dicts(input_dicts)
    expected = batch_from_dicts(output_dicts)
    actual = stage(input_)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@pytest.fixture()
def recency_dates():
    # recency dates are sorted oldest to newest
    yield list(
        sorted(
            [
                datetime.datetime(2021, 10, 21, 9, 24, 12, 1211, datetime.timezone.utc),
                # a few milliseconds earlier
                datetime.datetime(2021, 10, 21, 9, 24, 12, 1201, datetime.timezone.utc),
                # a second earlier
                datetime.datetime(2021, 10, 21, 9, 24, 11, 1201, datetime.timezone.utc),
                # a day earlier
                datetime.datetime(2021, 10, 20, 9, 24, 11, 1201, datetime.timezone.utc),
                # a year earlier
                datetime.datetime(2020, 10, 20, 9, 24, 11, 1201, datetime.timezone.utc),
            ],
            reverse=True,
        )
    )


@pytest.fixture()
def postgres_timestamp_format():
    return "%Y-%m-%dT%H:%M:%S.%f"


@pytest.fixture()
def survivorship_test_config():
    return {"some_field": {"rule": "recency"}}


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency(
    processes, recency_dates, postgres_timestamp_format, survivorship_test_config
):
    # first record is the oldest
    string_date_records = [
        {
            "some_field": f"datetime_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[seq].strftime(postgres_timestamp_format),
        }
        for seq in reversed(range(len(recency_dates)))
    ]
    # input record is the oldest
    # matches go oldest to newest
    input_dict = copy(string_date_records[0])
    input_dict["matches"] = copy(string_date_records[1:])
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the last (newest) record.
    expected_dict["some_field"] = string_date_records[-1]["some_field"]
    stage = FieldSurvivorshipStage(
        survivorship_test_config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency_include_updated(processes, recency_dates):
    config = {("some_field", "ts_updated"): {"rule": "recency"}}
    # when surviving by recency and ts_updated configured fields survive updated value from most recent record
    # use ts_updated dates in un-stringified form so we don't have to contend with weird timezone issues survived ts_updated
    # first record is the oldest
    date_records = [
        {
            "some_field": f"datetime_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[seq],
        }
        for seq in reversed(range(len(recency_dates)))
    ]

    # input record is the oldest
    # matches go oldest to date_records
    input_dict = copy(date_records[0])
    input_dict["matches"] = copy(date_records[1:])
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the last (newest) record.
    expected_dict["some_field"] = date_records[-1]["some_field"]
    expected_dict["ts_updated"] = date_records[-1]["ts_updated"]
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency_multiple_fields(processes, recency_dates):
    config = {("1", "2"): {"rule": "recency"}}
    # first record is the oldest
    date_records = [
        {
            "1": f"datetime_{seq}",
            "2": f"secondfield_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[seq],
        }
        for seq in reversed(range(len(recency_dates)))
    ]

    # input record is the oldest
    # matches go oldest to date_records
    input_dict = copy(date_records[0])
    input_dict["matches"] = copy(date_records[1:])
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the last (newest) record.
    expected_dict["1"] = date_records[-1]["1"]
    expected_dict["2"] = date_records[-1]["2"]
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency_empty_value(
    processes, recency_dates, postgres_timestamp_format, survivorship_test_config
):
    # when all have same updated date survivorship chooses the first record in matches list
    stage = FieldSurvivorshipStage(
        survivorship_test_config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # all fields have the same updated date
    string_date_records = [
        {
            "some_field": f"datetime_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[seq].strftime(postgres_timestamp_format),
        }
        for seq in reversed(range(len(recency_dates)))
    ]
    # the most recent record has empty value
    string_date_records[-1]["some_field"] = None
    input_dict = copy(string_date_records[0])
    input_dict["matches"] = copy(string_date_records[1:])
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the second to last (newest) record. it has a non-empty value
    expected_dict["some_field"] = string_date_records[-2]["some_field"]
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency_empty_value_null_override(
    processes, recency_dates, postgres_timestamp_format
):
    override_null_test_config = {
        "some_field": {"rule": "recency", "include_nulls": True}
    }
    # when all have same updated date survivorship chooses the first record in matches list
    stage = FieldSurvivorshipStage(
        override_null_test_config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # all fields have the same updated date
    string_date_records = [
        {
            "some_field": f"datetime_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[seq].strftime(postgres_timestamp_format),
        }
        for seq in reversed(range(len(recency_dates)))
    ]
    # the most recent record has empty value
    string_date_records[-1]["some_field"] = None
    input_dict = copy(string_date_records[0])
    input_dict["matches"] = copy(string_date_records[1:])
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the last (newest) record
    # it has an empty value but that's ALLOWED by the rule
    expected_dict["some_field"] = string_date_records[-1]["some_field"]
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_recency_tiebreaker(
    processes, recency_dates, postgres_timestamp_format, survivorship_test_config
):
    # when all have same updated date survivorship chooses the first record in matches list
    stage = FieldSurvivorshipStage(
        survivorship_test_config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # all fields have the same updated date
    string_date_records = [
        {
            "some_field": f"datetime_{seq}",
            "unscoped_field": seq,
            "ts_updated": recency_dates[0].strftime(postgres_timestamp_format),
        }
        for seq in range(len(recency_dates))
    ]
    input_dict = copy(string_date_records[0])
    # different value for source record
    input_dict["some_field"] = "input_dict_value"
    input_dict["matches"] = copy(string_date_records)
    # its a string date now
    expected_dict = copy(input_dict)
    # survived value for some_field comes from the first match
    # as records are collected in order matches, input_record
    # and this sort order is preserved for all records that have the same updated timestamp
    expected_dict["some_field"] = string_date_records[0]["some_field"]
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_aggregation(processes):
    config = {"some_field": {"rule": "aggregate"}}
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "some_field": ["a", "b", "c"],
        "matches": [
            # same values, list
            {"some_field": ["a", "b", "c"]},
            # different values, list
            {"some_field": ["a", "b", "d"]},
            # different values, tuple
            {"some_field": ("d", "c", "a")},
            # different value, string
            {"some_field": "y"},
            # empty
            {"some_field": None},
            # empty array
            {"some_field": []},
            # missing survived field: not included
            {"not_present": "x"},
        ],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = ["a", "b", "c", "d", "y"]
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_aggregation_singletons(processes):
    # test aggregation for non-list source data
    config = {"some_field": {"rule": "aggregate"}}
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "some_field": "x",
        "matches": [
            # same value
            {"some_field": "x"},
            # different value
            {"some_field": "y"},
            # different case
            {"some_field": "Y"},
            # empty
            {"some_field": None},
            # missing survived field: not included
            {"not_present": "x"},
        ],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = ["Y", "x", "y"]
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_aggregation_nomatch(processes):
    # test aggregation for non-list source data
    config = {"some_field": {"rule": "aggregate"}}
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "some_field": "x",
        "matches": [],
    }
    expected_dict = copy(input_dict)
    # aggregation always survives a list
    expected_dict["some_field"] = []
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_nomatch(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "some_field": "x",
        "data_source": "source_1",
        "matches": [],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = None
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_multiple_fields(processes):
    config = {
        ("1", "2", "3"): {
            "rule": "source_priority",
            "order": ["source_1", "source_2"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "1": None,
        "2": "input",
        "3": "input",
        "data_source": "source_1",
        "matches": [
            # no non-empty values
            {
                "id": "match1",
                "1": None,
                "2": None,
                "3": None,
                "data_source": "source_1",
            },
            # source not in survived list
            {
                "id": "match2",
                "1": "1_2",
                "2": "2_2",
                "3": None,
                "data_source": "source_3",
            },
            # lower priority source
            {
                "id": "match3",
                "1": None,
                "2": "2_3",
                "3": "3_3",
                "data_source": "source_2",
            },
            # lower priority source
            {
                "id": "match4",
                "1": "1_4",
                "2": "2_4",
                "3": "3_4",
                "data_source": "source_2",
            },
            # a non-empty field in highest priority source.  USE FALSE IN VALUE TO ENSURE IT EVALUATES AS PRESENT
            {
                "id": "match5",
                "1": None,
                "2": None,
                "3": False,
                "data_source": "source_1",
            },
            {
                "id": "match6",
                "1": None,
                "2": "2_6",
                "3": None,
                "data_source": "source_1",
            },
            {
                "id": "match7",
                "1": None,
                "2": None,
                "3": "3_7",
                "data_source": "source_1",
            },
        ],
    }
    expected_dict = copy(input_dict) | {"1": None, "2": None, "3": False}
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_input_not_in_list(processes):
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_dict = {
        "some_field": "x",
        "data_source": "source_2",
        "matches": [],
    }
    expected_dict = copy(input_dict)
    # survived removes the source records value
    expected_dict["some_field"] = None
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_tiebreaker(processes):
    # when highest ranking source has multiple records, pick the most frequent value
    # when multiple values have same cardinality, pick the first one in match order
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1", "source_2", "source_3", "source_4", "source_5"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # no matches in source 1
    # source 2 has only empty values
    # multiple matches in source 3, three different values found:
    #   value 1 one match
    #   value 2 two matches
    #   value 3 three matches = winner
    input_dict = {
        "some_field": "value_s4_1",
        "data_source": "source_4",
        "matches": [
            # source 3: three different values
            # value_2 is most recent value so it gets picked
            {"data_source": "source_3", "some_field": "value_2"},
            {"data_source": "source_3", "some_field": "value_3"},
            {"data_source": "source_3", "some_field": "value_3"},
            {"data_source": "source_3", "some_field": "value_1"},
            {"data_source": "source_3", "some_field": "value_2"},
            {"data_source": "source_3", "some_field": "value_3"},
            # source 2: only empty
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
        ],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = "value_2"
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_tiebreaker_multiple_hits(processes):
    # when highest ranking source has multiple records, pick the most frequent value
    # when multiple values have same cardinality, pick the first one in match order
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1", "source_2", "source_3", "source_4", "source_5"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # no matches in source 1
    # source 2 has only empty values
    # multiple matches in source 3, three different values found:
    #   value 1 THREE matches
    #   value 2 THREE matches
    #   value 3 THREE matches
    # will pick value 1 as it is the first one in the matches list
    input_dict = {
        "some_field": "value_s4_1",
        "data_source": "source_4",
        "matches": [
            # first in the list = picked
            {"data_source": "source_3", "some_field": "value_1"},
            {"data_source": "source_3", "some_field": "value_2"},
            {"data_source": "source_3", "some_field": "value_3"},
            {"data_source": "source_3", "some_field": "value_3"},
            {"data_source": "source_3", "some_field": "value_1"},
            {"data_source": "source_3", "some_field": "value_1"},
            {"data_source": "source_3", "some_field": "value_2"},
            {"data_source": "source_3", "some_field": "value_2"},
            {"data_source": "source_3", "some_field": "value_3"},
            # source 2: only empty
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
            {"data_source": "source_2", "some_field": None},
        ],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = "value_1"
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)


config_validation_test_cases = {
    "empty_config": {"config": {}, "exception_value": "must have at least one rule"},
    "invalid_rule": {
        "config": {"field_1": {"rule": "field_order"}},
        "exception_value": "invalid rule type",
    },
    "duplicate_keys": {
        "config": {
            "field_1": {"rule": "recency"},
            ("field_1", "field_2"): {"rule": "recency"},
        },
        "exception_value": "must have no more than one rule per field",
    },
    "aggregate_rules": {
        "config": {("field_1", "field_2"): {"rule": "aggregate"}},
        "exception_value": "aggregate survivorship rules cannot be configured for field groups",
    },
    "source_priority_order_missing": {
        "config": {"field_1": {"rule": "source_priority"}},
        "exception_value": "source_priority survivorship rules must include a list of data sources",
    },
    "source_priority_order_int": {
        "config": {
            "field_1": {
                "rule": "source_priority",
                "order": [
                    1,
                ],
            }
        },
        "exception_value": "source_priority survivorship rules must include a list of data sources",
    },
    "source_priority_order_dict": {
        "config": {"field_1": {"rule": "source_priority", "order": {}}},
        "exception_value": "source_priority survivorship rules must include a list of data sources",
    },
    "source_priority_order_empty": {
        "config": {"field_1": {"rule": "source_priority", "order": []}},
        "exception_value": "source_priority survivorship rules must include a list of data sources",
    },
    "recency_include_nulls_not_boolean": {
        "config": {"field_1": {"rule": "recency", "include_nulls": "bad value"}},
        "exception_value": """recency survivorship key "include_nulls" must supply a boolean value""",
    },
}


@pytest.mark.parametrize(
    "validation_test_cases",
    config_validation_test_cases.values(),
    ids=config_validation_test_cases.keys(),
)
def test_survivorship_config_validation(validation_test_cases):
    config = validation_test_cases.get("config", {})
    stage = FieldSurvivorshipStage(
        config,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    expected = {
        "exception": validation_test_cases.get("exception_type", "ValueError"),
        "exception_value": validation_test_cases.get(
            "exception_value", "unknown exception value"
        ),
    }
    actual = {}
    try:
        stage(batch_from_dicts([{"field_1": 1}]))
    except Exception as exc:
        actual["exception"] = type(exc).__name__
        actual["exception_value"] = exc.args[0] if exc.args else ""
    assert actual, "exception was raised"
    assert expected.get("exception") == actual.get("exception"), "exception type"
    assert expected.get("exception_value") in actual.get(
        "exception_value"
    ), "exception text"


def test_survivorship_config_second_fail():
    """in the event that validation is skipped for whatever reason, ensure we dont try to evalue a survivorship rule that is not defined"""
    test_case = config_validation_test_cases.get("invalid_rule")
    config = test_case.get("config", {})
    stage = FieldSurvivorshipStage(
        config,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # force the validated flag to true to skip normal validation
    stage.validated = True
    expected = {
        "exception": "ValueError",
        "exception_value": "Unknown rule",
    }
    actual = {}
    try:
        stage(batch_from_dicts([{"field_1": 1}]))
    except Exception as exc:
        actual["exception"] = type(exc).__name__
        actual["exception_value"] = exc.args[0] if exc.args else ""
    assert actual, "exception was raised"
    assert expected.get("exception") == actual.get("exception"), "exception type"
    assert expected.get("exception_value") in actual.get(
        "exception_value"
    ), "exception text"


@parametrize_processes(FieldSurvivorshipStage)
def test_survivorship_source_priority_grouped_sources(processes):
    # when multiple sources are grouped together treat them as the same priority
    # source_2 and source_3 are grouped together.  pick the first value from either source as survived
    config = {
        "some_field": {
            "rule": "source_priority",
            "order": ["source_1", ("source_2", "source_3"), "source_4", "source_5"],
        },
    }
    stage = FieldSurvivorshipStage(
        config,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    # no matches in source 1
    # source 5 appears first in the list of values but it is lower in priority than source 2 and source 3
    # source 2 and source 3 have matches with source 3 being the most recent by sort order
    # source 2 and source 3 are considered as the same priority as they are grouped together into a tuple
    # the most recent value from the list of source 2 and source 3 values is returned as the survived value
    input_dict = {
        "some_field": "value_s4_1",
        "data_source": "source_4",
        "matches": [
            {"data_source": "source_5", "some_field": "value_s5_1"},
            # this value is survived as its first value from the highest priority source group
            {"data_source": "source_3", "some_field": "value_s3_1"},
            {"data_source": "source_2", "some_field": "value_s2_1"},
            {"data_source": "source_4", "some_field": "value_s4_1"},
        ],
    }
    expected_dict = copy(input_dict)
    expected_dict["some_field"] = "value_s3_1"
    input_batch = batch_from_dicts([input_dict])
    expected = batch_from_dicts([expected_dict])
    actual = stage(input_batch)
    assert dicts_from_batch(actual) == dicts_from_batch(expected)
