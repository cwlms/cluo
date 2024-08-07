import copy
import datetime
import datetime as dt
import json
import logging
from unittest.mock import patch

import pytest

from cluo.config import RunMode, set_run_mode
from cluo.core import Batch, Pipeline, Record
from cluo.core.error_handler import ErrorHandlerFailureAction
from cluo.error_handlers import KafkaErrorHandler
from cluo.sinks import DiscardSink
from cluo.sources import DummySource
from cluo.utils.kafka import _consume_until_num_messages

pytestmark = [pytest.mark.kafka_error]


@pytest.fixture
def exception_message():
    return "TEST FIXTURE EXCEPTION VALUE XYZ"


@pytest.fixture
def in_record():
    yield Record(data={"a": 3})


@pytest.fixture
def utc_now_value():
    return datetime.datetime(2021, 10, 21, 10, 24, 12, 1211, datetime.timezone.utc)


@pytest.fixture
def standard_pipeline(in_record, send_to_error_stage):
    set_run_mode(RunMode.WRITE)
    dummy_source = DummySource(record=in_record, batch_size=1)
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> send_to_error_stage >> trash
    yield p


@pytest.fixture
def kafka_fails(exception_message):
    with patch.object(KafkaErrorHandler, "_get_producer") as m_failer:
        m_failer.side_effect = ValueError(exception_message)
        yield m_failer


@pytest.fixture
def logging_fails(exception_message):
    with patch.object(KafkaErrorHandler, "serialize") as m_failer:
        m_failer.side_effect = ValueError(exception_message)
        yield m_failer


def assert_log_error_json(error_json, expected_record):
    assert error_json["data"] == expected_record.data
    assert error_json["record_uuid"] is not None and isinstance(
        error_json["record_uuid"], str
    )
    exception_list = error_json["exceptions"]
    assert len(exception_list) > 0
    test_exception = exception_list[0]
    assert test_exception["stage_name"] == "SendToErrorStage"
    assert test_exception["handling_method"] == "SEND_TO_ERROR"
    assert dt.datetime.fromisoformat(test_exception["error_dt"]) < dt.datetime.utcnow()
    assert test_exception["cause"]["error_type"] == "ValueError"
    assert test_exception["cause"]["message"] == "A wild error appeared!"
    assert test_exception["cause"]["traceback"].endswith(
        'raise ValueError("A wild error appeared!")\n'
    )


def assert_kafka_error_json(message, expected_record, record_number=1):
    actual_data = copy.deepcopy(message)
    del actual_data["_metadata"]
    assert (
        actual_data == expected_record.data
    ), f"record data in kafka error number {record_number}"
    # non-data record attributes stored in the metadata key
    error_json = message["_metadata"]
    assert error_json["record_uuid"] is not None and isinstance(
        error_json["record_uuid"], str
    ), f"record_uuid in kafka error number {record_number}"
    exception_list = error_json["exceptions"]
    assert len(exception_list) > 0
    test_exception = exception_list[0]
    assert (
        test_exception["stage_name"] == "SendToErrorStage"
    ), "stage_name in exception data"
    assert test_exception["handling_method"] == "SEND_TO_ERROR"
    assert dt.datetime.fromisoformat(test_exception["error_dt"]) < dt.datetime.utcnow()
    assert test_exception["cause"]["error_type"] == "ValueError"
    assert test_exception["cause"]["message"] == "A wild error appeared!"
    assert test_exception["cause"]["traceback"].endswith(
        'raise ValueError("A wild error appeared!")\n'
    )


def test_kafka_handler_produces_messages(
    kafka_test_servers: str,
    kafka_test_topic: str,
    standard_pipeline,
    in_record,
    kafka_consumer,
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.LOG,
        )
    )
    standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
    messages = _consume_until_num_messages(kafka_consumer, num_messages=1, timeout=15)
    assert len(messages) == 1
    assert_kafka_error_json(messages[0], in_record)


def test_kafka_handler_log_records_flag(
    kafka_test_servers: str,
    kafka_test_topic: str,
    standard_pipeline,
    in_record,
    kafka_consumer,
    caplog,
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            log_records=True,
        )
    )

    with caplog.at_level(logging.ERROR):
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
    messages = _consume_until_num_messages(kafka_consumer, num_messages=1, timeout=15)
    assert len(messages) == 1
    assert_kafka_error_json(messages[0], in_record)
    assert_log_error_json(json.loads(caplog.records[0].message), in_record)


def test_kafka_handler_serializes_dates(
    kafka_test_servers: str,
    kafka_test_topic: str,
    send_to_error_stage,
    kafka_consumer,
    utc_now_value,
):
    set_run_mode(RunMode.WRITE)
    date_record = Record(data={"a": utc_now_value})
    dummy_source = DummySource(record=date_record, batch_size=1)
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> send_to_error_stage >> trash
    p.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.RAISE,
        )
    )
    p._single_run(p.sources[0], Batch())
    messages = _consume_until_num_messages(kafka_consumer, num_messages=1, timeout=15)
    assert len(messages) == 1

    actual_data = copy.deepcopy(messages[0])
    del actual_data["_metadata"]
    assert actual_data == {
        "a": utc_now_value.isoformat()
    }, "record data matches expected"
    # non-data record attributes stored in the metadata key
    error_json = messages[0]["_metadata"]
    assert error_json["record_uuid"] is not None and isinstance(
        error_json["record_uuid"], str
    ), "record_uuid"
    exception_list = error_json["exceptions"]
    assert len(exception_list) > 0
    test_exception = exception_list[0]
    assert (
        test_exception["stage_name"] == "SendToErrorStage"
    ), "stage_name in exception data"
    assert test_exception["handling_method"] == "SEND_TO_ERROR"
    assert dt.datetime.fromisoformat(test_exception["error_dt"]) < dt.datetime.utcnow()
    assert test_exception["cause"]["error_type"] == "ValueError"
    assert test_exception["cause"]["message"] == "A wild error appeared!"
    assert test_exception["cause"]["traceback"].endswith(
        'raise ValueError("A wild error appeared!")\n'
    )


def test_kafka_handler_serializes_nested_objects(
    kafka_test_servers: str,
    kafka_test_topic: str,
    send_to_error_stage,
    kafka_consumer,
):
    set_run_mode(RunMode.WRITE)
    obj_record = Record(data={"a": {"b": 10}})
    dummy_source = DummySource(record=obj_record, batch_size=1)
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> send_to_error_stage >> trash
    p.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.RAISE,
        )
    )
    p._single_run(p.sources[0], Batch())
    messages = _consume_until_num_messages(kafka_consumer, num_messages=1, timeout=15)
    assert len(messages) == 1

    actual_data = copy.deepcopy(messages[0])
    del actual_data["_metadata"]
    assert actual_data == obj_record.data, "record data matches expected"
    # non-data record attributes stored in the metadata key
    error_json = messages[0]["_metadata"]
    assert error_json["record_uuid"] is not None and isinstance(
        error_json["record_uuid"], str
    ), "record_uuid"
    exception_list = error_json["exceptions"]
    assert len(exception_list) > 0
    test_exception = exception_list[0]
    assert (
        test_exception["stage_name"] == "SendToErrorStage"
    ), "stage_name in exception data"
    assert test_exception["handling_method"] == "SEND_TO_ERROR"
    assert dt.datetime.fromisoformat(test_exception["error_dt"]) < dt.datetime.utcnow()
    assert test_exception["cause"]["error_type"] == "ValueError"
    assert test_exception["cause"]["message"] == "A wild error appeared!"
    assert test_exception["cause"]["traceback"].endswith(
        'raise ValueError("A wild error appeared!")\n'
    )


def test_kafka_handler_produces_multiple_messages(
    kafka_test_servers: str,
    kafka_test_topic: str,
    send_to_error_stage,
    in_record,
    kafka_consumer,
):
    set_run_mode(RunMode.WRITE)
    dummy_source = DummySource(record=in_record, batch_size=10)
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> send_to_error_stage >> trash

    p.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.LOG,
        )
    )
    p._single_run(p.sources[0], Batch())
    messages = _consume_until_num_messages(kafka_consumer, num_messages=10, timeout=15)
    assert len(messages) == 10
    for rec_num in range(len(messages)):
        assert_kafka_error_json(messages[rec_num], in_record, rec_num)


def test_kafka_handler_failure_action_log(
    kafka_test_servers: str,
    kafka_test_topic: str,
    caplog,
    kafka_fails,
    standard_pipeline,
    in_record,
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.LOG,
        )
    )
    with caplog.at_level(logging.ERROR):
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
    assert_log_error_json(json.loads(caplog.records[0].message), in_record)


def test_kafka_handler_failure_action_log_cascaded(
    kafka_test_servers: str,
    kafka_test_topic: str,
    logging_fails,
    kafka_fails,
    standard_pipeline,
    in_record,
):
    # ensure a failure in logging calls still produces output
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.LOG,
        )
    )
    with pytest.raises(ValueError) as ve:
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
        assert exception_message in str(ve)


def test_kafka_handler_failure_raise_preview(
    kafka_test_servers: str, kafka_test_topic: str, caplog, standard_pipeline, in_record
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.RAISE,
        )
    )
    set_run_mode(RunMode.PREVIEW)
    with caplog.at_level(logging.ERROR):
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
    assert_log_error_json(json.loads(caplog.records[0].message), in_record)


@pytest.mark.parametrize(
    "failure_action_type",
    [ErrorHandlerFailureAction.RAISE, None],
)
def test_kafka_handler_failure_action_raise(
    kafka_test_servers: str,
    kafka_test_topic: str,
    kafka_fails,
    failure_action_type,
    exception_message,
    standard_pipeline,
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=failure_action_type,
        )
    )
    with pytest.raises(ValueError) as ve:
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
        assert exception_message in str(ve)


def test_logging_handler_failure_action_ignore(
    kafka_test_servers: str,
    kafka_test_topic: str,
    kafka_fails,
    caplog,
    standard_pipeline,
):
    standard_pipeline.add_error_handler(
        KafkaErrorHandler(
            bootstrap_servers=kafka_test_servers,
            topic=kafka_test_topic,
            failure_action=ErrorHandlerFailureAction.IGNORE,
        )
    )
    with caplog.at_level(logging.ERROR):
        standard_pipeline._single_run(standard_pipeline.sources[0], Batch())
    # ignore = no exception, no output
    assert len(caplog.records) == 0
