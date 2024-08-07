import datetime as dt
import json
import logging
from unittest.mock import patch

import pytest

from cluo.core import Batch, Pipeline, Record
from cluo.core.error_handler import ErrorHandlerFailureAction
from cluo.error_handlers import LoggingHandler
from cluo.sinks import DiscardSink
from cluo.sources import DummySource

pytestmark = [pytest.mark.logging]


@pytest.fixture
def exception_message():
    return "TEST FIXTURE EXCEPTION VALUE XYZ"


@pytest.fixture
def handler_fails(exception_message):
    with patch.object(LoggingHandler, "serialize") as m_serialize:
        m_serialize.side_effect = ValueError(exception_message)
        yield m_serialize


def test_logging_handler_sends_to_console(send_to_error_stage, caplog):
    in_record = Record(data={"a": 3})
    dummy_source = DummySource(record=in_record, batch_size=1)
    trash = DiscardSink()
    log_handler = LoggingHandler()

    p = Pipeline()
    p.add_source(dummy_source)
    p.add_error_handler(log_handler)

    dummy_source >> send_to_error_stage >> trash

    with caplog.at_level(logging.ERROR):
        p._single_run(dummy_source, Batch())

    error_json = json.loads(caplog.records[0].message)

    assert error_json["data"] == in_record.data
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


@pytest.mark.parametrize(
    "failure_action_type",
    [ErrorHandlerFailureAction.RAISE, ErrorHandlerFailureAction.LOG, None],
)
def test_logging_handler_failure_action(
    send_to_error_stage, handler_fails, failure_action_type, exception_message
):
    in_record = Record(data={"a": 3})
    dummy_source = DummySource(record=in_record, batch_size=1)
    trash = DiscardSink()
    log_handler = LoggingHandler(failure_action=failure_action_type)

    p = Pipeline()
    p.add_source(dummy_source)
    p.add_error_handler(log_handler)

    dummy_source >> send_to_error_stage >> trash

    with pytest.raises(ValueError) as ve:
        p._single_run(dummy_source, Batch())
    assert exception_message in str(ve)


def test_logging_handler_failure_action_ignore(
    send_to_error_stage, handler_fails, caplog
):
    # failure action log should raise exception on log error handler
    in_record = Record(data={"a": 3})
    dummy_source = DummySource(record=in_record, batch_size=1)
    trash = DiscardSink()
    log_handler = LoggingHandler(failure_action=ErrorHandlerFailureAction.IGNORE)

    p = Pipeline()
    p.add_source(dummy_source)
    p.add_error_handler(log_handler)

    dummy_source >> send_to_error_stage >> trash

    with caplog.at_level(logging.ERROR):
        p._single_run(dummy_source, Batch())

    # ignore = no exception, no output
    assert len(caplog.records) == 0
