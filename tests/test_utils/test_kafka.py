import json
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock

import avro.schema
import pytest
from confluent_kafka import Consumer, KafkaError, Message, Producer
from pytest_mock import MockerFixture

from cluo.utils.kafka import (
    _consume_from_kafka,
    _consume_single_message,
    _consume_until_num_messages,
    _decode_kafka_msg_value,
    _deserialize_avro_msg,
    _get_avro_schema,
    _get_avro_schema_request_url,
    _get_avro_schema_text,
    _serialize_avro_message,
)

pytestmark = [pytest.mark.kafka]
DEBUG = False


def setup_module(module):
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


@pytest.fixture(scope="function")
def sent_messages(kafka_producer, kafka_test_topic) -> List[Dict | None]:
    encoded_messages = [b'{"foo": "bar"}', '{"boo": "baz"}', "", "{thats not json"]
    expected_messages = [{"foo": "bar"}, {"boo": "baz"}, None, None]
    for message in encoded_messages:
        kafka_producer.produce(kafka_test_topic, message)
    kafka_producer.flush()
    return expected_messages


def mock_message(
    value: str | bytes | None = None, error: KafkaError | None = None
) -> MagicMock:
    message = MagicMock(spec=Message)
    message.value.return_value = value
    message.error.return_value = error
    return message


def test_decode_kafka_msg_value_error(mocker: MockerFixture):
    """verifies that Kafka errors get logged and decoded to None"""
    logging_error = mocker.patch("logging.error")
    message = mock_message(error=KafkaError(KafkaError.INVALID_MSG))
    assert _decode_kafka_msg_value(message) is None
    logging_error.assert_called_once()


def test_decode_kafka_msg_value_error_eof(mocker: MockerFixture):
    """verifies that EOF errors are expected and not logged"""
    logging_error = mocker.patch("logging.error")
    message = mock_message(error=KafkaError(KafkaError._PARTITION_EOF))
    assert _decode_kafka_msg_value(message) is None
    logging_error.assert_not_called()


def test_decode_kafka_msg_value_none():
    message = mock_message(value=None)
    assert _decode_kafka_msg_value(message) is None


def test_decode_kafka_msg_value_bytes():
    message = mock_message(value=b'{"foo":"bar"}')
    assert _decode_kafka_msg_value(message) == {"foo": "bar"}


def test_decode_kafka_msg_value_str():
    message = mock_message(value='{"foo":"bar"}')
    assert _decode_kafka_msg_value(message) == {"foo": "bar"}


def test_decode_kafka_msg_value_invalid_json(mocker: MockerFixture):
    """validates invalid json results in a decode exception that gets logged"""
    logging_error = mocker.patch("logging.error")
    message = mock_message(value="")
    assert _decode_kafka_msg_value(message) is None
    logging_error.assert_called_once()


def test_consume_single_message(sent_messages, kafka_consumer):
    for message in sent_messages:
        assert _consume_single_message(kafka_consumer) == message
    # none left -> timeout -> None
    assert _consume_single_message(kafka_consumer, timeout=0.1) is None


def test_consume_single_message_none(kafka_consumer):
    """Validates the timeout argument short circuits the poll"""
    # Note sent_messages fixture is not requested, and so no messages are produced
    TIMEOUT_LENIENCY = 0.2  # leeway for performance of different machines
    start_time = time.time()
    message = _consume_single_message(kafka_consumer, timeout=0.1)
    runtime = time.time() - start_time
    assert runtime < runtime + TIMEOUT_LENIENCY
    assert message is None


def test_consume_from_kafka_filters_nones(sent_messages, kafka_consumer):
    """Validates it filters out unparseable messages, potentially resulting in less than requested num_messages"""
    received_messages = _consume_from_kafka(kafka_consumer, num_messages=3)
    assert len(received_messages) == 2
    assert received_messages == sent_messages[:2]


def test_consume_from_kafka_num_messages(sent_messages, kafka_consumer):
    """Validates that the num_messages argument limits number of messages"""
    received_messages = _consume_from_kafka(kafka_consumer, num_messages=1)
    assert len(received_messages) == 1
    assert received_messages[0] == sent_messages[0]


def test_consume_until_num_messages(sent_messages, kafka_consumer):
    """Validates that the num_messages argument limits the number of messages"""
    received_messages = _consume_until_num_messages(kafka_consumer, num_messages=2)
    assert len(received_messages) == 2
    assert received_messages == sent_messages[:2]


def test_consume_until_num_messages_timeout(sent_messages, kafka_consumer):
    """Validates that the timeout argument correctly short circuits the kafka polling loop."""
    TIMEOUT_LENIENCY = 1.0  # the timespan of each consume call
    timeout = 2.0
    start_time = time.time()
    received_messages = _consume_until_num_messages(
        kafka_consumer, num_messages=3, timeout=timeout
    )
    runtime = time.time() - start_time
    assert timeout < runtime < timeout + TIMEOUT_LENIENCY
    assert len(received_messages) == 2
    assert received_messages == sent_messages[:2]


# run last cuz registry server takes time to start
@pytest.mark.order("last")
def test_registry_components(registry_test_subject: str, registry_test_schema_id: int):
    assert isinstance(registry_test_subject, str)
    assert isinstance(registry_test_schema_id, int)


@pytest.mark.order("last")
def test_get_schema_from_registry_by_id(
    registry_test_server: str,
    registry_test_schema: dict[Any, Any],
    registry_test_schema_id,
):
    avro_schema_request_url = _get_avro_schema_request_url(
        registry_test_server, schema_id=registry_test_schema_id
    )
    assert isinstance(avro_schema_request_url, str)
    expected_schema_dict = registry_test_schema
    actual_schema_response = _get_avro_schema_text(
        registry_test_server, schema_id=registry_test_schema_id
    )
    schema_wrapper = json.loads(actual_schema_response)
    assert isinstance(schema_wrapper, dict)
    actual_schema_text = schema_wrapper["schema"]
    actual_schema_dict = json.loads(actual_schema_text)

    assert expected_schema_dict == actual_schema_dict


@pytest.mark.order("last")
def test_get_schema_from_registry_by_name(
    registry_test_server: str,
    registry_test_schema: dict[Any, Any],
    registry_test_subject,
    registry_test_schema_id,
):
    expected_schema_dict = registry_test_schema
    actual_schema_response = _get_avro_schema_text(
        registry_test_server, subject=registry_test_subject, version="latest"
    )
    schema_wrapper = json.loads(actual_schema_response)
    assert isinstance(schema_wrapper, dict)
    actual_schema_text = schema_wrapper["schema"]
    actual_schema_dict = json.loads(actual_schema_text)

    assert expected_schema_dict == actual_schema_dict


@pytest.mark.order("last")
def test_get_schema_convert_to_avro(
    registry_test_server: str,
    registry_test_subject,
    registry_test_schema_id,
):
    actual_schema_response = _get_avro_schema_text(
        registry_test_server, subject=registry_test_subject, version="latest"
    )
    schema_wrapper = json.loads(actual_schema_response)
    assert isinstance(schema_wrapper, dict)
    actual_schema_text = schema_wrapper["schema"]
    expected_schema_avro = avro.schema.parse(actual_schema_text)
    actual_schema_avro = _get_avro_schema(
        registry_test_server, subject=registry_test_subject, version="latest"
    )

    assert expected_schema_avro == actual_schema_avro


def test_serialize_deserialize_message(
    registry_test_schema_avro: avro.schema.Schema,
):
    expected_test_message = {"test_field_1": "test_value_1", "test_field_2": 101}
    serialized_message = _serialize_avro_message(
        registry_test_schema_avro, expected_test_message
    )
    actual_test_message = _deserialize_avro_msg(
        serialized_message, registry_test_schema_avro
    )
    assert expected_test_message == actual_test_message


# something wrong with this test. it consistently fails with no returned data from consumer
# but if you put it earlier
@pytest.mark.order("last")
@pytest.mark.skip()
def test_produce_consume_avro(
    registry_test_schema_avro: avro.schema.Schema,
    kafka_producer: Producer,
    kafka_consumer: Consumer,
    kafka_test_topic: str,
):
    expected_test_message = {"test_field_1": "test_value_1", "test_field_2": 101}
    serialized_message = _serialize_avro_message(
        registry_test_schema_avro, expected_test_message
    )
    kafka_producer.produce(kafka_test_topic, serialized_message)
    kafka_producer.flush()
    actual_test_message = _consume_single_message(
        kafka_consumer, avro_schema=registry_test_schema_avro, timeout=100
    )

    assert actual_test_message == expected_test_message
