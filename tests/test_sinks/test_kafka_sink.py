import decimal
from datetime import date, datetime, timezone

import pytest
from confluent_kafka import Consumer

from cluo.config import set_run_mode
from cluo.core.batch import Batch, Record
from cluo.run_mode import RunMode
from cluo.sinks.kafka_sink import KafkaSink
from cluo.utils.kafka import _consume_from_kafka, _consume_until_num_messages

pytestmark = [pytest.mark.kafka_sink]
DEBUG = False


def setup_module(module):
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


def test_doesnt_produce_in_preview_mode(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    set_run_mode(RunMode.PREVIEW)
    batch = Batch([Record({"a": 1}), Record({"b": 2})])
    s = KafkaSink(bootstrap_servers=kafka_test_servers, topic=kafka_test_topic)
    with s:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_from_kafka(kafka_consumer, 1)
    assert messages == []


@pytest.mark.order("last")
def test_produces_kafka_message(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    set_run_mode(RunMode.WRITE)
    s = KafkaSink(bootstrap_servers=kafka_test_servers, topic=kafka_test_topic)
    batch = Batch([Record({"a": 1}), Record({"b": 2})])
    with s:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_until_num_messages(kafka_consumer, num_messages=2, timeout=5)
    assert messages == [{"a": 1}, {"b": 2}]


@pytest.mark.order("last")
def test_encodes_complex_types(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    set_run_mode(RunMode.WRITE)
    s = KafkaSink(bootstrap_servers=kafka_test_servers, topic=kafka_test_topic)
    input_data = {
        "decimal": decimal.Decimal(1.5),
        "date": date(2000, 1, 1),
        "datetime": datetime(2020, 1, 1, 0, 0, 0, 0, timezone.utc),
        "other": Exception({"x": "y"}),
    }
    expected_data = {
        "decimal": 1.5,
        "date": "2000-01-01",
        "datetime": "2020-01-01T00:00:00+00:00",
        "other": "{'x': 'y'}",
    }

    batch = Batch([Record(input_data), Record({"b": 2})])
    with s:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_until_num_messages(kafka_consumer, num_messages=2, timeout=5)
    assert messages == [expected_data, {"b": 2}]


@pytest.mark.order("last")
def test_transaction(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    """
    Validates messages don't get produced if an exception is encountered
    """
    set_run_mode(RunMode.WRITE)

    batch = Batch([Record({"a": 1}), Record({"b": 2})])

    # Make sure no messages get produced if an exception is encountered
    s = KafkaSink(bootstrap_servers=kafka_test_servers, topic=kafka_test_topic)
    try:
        with s:
            outgoing_batch = s(batch)
            assert outgoing_batch.has_same_record_data(batch)
            raise Exception("Timmy's fallen into a well!")
    except Exception:  # noqa: S110
        pass
    messages = _consume_from_kafka(kafka_consumer, num_messages=2, timeout=1)
    assert messages == []

    # Make sure it picks back up where it left off
    with s:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_until_num_messages(kafka_consumer, num_messages=2, timeout=5)
    assert messages == [{"a": 1}, {"b": 2}]


@pytest.mark.order("last")
def test_transaction_without_context_manager(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    """
    Validates messages don't get produced if an exception is encountered
    """
    set_run_mode(RunMode.WRITE)

    batch = Batch([Record({"a": 1}), Record({"b": 2})])

    # Make sure no messages get produced if an exception is encountered
    s = KafkaSink(bootstrap_servers=kafka_test_servers, topic=kafka_test_topic)
    try:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
        raise Exception("Timmy's fallen into a well!")
    except Exception:  # noqa: S110
        pass
    messages = _consume_from_kafka(kafka_consumer, num_messages=2, timeout=1)
    assert messages == []

    # Make sure it picks back up where it left off
    outgoing_batch = s(batch)
    assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_until_num_messages(kafka_consumer, num_messages=2, timeout=5)
    assert messages == [{"a": 1}, {"b": 2}]


@pytest.mark.order("last")
def test_produces_with_key(
    kafka_test_servers: str, kafka_test_topic: str, kafka_consumer: Consumer
):
    set_run_mode(RunMode.WRITE)

    def get_message_key(s: KafkaSink, b: Batch, r: Record) -> str | None:
        return r.data.get("id")

    s = KafkaSink(
        bootstrap_servers=kafka_test_servers,
        topic=kafka_test_topic,
        get_message_key=get_message_key,
    )
    batch = Batch([Record({"a": 1, "id": "1"}), Record({"b": 2, "id": "2"})])
    with s:
        outgoing_batch = s(batch)
        assert outgoing_batch.has_same_record_data(batch)
    messages = _consume_until_num_messages(
        kafka_consumer, num_messages=2, timeout=5, include_keys=True
    )
    assert messages == [
        {"a": 1, "id": "1", "_key": "1"},
        {"b": 2, "id": "2", "_key": "2"},
    ]
