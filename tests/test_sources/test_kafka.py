import time
from json import dumps

import pytest
from avro.schema import Schema
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient

from cluo.config import RunMode, set_run_mode
from cluo.core import Batch, Record
from cluo.sources import KafkaAvroSource, KafkaSource
from cluo.utils.kafka import _serialize_avro_message

pytestmark = [pytest.mark.kafka_sources]
DEBUG = False


def setup_module(module):
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


def test_kafka_batch(kafka_producer: Producer, kafka_test_topic: str):
    """Validates kafka source correctly batches"""
    kafka_producer.produce(kafka_test_topic, dumps({"a": 1}))
    kafka_producer.produce(kafka_test_topic, dumps({"b": 2}))
    kafka_producer.flush()
    k = KafkaSource([kafka_test_topic], batch_size=2)
    test_batch = Batch([Record({"a": 1}), Record({"b": 2})])
    with k:
        new_batch = k(Batch([]))
        assert new_batch.has_same_record_data(test_batch)


def test_kafka_avro_v1(
    kafka_producer: Producer,
    kafka_test_topic: str,
    registry_test_schema_avro: Schema,
    registry_test_subject: str,
    registry_test_schema_id: int,
):
    """
    Validates KafkaSource consumes avro messages
    """

    test_messages = [
        {"test_field_1": "test_value_1", "test_field_2": 101},
        {"test_field_1": "test_value_1.2", "test_field_2": -1},
    ]
    test_batch = Batch([Record(test_message) for test_message in test_messages])
    serialized_messages = [
        _serialize_avro_message(registry_test_schema_avro, test_message)
        for test_message in test_messages
    ]
    for message in serialized_messages:
        kafka_producer.produce(kafka_test_topic, message)
    kafka_producer.flush()
    k = KafkaSource(
        [kafka_test_topic],
        batch_size=2,
        avro_schema_name=registry_test_subject,
    )
    with k:
        new_batch = k(Batch([]))
        assert new_batch.has_same_record_data(test_batch)


def test_kafka_avro(
    kafka_avro_producer: AvroProducer,
    kafka_test_topic: str,
    registry_test_server: str,
    registry_test_schema_id: int,
):
    """Validates KafkaAvroSource consumes avro messages"""

    test_messages = [
        {"test_field_1": "test_value_1", "test_field_2": 101},
        {"test_field_1": "test_value_1.2", "test_field_2": -1},
    ]
    test_batch = Batch([Record(test_message) for test_message in test_messages])
    schema_registry_client = SchemaRegistryClient({"url": registry_test_server})
    schema_str = schema_registry_client.get_schema(
        schema_id=registry_test_schema_id
    ).schema_str

    for message in test_messages:
        kafka_avro_producer.produce(
            topic=kafka_test_topic,
            value=message,
            value_schema=schema_str,
        )
    kafka_avro_producer.flush()
    k = KafkaAvroSource(
        [kafka_test_topic], batch_size=2, registry_server=registry_test_server
    )
    with k:
        new_batch = k(Batch([]))
        assert new_batch.has_same_record_data(test_batch)


def test_kafka_offset_write_mode(kafka_producer: Producer, kafka_test_topic: str):
    """Validates kafka source context manager correctly commits offset in write mode."""
    set_run_mode(RunMode.WRITE)
    kafka_producer.produce(kafka_test_topic, dumps({"a": 1}))
    kafka_producer.produce(kafka_test_topic, dumps({"b": 2}))
    kafka_producer.flush()

    k = KafkaSource([kafka_test_topic], batch_size=1)
    with k:
        assert {"a": 1} == k(Batch([])).records[0].data

    k = KafkaSource([kafka_test_topic], batch_size=1)
    with k:
        assert {"b": 2} == k(Batch([])).records[0].data


def test_kafka_offset_preview_mode(kafka_producer: Producer, kafka_test_topic: str):
    """Validates kafka source context manager does not commit offset in preview mode."""
    set_run_mode(RunMode.PREVIEW)
    kafka_producer.produce(kafka_test_topic, dumps({"a": 1}))
    kafka_producer.produce(kafka_test_topic, dumps({"b": 2}))
    kafka_producer.flush()

    k = KafkaSource([kafka_test_topic], batch_size=1)
    with k:
        assert {"a": 1} == k(Batch([])).records[0].data

    k = KafkaSource([kafka_test_topic], batch_size=1)
    with k:
        assert {"a": 1} == k(Batch([])).records[0].data


def test_kafka_offset_error(kafka_producer: Producer, kafka_test_topic: str):
    """Validates kafka source context manager correctly keeps offset on error"""
    set_run_mode(RunMode.WRITE)
    kafka_producer.produce(kafka_test_topic, dumps({"a": 1}))
    kafka_producer.flush()

    k = KafkaSource([kafka_test_topic], batch_size=1)
    try:
        with k:
            assert {"a": 1} == k(Batch([])).records[0].data
            raise RuntimeError()
    except Exception:  # noqa: S110
        pass

    k = KafkaSource([kafka_test_topic], batch_size=1)
    with k:
        assert {"a": 1} == k(Batch([])).records[0].data


def test_kafka_timeout(kafka_test_topic: str):
    """Validates that the timeout argument correctly short circuits the kafka polling loop."""
    TIMEOUT_LENIENCY = 1.0  # allow 1 seconds for the context manager cleanup
    timeout = 2.0
    start_time = time.time()
    k = KafkaSource([kafka_test_topic], batch_size=2, poll_timeout=timeout)
    with k:
        new_batch = k(Batch())
    runtime = time.time() - start_time
    assert timeout < runtime < timeout + TIMEOUT_LENIENCY
    assert len(new_batch) == 0


def test_decode_messages_filters_nones(kafka_producer: Producer, kafka_test_topic: str):
    encoded_messages = [b'{"foo": "bar"}', '{"boo": "baz"}', "", "{thats not json"]
    for message in encoded_messages:
        kafka_producer.produce(kafka_test_topic, message)
    kafka_producer.flush()

    k = KafkaSource([kafka_test_topic], batch_size=4)
    test_batch = Batch([Record({"foo": "bar"}), Record({"boo": "baz"})])
    with k:
        new_batch = k(Batch([]))
        assert new_batch.has_same_record_data(test_batch)
