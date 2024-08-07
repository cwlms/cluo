import io
import json
import logging
import time
from typing import List

import avro.datafile
import avro.io
import requests
from avro.schema import Schema
from confluent_kafka import Consumer, KafkaError, Message
from requests.compat import urljoin

DEFAULT_KAFKA_SERVER = "localhost:9092"
DEFAULT_CONFLUENT_REGISTRY = "localhost:8081"
DEFAULT_CONSUMER_TIMEOUT = 1.0
DEFAULT_FLUSH_TIMEOUT = 5.0

logging.getLogger("requests").setLevel(logging.ERROR)


def _get_message_key_from_message(msg: Message) -> str | None:
    msg_key = msg.key()
    return msg_key.decode("utf-8") if isinstance(msg_key, bytes) else msg_key


def _decode_kafka_msg_value(
    msg: Message,
    avro_schema: Schema | None = None,
    include_keys: bool = False,
) -> dict | None:
    """
    parses a single Message object, handling error cases.
    if a valid Message.value() exists, tries to JSON-parse
    it into a dict and return that. otherwise returns None.

    note: this only works on messages that are strings or
          utf-8 encoded bytes
    """

    def _decode_helper(
        msg_value: str | bytes, avro_schema: Schema | None
    ) -> dict | None:
        if avro_schema and isinstance(msg_value, bytes):
            return _deserialize_avro_msg(msg_value, avro_schema=avro_schema)
        msg_str = (
            msg_value.decode("utf-8") if isinstance(msg_value, bytes) else msg_value
        )
        try:
            return json.loads(msg_str) if isinstance(msg_str, str) else msg_str
        except json.decoder.JSONDecodeError as e:
            logging.error("Failed to decode message: %s. Reason: %s", msg_str, e)
            return None

    if msg.error():
        # if we've processed all messages in this partition,
        # we don't care, we might be able to turn this behavior off with enable.partition.eof
        if msg.error().code() != KafkaError._PARTITION_EOF:
            logging.error(msg.error())
        return None
    msg_value = msg.value()
    if msg_value is None:
        return None

    decoded = _decode_helper(msg_value, avro_schema)

    if include_keys and (msg_key := _get_message_key_from_message(msg)) and decoded:
        if "_key" in decoded:
            logging.warn(
                f'kafka message body contains value for key "_key" (value redacted). Decoding logic over-wrote the value with the kafka message\'s assigned key value: ("{msg_key}")'
            )
        decoded["_key"] = msg_key

    return decoded


def _consume_single_message(
    consumer: Consumer,
    timeout: float = DEFAULT_CONSUMER_TIMEOUT,
    avro_schema: Schema | None = None,
    include_keys: bool = False,
) -> dict | None:
    c_message = consumer.poll(timeout)
    if c_message is None:
        return None
    return _decode_kafka_msg_value(
        c_message, avro_schema=avro_schema, include_keys=include_keys
    )


def _consume_from_kafka(
    consumer: Consumer,
    num_messages: int,
    timeout: float = DEFAULT_CONSUMER_TIMEOUT,
    avro_schema: Schema | None = None,
    include_keys: bool = False,
) -> List[dict]:
    """
    This function is a wrapper to `consume` with decoding
    and filtering out null/invalid messages

    Note: this can return less than num_messages messages if `consume`
    returns messages that get decoded into None
    """
    messages = consumer.consume(num_messages=num_messages, timeout=timeout)
    messages = [
        _decode_kafka_msg_value(msg, avro_schema=avro_schema, include_keys=include_keys)
        for msg in messages
    ]
    return [m for m in messages if m is not None]


def _consume_until_num_messages(
    consumer: Consumer,
    num_messages: int,
    timeout: float = DEFAULT_CONSUMER_TIMEOUT,
    avro_schema: Schema | None = None,
    include_keys: bool = False,
) -> List[dict]:
    """
    This function loops until it receives num_messages non-empty messages
    or times out.
    """
    messages: List[dict] = []
    timeout = time.time() + timeout
    while len(messages) < num_messages and time.time() < timeout:
        message = _consume_single_message(
            consumer, avro_schema=avro_schema, include_keys=include_keys
        )
        if message is not None:
            messages.append(message)
    return messages


def _deserialize_avro_msg(msg_value: bytes, avro_schema: Schema):
    """
    parses a single avro Message object, handling error cases.
    if a valid Message.value() exists, tries to deserialize with schema
    turns it into a dict and return that. otherwise returns None.

    note: this only works on messages that are strings or
          utf-8 encoded bytes
    """

    bytes_reader = io.BytesIO(msg_value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(avro_schema)

    if msg_value is None:
        return None

    try:
        decoded_msg = reader.read(decoder)
        return decoded_msg
    except Exception as e:
        logging.error("Failed to decode message: %s. Reason: %s", msg_value, e)
        return None


def _serialize_avro_message(schema: Schema, datum: dict) -> bytes:
    """
    used to generate the .avro fixture, kept for reference
    """
    # avro.datafile.DataFileWriter
    datum_writer = avro.io.DatumWriter(schema)
    buffer_writer = io.BytesIO()
    buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
    datum_writer.write(datum, buffer_encoder)
    return buffer_writer.getvalue()


def _get_avro_schema_request_url(
    registry_server: str,
    schema_id: int | None = None,
    subject: str | None = None,
    version: str | None = None,
):
    if not registry_server.startswith("http"):
        registry_server = f"http://{registry_server}"

    if schema_id:
        avro_schema_url = urljoin(registry_server, f"schemas/ids/{schema_id}")
    elif subject:
        if version:
            version_str = str(version)
        else:
            version_str = "latest"
        avro_schema_url = urljoin(
            registry_server, f"subjects/{subject}/versions/{version_str}"
        )
    else:
        raise ValueError("must supply either schema_id or subject parameter")
    return avro_schema_url


def _get_avro_schema_text(
    registry_server: str,
    schema_id: int | None = None,
    subject: str | None = None,
    version: str | None = None,
):
    response = requests.get(
        _get_avro_schema_request_url(registry_server, schema_id, subject, version),
        timeout=10,
    )
    if response.status_code == 200:
        return response.text
    else:
        raise ValueError("schema not available")


def _get_avro_schema(
    registry_server: str,
    schema_id: int | None = None,
    subject: str | None = None,
    version: str | None = None,
):
    return avro.schema.parse(
        json.loads(
            _get_avro_schema_text(registry_server, schema_id, subject, version)
        ).get("schema")
    )
