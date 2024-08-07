import time
from types import TracebackType
from typing import Type

from avro.schema import Schema
from confluent_kafka import Consumer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from cluo._config_state import config_state  # type: ignore
from cluo.core import Batch, ErrorHandlingMethod, Record, Source
from cluo.run_mode import RunMode
from cluo.utils.kafka import (
    DEFAULT_CONFLUENT_REGISTRY,
    DEFAULT_KAFKA_SERVER,
    _consume_until_num_messages,
    _get_avro_schema,
)

KAFKA_SOURCE_CONSUME_TIMEOUT = 1.0


def _basic_consume_loop(
    consumer: Consumer,
    num_messages: int,
    timeout: float,
    avro_schema: Schema | None,
) -> list[Record]:
    """This function loops while checking for new messages in the kafka consumer.

    Args:
        consumer (confluent_kafka.Consumer): Configured consumer used to check for messages.
        num_messages (int): Max size of the batch to consume.
        timeout (float): Number of seconds to poll before exiting.
    """
    timeout = time.time() + timeout
    while time.time() < timeout:
        messages = _consume_until_num_messages(
            consumer,
            num_messages,
            timeout=KAFKA_SOURCE_CONSUME_TIMEOUT,
            avro_schema=avro_schema,
        )
        if len(messages) == 0:
            continue
        return [Record(message) for message in messages]
    return []


class KafkaSource(Source):
    """Obtains data from a Kafka topic."""

    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str = DEFAULT_KAFKA_SERVER,
        batch_size: int | None = None,
        poll_timeout: float = 30.0,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        group_id: str = "KafkaSource",
        registry_server: str = DEFAULT_CONFLUENT_REGISTRY,  # kafka avro v1 parameter: DEPRECATED
        avro_schema_name: str | None = None,  # kafka avro v1 parameter: DEPRECATED
        avro_schema_version: str
        | None = "latest",  # kafka avro v1 parameter: DEPRECATED
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `KafkaSource`.

        Args:
            topics (list[str]): Topics from which to consume.
            bootstrap_servers (str): The Kafka server to connect to. Defaults to `"localhost:9092"`.
            batch_size (int, optional): Number of records to include in each batch. Defaults to 10.
            poll_timeout (float): The number of seconds to continue polling kafka before timing out.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            group_id (str, optional): Kafka consumer group id.  Defaults to `"KafaSource"`.
            registry_server (str, optional): DEPRECATED String used to get the avro schema from a url.  Used to decode messages in avro format.
            avro_schema_name(str, optional): DEPRECATED The Avro Schema name.  When this parameter is supplied the source expects all consumed messages to be in avro binary format.  Also known as the "subject" in the confluent registry server.  All consumed messages must use the same schema.
            avro_schema_version(str, optional): DEPRECATED The Avro Schema version.  All messages consumed must use the same version.  Defaults to "latest"  Only used when avro_schema_name is supplied.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
        """
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,  # TODO: self.unique_name()
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
        self.topics = topics
        self.consumer: Consumer = None
        self.poll_timeout = poll_timeout
        self._received_messages_since_last_commit: bool = False
        self.avro_schema = None
        self.registry_server = registry_server
        self.avro_schema_name = avro_schema_name
        self.avro_schema_version = avro_schema_version

    def __del__(self) -> None:
        if self.consumer:
            self.consumer.close()

    def __enter__(self) -> None:
        if not self.consumer:
            self.consumer = Consumer(self.conf)
            self.consumer.subscribe(self.topics)
            if self.avro_schema_name and self.avro_schema_version:
                self.avro_schema = _get_avro_schema(
                    registry_server=self.registry_server,
                    subject=self.avro_schema_name,
                    version=self.avro_schema_version,
                )

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        has_exception = (
            exc_type is not None or exc_value is not None or exc_tb is not None
        )
        if (
            not has_exception
            and config_state.RUN_MODE == RunMode.WRITE
            and self.consumer
        ):
            # we can get an error if we try to commit after reading no messages.
            if self._received_messages_since_last_commit:
                self.consumer.commit(asynchronous=False)
                self._received_messages_since_last_commit = False
            return True
        return False

    def process_batch(self, batch: Batch) -> Batch:
        new_batch = Batch(
            records=_basic_consume_loop(
                consumer=self.consumer,
                num_messages=self.batch_size,
                timeout=self.poll_timeout,
                avro_schema=self.avro_schema,
            )
        )
        if len(new_batch) > 0:
            self._received_messages_since_last_commit = True
        return new_batch


class KafkaAvroSource(KafkaSource):
    """Obtains data from a Kafka topic with an avro schema."""

    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str = DEFAULT_KAFKA_SERVER,
        batch_size: int | None = None,
        poll_timeout: float = 30.0,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        group_id: str = "KafkaSource",
        registry_server: str = DEFAULT_CONFLUENT_REGISTRY,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `KafkaAvroSource`.

        Args:
            topics (list[str]): Topics from which to consume.
            bootstrap_servers (str): The Kafka server to connect to. Defaults to `"localhost:9092"`.
            batch_size (int, optional): Number of records to include in each batch. Defaults to 10.
            poll_timeout (float): The number of seconds to continue polling kafka before timing out.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            group_id (str, optional): Kafka consumer group id.  Defaults to `"KafaSource"`.
            registry_server (str, optional): String used to get the avro schema from a url.  Used to decode messages in avro format.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
        """
        KafkaSource.__init__(
            self,
            topics=topics,
            bootstrap_servers=bootstrap_servers,
            batch_size=batch_size,
            poll_timeout=poll_timeout,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            group_id=group_id,
            expose_metrics=expose_metrics,
        )
        self.registry_server = registry_server

    def __enter__(self) -> None:
        if not self.consumer:
            self.schema_registry_client = SchemaRegistryClient(
                {"url": self.registry_server}
            )
            self.avro_deserializer = AvroDeserializer(
                schema_registry_client=self.schema_registry_client
            )
            self.conf["value.deserializer"] = self.avro_deserializer
            self.consumer = DeserializingConsumer(self.conf)
            self.consumer.subscribe(self.topics)
