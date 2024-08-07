import decimal
import json
from datetime import date
from types import TracebackType
from typing import Callable, Optional, Type
from uuid import uuid4

from confluent_kafka import KafkaError, Producer

from cluo.config import RunMode, config_state
from cluo.core import Batch, Sink
from cluo.core.record import Record

# Type Aliases
GetMessageKeyCallable = Callable[["KafkaSink", Batch, Record], Optional[str]]


def _json_serializer(obj_to_serialize) -> str | float | int:
    if isinstance(obj_to_serialize, date):
        return obj_to_serialize.isoformat()
    elif isinstance(obj_to_serialize, decimal.Decimal):
        return float(obj_to_serialize)
    else:
        return str(obj_to_serialize)


def _record_to_kafka_message_value(record: Record) -> bytes:
    return json.dumps(record.data, default=_json_serializer).encode("utf-8")


class KafkaSink(Sink):
    """For producing messages to a Kafka topic."""

    DEFAULT_FLUSH_TIMEOUT = 5.0

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        get_message_key: GetMessageKeyCallable = lambda s, b, r: None,
        name: str | None = None,
        processes: int = 1,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `KafkaSink`.

        Args:
            bootstrap_servers (str, required): Initial list of Kafka brokers.
            topic (str, required): Name of the topic that messages will be passed to.
            flush_timeout (float, optional): Maximum time flushing messages will block.
            get_message_key (GetMessageKeyCallable, optional): A callback to generate a Kafka message key to ensure ordering. Defaults to returning None.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            expose_metrics (bool, optional): Whether or not to expose metrics for this sink. Defaults to False.
        """
        Sink.__init__(
            self, name=name, processes=processes, expose_metrics=expose_metrics
        )

        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "transactional.id": uuid4(),  # TODO: self.unique_name()
        }
        self.producer: Optional[Producer] = None

        self.topic = topic
        self.flush_timeout = flush_timeout
        self.get_message_key = get_message_key
        # used to ensure transactions are enforced
        self._inside_context = False

    def _get_producer(self) -> Producer:
        if self.producer is None:
            self.producer = Producer(self.conf)
            try:
                self.producer.init_transactions()
            except KafkaError as exc:
                if exc.args[0].retriable():
                    self.producer.init_transactions()
                else:
                    raise exc
        return self.producer

    def __enter__(self) -> None:
        self._inside_context = True
        if config_state.RUN_MODE == RunMode.WRITE:
            producer = self._get_producer()
            try:
                producer.begin_transaction()
            except KafkaError as exc:
                if exc.args[0].retriable():
                    producer.begin_transaction()
                else:
                    raise exc

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self._inside_context = False
        if config_state.RUN_MODE == RunMode.WRITE:
            producer = self._get_producer()
            has_exception = (
                exc_type is not None or exc_value is not None or exc_tb is not None
            )
            if has_exception:
                try:
                    producer.abort_transaction()
                except KafkaError as exc:
                    if exc.args[0].retriable():
                        producer.abort_transaction()
                    else:
                        raise exc
                return False

            try:
                producer.commit_transaction()
            except KafkaError as exc:
                if exc.args[0].retriable():
                    producer.commit_transaction()
                else:
                    if exc.args[0].txn_requires_abort():
                        producer.abort_transaction()
                    raise exc
        return True

    def _batch_worker(self, batch: Batch, producer: Producer) -> None:
        for record in batch.records:
            message_json = _record_to_kafka_message_value(record)
            producer.produce(
                self.topic, message_json, self.get_message_key(self, batch, record)
            )

    def process_batch(self, batch: Batch) -> Batch:
        """
        Produces one kafka message per record in the Batch.
        Blocks until all messages have been flushed.

        Args:
            batch (Batch): The batch.

        Returns:
            Batch: The batch.
        """
        if config_state.RUN_MODE == RunMode.WRITE:
            producer = self._get_producer()
            if not self._inside_context:
                with self:
                    self._batch_worker(batch, producer)
            else:
                self._batch_worker(batch, producer)
        return batch
