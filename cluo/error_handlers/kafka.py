import copy
from typing import Callable, Optional

from confluent_kafka import Producer

from cluo.config import RunMode, config_state
from cluo.core import Record
from cluo.core.error_handler import BaseErrorHandler, ErrorHandlerFailureAction
from cluo.utils.log import LoggingMixin

# Type Aliases
GetMessageKeyCallable = Callable[["KafkaErrorHandler", Record], Optional[str]]


class KafkaErrorHandler(BaseErrorHandler, LoggingMixin):
    """This will send any records passed to it to the kafka service + topic specified"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        get_message_key: GetMessageKeyCallable = lambda s, r: None,
        failure_action: ErrorHandlerFailureAction = ErrorHandlerFailureAction.RAISE,
        log_records: bool = False,
    ) -> None:
        """Initialize the `KafkaErrorHandler`.

        Args:
            bootstrap_servers (str, required): Initial list of Kafka brokers.
            topic (str, required): Name of the topic that messages will be passed to.
            get_message_key (GetMessageKeyCallable, optional): A callback to generate a Kafka message key to ensure ordering. Defaults to returning None.
            failure_action (ErrorHandlerFailureAction, optional): fall-back action when error in handler execution.  Default is to raise the error exception.
            log_records (bool, optional): mirror record errors to the logging mixin.  used only for diagnostics and testing.  Defaults to False
        """
        super().__init__(failure_action=failure_action)
        self.conf = {
            "bootstrap.servers": bootstrap_servers,
        }
        self.producer: Optional[Producer] = None
        self.topic = topic
        self.get_message_key = get_message_key
        self.log_records = log_records

    def _get_producer(self) -> Producer:
        if self.producer is None:
            self.producer = Producer(self.conf)
        return self.producer

    def _record_to_kafka_message_value(self, record: Record) -> bytes:
        record_dict = copy.deepcopy(record.as_dict()["data"])
        record_dict["_metadata"] = {
            key: copy.deepcopy(value)
            for key, value in record.as_dict().items()
            if key not in ("data")
        }
        return self.serialize(record_dict).encode("utf-8")

    def process_error_record(self, record: Record) -> None:
        """
        Writes the error record to kafka
        Pushes immediately flushes after every record.
        RunMode.PREVIEW delegates to the LoggingHandler
        Args:
            record (Record): The batch.

        Returns:
            None
        """
        try:
            if config_state.RUN_MODE == RunMode.WRITE:
                producer = self._get_producer()
                message_json = self._record_to_kafka_message_value(record)
                producer.produce(
                    self.topic, message_json, self.get_message_key(self, record)
                )
                # flush immediately to ensure timely error record delivery.
                producer.flush()
            if config_state.RUN_MODE == RunMode.PREVIEW or self.log_records:
                self.log.error(self.serialize(record.as_dict()))
        except Exception as ex:
            if self.failure_action == ErrorHandlerFailureAction.RAISE:
                raise ex
            if self.failure_action == ErrorHandlerFailureAction.LOG:
                # this may raise an exception in preview mode
                # let it happen and get reported as normal
                self.log.error(self.serialize(record.as_dict()))
            elif self.failure_action != ErrorHandlerFailureAction.IGNORE:
                # fail any unexpected failure action values
                raise ex
