from .discard import DiscardSink
from .kafka_sink import KafkaSink
from .logger import LoggerSink
from .postgres_append import PostgresAppendSink
from .postgres_upsert import PostgresUpsertSink

__all__ = [
    "LoggerSink",
    "PostgresAppendSink",
    "DiscardSink",
    "PostgresUpsertSink",
    "KafkaSink",
]
