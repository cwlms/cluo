import logging
import time

from cluo import config
from cluo.core import Pipeline, Record
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import PythonCallableStage

config.set_logging_level(logging.INFO)

dummy_source = DummySource(
    record=Record(data={"a": 1, "b": 2, "key": "value"}),
    batch_size=10,
    expose_metrics=True,
)


def record_sleep(record):
    time.sleep(1)
    return record


time_records = PythonCallableStage(
    name="time_records", process_record=record_sleep, expose_metrics=True
)

logger_sink_enabled = LoggerSink(name="logger_sink_enabled", expose_metrics=True)

logger_sink_disabled = LoggerSink(name="logger_sink_disabled", expose_metrics=False)

dummy_source >> time_records >> [logger_sink_enabled, logger_sink_disabled]

if __name__ == "__main__":
    pipeline = Pipeline("dummy", interval=5, expose_metrics=True)
    pipeline.add_source(dummy_source)
    pipeline.run()
