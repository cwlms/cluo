import logging

from cluo import config
from cluo.core import Pipeline, Record
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import KeepFieldsStage, RenameFieldsStage, UpdateFieldsStage

config.set_logging_level(logging.INFO)
dummy_source = DummySource(
    record=Record(data={"a": 1, "b": 2, "key": "value"}), batch_size=10
)
rename_stage = RenameFieldsStage(mapping={"key": "new"})

keep_fields_stage = KeepFieldsStage(keep_fields={"new"})
add_data_source = UpdateFieldsStage(fields={"data_source": "aim"})

logger1 = LoggerSink(name="logger1")
# logger2 = LoggerSink(name="logger2")

(dummy_source >> rename_stage >> keep_fields_stage >> add_data_source >> logger1)

if __name__ == "__main__":
    pipeline = Pipeline("dummy", interval=5)
    pipeline.add_source(dummy_source)
    pipeline.run()
