import logging

from cluo import config
from cluo.core import Pipeline, Record, Stage
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import RenameFieldsStage

config.set_logging_level(logging.BATCH)  # type: ignore

dummy_source = DummySource(
    record=Record(data={"a": 1, "b": 2, "key": "value"}),
    batch_size=10,
    name="my dummy source",
)
rename_stage = RenameFieldsStage(mapping={"key": "new"}, name="Key to new", processes=4)


class ExponentiationStage(Stage):
    def __init__(self, exponent: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.exponent = exponent

    def process_record(self, record: Record) -> Record:
        record.data["a"] **= self.exponent
        record.data["b"] **= self.exponent
        return record


exp_stage = ExponentiationStage(exponent=2)

logger1 = LoggerSink(name="logger1")
logger2 = LoggerSink(name="logger2")

dummy_source >> rename_stage >> exp_stage >> [logger1, logger2]

if __name__ == "__main__":
    pipeline = Pipeline("my first pipeline")
    pipeline.add_source(dummy_source)
    print(pipeline)
    # pipeline.run()
