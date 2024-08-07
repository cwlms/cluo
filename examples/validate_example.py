import logging

from cluo.config import set_logging_level
from cluo.core import Batch, Pipeline, PipelineValidationError, Record
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import RenameFieldsStage

# Define Batch
batch = Batch(records=[Record(data={"key": 12}), Record(data={"key": 13})])

# Define Stages
dummy_source = DummySource(record=Record(data={"x": 12}), batch_size=1)
log_sink = LoggerSink(name="log_1")
rename_stage = RenameFieldsStage(name="renamer", mapping={"key": "new"})

# Define Flow
dummy_source >> rename_stage

# Define Pipeline
p = Pipeline()
p.add_source(dummy_source)


if __name__ == "__main__":
    set_logging_level(logging.DEBUG)
    try:
        # This should raise a pipeline validation error due to no sink
        print(p)
    except PipelineValidationError as e:
        logging.error(type(e).__name__)
        logging.error(e)
    rename_stage >> log_sink
    # This should be fine now that we added a sink.
    print(p)
    print(p)
    # We purposefully add a cycle to the pipeline.
    log_sink >> rename_stage
    try:
        # This should raise a pipeline validation error due to a cycle
        print(p)
    except PipelineValidationError as e:
        logging.error(type(e).__name__)
        logging.error(e)
