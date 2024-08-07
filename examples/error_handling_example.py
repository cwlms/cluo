from cluo.core import Batch, Pipeline, Record
from cluo.core.exceptions import ErrorHandlingMethod
from cluo.error_handlers import LoggingHandler
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import PythonCallableStage


def raise_if_x_12(record: Record) -> Record:
    if record.data["x"] == 12:
        raise ValueError("!")
    return record


batch = Batch(records=[Record(data={"x": 1}), Record(data={"x": 12})])

"""
We will make this pipeline error out if the value of 'x' is 12.
"""

dummy_source = DummySource(record=Record(data={"x": 12}), batch_size=1)
error_stage = PythonCallableStage(
    process_record=raise_if_x_12,
    error_handling_method=ErrorHandlingMethod.SEND_TO_ERROR,
)
log_stage = LoggerSink()
log_handler = LoggingHandler()

dummy_source >> error_stage >> log_stage
p = Pipeline(name="error_pipeline")
p.add_source(dummy_source)
p.add_error_handler(log_handler)


if __name__ == "__main__":
    print(batch.records)
    print(p)
    p._single_run(error_stage, batch)
