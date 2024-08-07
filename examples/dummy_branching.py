from cluo.core import Batch, Pipeline, Record
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import BranchStage, RenameFieldsStage

# Define Batch
batch = Batch(records=[Record(data={"key": 12}), Record(data={"key": 13})])

# Define Stages
dummy_source = DummySource(record=Record(data={"x": 12}), batch_size=1)
branch_stage = BranchStage(
    name="test",
    channel_mapping={
        "hello": lambda x: x.data["key"] == 13,
        "goodbye": lambda x: True,
    },
    single_channel=True,
)
log_1 = LoggerSink(name="log_1")
log_2 = LoggerSink(name="log_2")
log_3 = LoggerSink(name="log_3")
rename_stage = RenameFieldsStage(name="renamer", mapping={"key": "new"})

# Define Pipeline
dummy_source >> branch_stage
branch_stage >> {"hello": rename_stage, "goodbye": [log_1, log_2]}
rename_stage >> [log_2, log_3]

p = Pipeline()
p.add_source(dummy_source)

if __name__ == "__main__":
    print(p)
    print(batch)
    p._single_run(branch_stage, batch)
