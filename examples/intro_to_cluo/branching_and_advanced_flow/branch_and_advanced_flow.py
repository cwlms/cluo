from cluo.core import Pipeline, Record
from cluo.sinks import LoggerSink
from cluo.sources import KafkaSource
from cluo.stages import BranchStage, UpdateFieldsStage

# Define our stages

# Kafka Source
kafka_source = KafkaSource(
    topics=["test.branchingpipeline"],
    bootstrap_servers="localhost:9092",
    batch_size=1,
    name="test_kafka_source",
)
# Update Fields Stage
update_a_stage = UpdateFieldsStage(fields={"a": "goodbye"}, name="set_a_to_goodbye")

# Conditional Branching Stage
"""
Let's make this stage so that it has one path/channel if the field b = 12
and another path/channel for all other cases
"""


def b_is_12(record: Record) -> bool:
    if record.data.get("b", None) == 12:
        return True
    return False


branch_stage = BranchStage(
    channel_mapping={"is_12": b_is_12, "is_not_12": lambda x: True},
    single_channel=True,
    name="branch_b_is_12",
)

# Logger Sink
logger_sink_1 = LoggerSink(name="logger_sink_1")
logger_sink_2 = LoggerSink(name="logger_sink_2")
logger_sink_3 = LoggerSink(name="logger_sink_3")

# Define our flow

kafka_source >> branch_stage
branch_stage >> {"is_12": update_a_stage, "is_not_12": [logger_sink_1, logger_sink_2]}
update_a_stage >> [logger_sink_2, logger_sink_3]

"""
This is equivalent to:
> kafka_source.add_downstream(branch_is_12)
> branch_is_12.add_downstream(update_a_stage, channel='is_12')
> branch_is_12.add_downstream(logger_sink_1, channel='is_not_12')
> branch_is_12.add_downstream(logger_sink_2, channel='is_not_12')
> update_a_stage.add_downstream(logger_sink_2)
> update_a_stage.add_downstream(logger_sink_3)

Some Notes:
* We use lists to add multiple downstream stages in a single expression.
* We use dictionaries to assign stages to a specific channel.
* All stages expose a single channel 'default' unless otherwise configured.
* Branch stages are configured with multiple channels.

This means the following expressions are all equivalent:
> kafka_source >> branch_is_12
> kafka_source >> [branch_is_12]
> kafka_source >> {'default': branch_is_12}
> kafka_source >> {'default': [branch_is_12]}
> kafka_source.add_downstream(branch_is_12)
> kafka_source.add_downstream(branch_is_12, channel='default')
"""


# Define our pipeline and add stages

pipeline = Pipeline(name="branching_pipeline")
pipeline.add_source(kafka_source)


if __name__ == "__main__":
    print(pipeline)
    pipeline.run()
