from cluo.core import Pipeline
from cluo.sinks import LoggerSink
from cluo.sources import KafkaSource
from cluo.stages import UpdateFieldsStage

# Define our stages

# Kafka Source
kafka_source = KafkaSource(
    topics=["test.myfirstpipeline"],
    bootstrap_servers="localhost:9092",
    batch_size=1,
    name="test_kafka_source",
)
# Update Fields Stage
update_a_stage = UpdateFieldsStage(fields={"a": "goodbye"}, name="set_a_to_goodbye")
# Logger Sink
logger_sink = LoggerSink(name="logger_sink")

# Define our flow

kafka_source >> update_a_stage >> logger_sink

"""
This is equivalent to
> kafka_source.add_downstream(update_a_stage)
> update_a_stage.add_downstream(logger_sink)
"""

# Define our pipeline and add stages

pipeline = Pipeline(name="my_first_pipeline")
pipeline.add_source(kafka_source)


if __name__ == "__main__":
    print(pipeline)
    pipeline.run()
