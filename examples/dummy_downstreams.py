from cluo.core import Pipeline
from cluo.sinks import LoggerSink
from cluo.sources import KafkaSource
from cluo.stages import KeepFieldsStage, RenameFieldsStage, UpdateFieldsStage

kafka_source = KafkaSource(
    topics=["test"], bootstrap_servers="localhost:9092", batch_size=10
)

rename_stage = RenameFieldsStage(mapping={"key": "new"})

keep_fields_stage = KeepFieldsStage(keep_fields={"new"})
add_data_source = UpdateFieldsStage(fields={"data_source": "aim"})

logger1 = LoggerSink(name="logger1")
logger2 = LoggerSink(name="logger2")

kafka_source.add_downstream(rename_stage)
rename_stage.add_downstream(keep_fields_stage)
keep_fields_stage.add_downstream(add_data_source)
add_data_source.add_downstream(logger1)
add_data_source.add_downstream(logger2)

if __name__ == "__main__":
    pipeline = Pipeline("dummy")
    pipeline.add_source(kafka_source)

    print(pipeline)

    pipeline.run()
