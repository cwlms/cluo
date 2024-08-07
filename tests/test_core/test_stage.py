import time
from types import TracebackType
from typing import Type
from unittest.mock import call, patch

import pytest

from cluo.core import Batch, ErrorHandlingMethod, Pipeline, Record, Source, Stage
from cluo.sinks import DiscardSink
from cluo.sources import DummySource

pytestmark = [
    pytest.mark.stage,
]


class ConcreteStage(Stage):
    def __init__(self, name=None, processes=1, expose_metrics=False) -> None:
        super().__init__(name=name, processes=processes, expose_metrics=expose_metrics)

    def process_batch(self, batch: Batch) -> Batch:
        return batch


class AnotherStage(Stage):
    def __init__(self, name=None, processes=1, expose_metrics=False) -> None:
        super().__init__(name=name, processes=processes, expose_metrics=expose_metrics)

    def process_batch(self, batch: Batch) -> Batch:
        return batch


def test_incrementing_id():
    # WARNING: This test test must be at the top of the file to run correctly, otherwise the .id values will be too high.
    stage1 = ConcreteStage(name="one")
    stage2 = ConcreteStage(name="two")
    stage3 = ConcreteStage(name="three")
    stage4 = AnotherStage(name="four")

    assert stage1.id == 1
    assert stage2.id == 2
    assert stage3.id == 3
    assert stage4.id == 1


def test_stage_init():
    stage = ConcreteStage(name=None)
    assert stage.name == "ConcreteStage"


def test_stage_name():
    stage = ConcreteStage(name="1")
    assert stage.name == "1"


def test_stage_add_downstream():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage1.add_downstream(stage2)
    assert len(stage1.get_downstreams()) == 1
    assert stage1.get_downstreams()[0].name == "2"


def test_stage_add_updstream():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage1.add_upstream(stage2)
    assert len(stage2.get_downstreams()) == 1
    assert stage2.get_downstreams()[0].name == "1"


def test_bitshift_right_single_stage():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage1 >> stage2
    assert len(stage1.get_downstreams()) == 1
    assert stage1.get_downstreams()[0].name == "2"


def test_bitshift_left_single_stage():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage1 << stage2
    assert len(stage2.get_downstreams()) == 1
    assert stage2.get_downstreams()[0].name == "1"


def test_bitshift_right_multiple_stages_fan_out():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    stage1 >> [stage2, stage3]
    assert len(stage1.get_downstreams()) == 2
    assert stage1.get_downstreams()[0].name == "2"
    assert stage1.get_downstreams()[1].name == "3"


def test_bitshift_right_multiple_stages_fan_in():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    [stage1, stage2] >> stage3
    assert len(stage1.get_downstreams()) == 1
    assert stage1.get_downstreams()[0].name == "3"
    assert len(stage2.get_downstreams()) == 1
    assert stage2.get_downstreams()[0].name == "3"


def test_bitshift_left_multiple_stages_fan_in():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    stage1 << [stage2, stage3]
    assert len(stage2.get_downstreams()) == 1
    assert stage2.get_downstreams()[0].name == "1"
    assert len(stage3.get_downstreams()) == 1
    assert stage2.get_downstreams()[0].name == "1"


def test_bitshift_left_multiple_stages_fan_out():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    [stage1, stage2] << stage3
    assert len(stage3.get_downstreams()) == 2
    assert stage3.get_downstreams()[0].name == "1"
    assert len(stage3.get_downstreams()) == 2
    assert stage3.get_downstreams()[1].name == "2"


def test_sequential_bitshift_right():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    stage1 >> stage2 >> stage3
    assert stage1.get_downstreams()[0].name == "2"
    assert stage2.get_downstreams()[0].name == "3"
    assert len(stage3.get_downstreams()) == 0


def test_sequential_bitshift_left():
    stage1 = ConcreteStage(name="1")
    stage2 = ConcreteStage(name="2")
    stage3 = ConcreteStage(name="3")
    stage1 << stage2 << stage3
    assert stage3.get_downstreams()[0].name == "2"
    assert stage2.get_downstreams()[0].name == "1"
    assert len(stage1.get_downstreams()) == 0


def test_processes_arg_validation():
    with pytest.raises(ValueError):
        ConcreteStage(name="1", processes=10)


class MPTestStage(Stage):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def process_record(self, record: Record) -> Record:
        time.sleep(0.01)
        return record


def test_pipeline_attribute():
    dummy_source = DummySource(name="my_dummy_source", record=Record(data={"a": 1}))
    concrete_stage = ConcreteStage(name="my_concrete_stage")
    pipeline = Pipeline(name="my_pipeline")

    dummy_source >> concrete_stage
    pipeline.add_source(dummy_source)

    assert isinstance(concrete_stage.pipeline, Pipeline)
    assert isinstance(dummy_source.pipeline, Pipeline)


def test_stage_unique_name():
    dummy_source = DummySource(name="my_dummy_source", record=Record(data={"a": 1}))
    concrete_stage = ConcreteStage(name="my_concrete_stage")
    pipeline = Pipeline(name="my_pipeline")

    dummy_source >> concrete_stage
    pipeline.add_source(dummy_source)

    assert "my_dummy_source" in dummy_source.unique_name
    assert "my_pipeline" in dummy_source.unique_name


class ErrorBranchStage(Stage):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def process_batch(self, batch: Batch) -> Batch:
        return batch

    def get_channels(self, record: Record) -> list[str]:
        raise Exception()
        return ["default"]


def test_process_batch_get_channel_error_is_raised():
    es = ErrorBranchStage(error_handling_method=ErrorHandlingMethod.RAISE)
    with pytest.raises(Exception):
        es(Batch(records=[Record(data={"a": 2})]))


def test_process_batch_get_channel_error_is_handled():
    es_1 = ErrorBranchStage(error_handling_method=ErrorHandlingMethod.STOP_PIPELINE)
    new_batch = es_1(Batch(records=[Record(data={"a": 2})]))
    assert (
        new_batch.records[0].exceptions[0].handling_method
        == ErrorHandlingMethod.STOP_PIPELINE
    )
    es_2 = ErrorBranchStage(error_handling_method=ErrorHandlingMethod.SEND_TO_ERROR)
    new_batch = es_2(Batch(records=[Record(data={"a": 2})]))
    assert (
        new_batch.records[0].exceptions[0].handling_method
        == ErrorHandlingMethod.SEND_TO_ERROR
    )


class ErrorBranchProcessRecordStage(Stage):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def process_record(self, record: Record) -> Batch:
        return record

    def get_channels(self, record: Record) -> list[str]:
        raise Exception()
        return ["default"]


def test_process_record_get_channel_error_is_raised():
    es = ErrorBranchProcessRecordStage(error_handling_method=ErrorHandlingMethod.RAISE)
    with pytest.raises(Exception):
        es(Batch(records=[Record(data={"a": 2})]))


def test_process_batch_get_channel_error_is_handled_errorbranchprocessrecordstage():
    es_1 = ErrorBranchProcessRecordStage(
        error_handling_method=ErrorHandlingMethod.STOP_PIPELINE
    )
    new_batch = es_1(Batch(records=[Record(data={"a": 2})]))
    assert (
        new_batch.records[0].exceptions[0].handling_method
        == ErrorHandlingMethod.STOP_PIPELINE
    )
    es_2 = ErrorBranchProcessRecordStage(
        error_handling_method=ErrorHandlingMethod.SEND_TO_ERROR
    )
    new_batch = es_2(Batch(records=[Record(data={"a": 2})]))
    assert (
        new_batch.records[0].exceptions[0].handling_method
        == ErrorHandlingMethod.SEND_TO_ERROR
    )


class EmptyRecordProcessStage(Stage):
    """
    stub stage that captures that number of records processed
    used to verify that when stage.process_empty_records=True
    the stage is still called
    In the short term, we will use this mechanism to raise a
    stop pipeline exception when
    the streaming processor encounters an empty batch
    This allows us to run a cluo streaming pipeline in a
    batch context
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.batch_size: int = -1

    def process_batch(self, batch: Batch) -> Batch:
        self.batch_size = len(batch.records)
        return batch


class EmptyBatchSource(Source):
    """source that returns an empty batch"""

    def __init__(
        self,
        batch_size: int | None = None,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
    ) -> None:
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
        )

    def process_batch(self, batch: Batch) -> Batch:
        return Batch(records=[])

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        pass


def test_process_empty_batches_flag():
    dummy_source = EmptyBatchSource()
    erps_stage = EmptyRecordProcessStage(process_empty_batches=True)
    dummy_sink = DiscardSink()
    pipeline = Pipeline()
    dummy_source >> erps_stage >> dummy_sink
    pipeline.add_source(dummy_source)
    assert erps_stage.batch_size == -1, "pre-run batch size"
    pipeline._single_run(dummy_source, Batch())
    assert erps_stage.batch_size == 0, "stage called for empty batch"


def record_sleep(record):
    time.sleep(1)
    return record


# mock the server starting, and the counters
@patch("cluo.core.pipeline.start_http_server")
@patch("cluo.core.pipeline.Counter")
@patch("cluo.core.pipeline.Gauge")
def test_expose_metrics(mock_gauge, mock_counter, mock_start_http_server):
    from cluo.stages import PythonCallableStage

    """Test that the counters and gauges are created and incremented when expose_metrics=True for a stage"""
    # set up stages and their connections
    dummy_source = DummySource(record=Record(data={"a": 3}))
    time_records = PythonCallableStage(process_record=record_sleep, expose_metrics=True)
    stage = ConcreteStage(expose_metrics=True)
    dummy_sink = DiscardSink()
    dummy_source >> time_records >> stage >> dummy_sink

    # set up pipeline and run it
    pipeline = Pipeline(expose_metrics=True)
    pipeline.add_source(dummy_source)
    pipeline._single_run(dummy_source, Batch())

    # check the server was started (optional, already tested)
    assert mock_start_http_server.call_count == 1

    # check the counters were created and incremented
    calls = [
        call(
            "cluo_runs_processed",
            "Number of runs processed by the pipeline",
            ["pipeline_name"],
        ),
        call(
            "cluo_records_processed",
            "Number of records processed by the pipeline",
            ["pipeline_name", "stage_name"],
        ),
        call(
            "cluo_records_errored",
            "Number of records errored by the pipeline",
            ["pipeline_name", "stage_name"],
        ),
        call().labels(
            pipeline_name="Pipeline", stage_name="PythonCallableStage"
        ),  # only stage in pipeline with expose_metrics=True
        call().labels().inc(10),  # 10 records processed
        call().labels(
            pipeline_name="Pipeline", stage_name="ConcreteStage"
        ),  # only stage in pipeline with expose_metrics=True
        call().labels().inc(10),  # 10 records processed
    ]

    mock_counter.assert_has_calls(calls, any_order=False)

    # Check calls to create gauges
    mock_gauge.assert_has_calls(
        [
            call(
                "cluo_pipeline_processing_time",
                "Seconds taken to process pipeline",
                ["pipeline_name"],
            ),
            call(
                "cluo_stage_processing_time",
                "Seconds taken to process a stage",
                ["stage_name"],
            ),
        ],
        any_order=False,
    )

    # Check calls to set gauge values
    assert mock_gauge.return_value.labels.call_args_list == [
        call(stage_name="PythonCallableStage"),
        call(stage_name="ConcreteStage"),
    ]

    assert mock_gauge.return_value.labels().set.call_count == 2

    # Checks if set calls are not None
    for calls in mock_gauge.return_value.labels().set.call_args_list:
        assert calls[0] is not None
