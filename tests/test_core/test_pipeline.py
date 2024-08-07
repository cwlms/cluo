import logging
from time import time
from unittest.mock import patch

import pytest

from cluo.config import set_logging_level
from cluo.core import Batch, Pipeline, PipelineValidationError, Record, RecordError
from cluo.sinks import DiscardSink
from cluo.sources import DummySource
from cluo.stages import IdentityStage


def test_init():
    p = Pipeline()
    assert p.sources == []


def test_name():
    p = Pipeline()
    assert p.name == "Pipeline"

    p = Pipeline("name")
    assert p.name == "name"


def test_add_one_source():
    p = Pipeline()
    s = DummySource(record={"dummy": "record"}, batch_size=1)
    p.add_source(s)
    assert p.sources == [s]


def test_add_two_sources():
    p = Pipeline()
    s1 = DummySource(record=Record(data={"dummy": "record"}), name="s1", batch_size=1)
    p.add_source(s1)

    s2 = DummySource(record=Record(data={"dummy": "record"}), name="s2", batch_size=1)
    p.add_source(s2)
    assert p.sources == [s1, s2]


def test_single_run():
    p = Pipeline()
    s = IdentityStage()
    p._single_run(s, Batch(records=[Record(data={"dummy": "record"})]))


def test_interval(stop_pipeline_stage):
    dummy_source = DummySource(record=Record(data={"a": 3}), delay=1)
    trash = DiscardSink()
    p = Pipeline(interval=5)
    p.add_source(dummy_source)
    dummy_source >> stop_pipeline_stage >> trash
    start = time()
    with pytest.raises(RecordError):
        p.run()
        end = time()
        assert (end - start) > 5


def test_send_to_error_does_not_stop_pipeline(send_to_error_stage):
    dummy_source = DummySource(record=Record(data={"a": 3}))
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> send_to_error_stage >> trash
    p._single_run(dummy_source, Batch())


def test_stop_pipeline_does_stop_pipeline(stop_pipeline_stage):
    dummy_source = DummySource(record=Record(data={"a": 3}))
    trash = DiscardSink()
    p = Pipeline()
    p.add_source(dummy_source)
    dummy_source >> stop_pipeline_stage >> trash
    with pytest.raises(RecordError):
        p._single_run(dummy_source, Batch())


def test_validation_caches_correctly(caplog):
    set_logging_level(logging.DEBUG)
    dummy_source = DummySource(record=Record(data={"a": 3}))
    renamer = IdentityStage()
    trash_1 = DiscardSink()
    trash_2 = DiscardSink()
    dummy_source >> renamer >> trash_1
    p = Pipeline()
    p.add_source(dummy_source)
    with caplog.at_level(logging.DEBUG):
        p.validate_pipeline()
        p.validate_pipeline()
        renamer >> trash_2
        p.validate_pipeline()
    assert caplog.messages[0] == "Validating Pipeline Pipeline"
    assert (
        caplog.messages[1]
        == "Pipeline (Pipeline) is in a validated state. Skipping validation."
    )
    assert caplog.messages[2] == "Validating Pipeline Pipeline"


def test_validation_raises_on_cycle():
    dummy_source = DummySource(record=Record(data={"a": 3}))
    renamer = IdentityStage()
    trash_1 = DiscardSink()
    dummy_source >> renamer >> trash_1 >> renamer
    p = Pipeline()
    p.add_source(dummy_source)
    with pytest.raises(PipelineValidationError):
        str(p)
    with pytest.raises(PipelineValidationError):
        p.run()
    with pytest.raises(PipelineValidationError):
        p._single_run(dummy_source, Batch())


def test_validation_raises_on_unmonitored_channel():
    dummy_source = DummySource(record=Record(data={"a": 3}))
    renamer = IdentityStage()
    dummy_source >> renamer
    p = Pipeline()
    p.add_source(dummy_source)
    with pytest.raises(PipelineValidationError):
        str(p)
    with pytest.raises(PipelineValidationError):
        p.run()
    with pytest.raises(PipelineValidationError):
        p._single_run(dummy_source, Batch())


# mock the server starting, and the counters and gauges being created
@patch("cluo.core.pipeline.start_http_server")
@patch("cluo.core.pipeline.Counter")
@patch("cluo.core.pipeline.Gauge")
def test_expose_metrics(mock_gauge, mock_counter, mock_start_http_server):
    """Test that the pipeline exposes metrics when expose_metrics=True"""
    # instantiate a pipeline with expose_metrics=True
    pipeline = Pipeline(expose_metrics=True)

    # check the server was started
    mock_start_http_server.assert_called_once()

    # check the counters and gauges were created
    assert mock_counter.call_count == 3
    assert mock_gauge.call_count == 2
    assert pipeline.runs_processed is not None
    assert pipeline.records_processed is not None
    assert pipeline.records_errored is not None
    assert pipeline.pipeline_processing_time is not None
    assert pipeline.stage_processing_time is not None
