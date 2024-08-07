import logging

import pytest

from cluo import config
from cluo.core import Batch, Pipeline, Record
from cluo.sinks import DiscardSink
from cluo.sources import DummySource
from cluo.stages.batch_limit import BatchLimitExceededException, BatchLimitStage
from tests.test_stages.test_empty_batch import EmptyBatchSource

pytestmark = [pytest.mark.batch_limit]


def test_batch_limit_stage():
    test_batch_limit = 3
    dummy_source = DummySource(record=Record(data={"a": 1}), batch_size=5, delay=0.1)
    limit_stage = BatchLimitStage(batch_limit=test_batch_limit)
    dummy_sink = DiscardSink()
    pipeline = Pipeline()
    dummy_source >> limit_stage >> dummy_sink
    pipeline.add_source(dummy_source)
    with pytest.raises(BatchLimitExceededException) as limit_exception:
        pipeline.run()
    assert "BatchLimitExceededException" in str(limit_exception)
    assert limit_stage.batch_count == test_batch_limit + 1, "batch count"


@pytest.mark.parametrize("test_batch_limit", [0, None])
def test_batch_limit_stage_no_limit(test_batch_limit):
    config.set_logging_level(logging.ERROR)
    dummy_source = DummySource(record=Record(data={"a": 1}), batch_size=5, delay=0.001)
    limit_stage = BatchLimitStage(batch_limit=test_batch_limit)
    dummy_sink = DiscardSink()
    pipeline = Pipeline()
    dummy_source >> limit_stage >> dummy_sink
    pipeline.add_source(dummy_source)
    batch_limit = 100
    for _ in range(batch_limit):
        pipeline._single_run(dummy_source, Batch())
    assert limit_stage.batch_count == batch_limit, "batch count"


def test_batch_limit_invalid_limit():
    with pytest.raises(ValueError) as ve:
        BatchLimitStage(batch_limit=-1)
    assert "Batch limit must be greater than zero" in str(ve)


def test_batch_limit_stage_empty_batches():
    test_batch_limit = 5
    # empty batch source returns empty batches for the last two batches
    dummy_source = EmptyBatchSource(
        record=Record(data={"a": 1}),
        batch_size=5,
        delay=0.1,
        batch_limit=test_batch_limit - 2,
    )
    limit_stage = BatchLimitStage(batch_limit=test_batch_limit)
    dummy_sink = DiscardSink()
    pipeline = Pipeline()
    dummy_source >> limit_stage >> dummy_sink
    pipeline.add_source(dummy_source)
    with pytest.raises(BatchLimitExceededException) as limit_exception:
        pipeline.run()
    assert "BatchLimitExceededException" in str(limit_exception)
    assert limit_stage.batch_count == test_batch_limit + 1, "batch count"
