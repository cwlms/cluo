import time
from types import TracebackType
from typing import Type

import pytest

from cluo.core import Batch, ErrorHandlingMethod, Pipeline, Record, Source
from cluo.sinks import DiscardSink
from cluo.stages.empty_batch import EmptyBatchException, EmptyBatchStage

pytestmark = [pytest.mark.empty_batch]


class EmptyBatchSource(Source):
    """source that returns an empty batch after a certain number of batches"""

    def __init__(
        self,
        record: Record,
        delay=1,
        batch_size: int | None = None,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        batch_limit: int = 1,
    ) -> None:
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
        )
        self.record = record
        self.delay = delay
        self.batch_count = 0
        self.batch_limit = batch_limit

    def process_batch(self, batch: Batch) -> Batch:
        self.batch_count += 1
        time.sleep(self.delay)
        if self.batch_count <= self.batch_limit:
            return Batch(records=[self.record for _ in range(self.batch_size)])
        else:
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


def test_empty_batch_stage():
    test_batch_limit = 3
    dummy_source = EmptyBatchSource(
        record=Record(data={"a": 1}),
        batch_size=5,
        delay=0.1,
        batch_limit=test_batch_limit,
    )
    empty_stage = EmptyBatchStage()
    dummy_sink = DiscardSink()
    pipeline = Pipeline()
    dummy_source >> empty_stage >> dummy_sink
    pipeline.add_source(dummy_source)
    with pytest.raises(EmptyBatchException) as empty_exception:
        pipeline.run()
    assert "EmptyBatchException" in str(empty_exception)
