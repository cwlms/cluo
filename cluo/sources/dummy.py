import time
from types import TracebackType
from typing import Type

from cluo.core import Batch, ErrorHandlingMethod, Record, Source


class DummySource(Source):
    """Allows user to create arbitrary records in a batch for processing. Typically used for development and testing."""

    def __init__(
        self,
        record: Record,
        delay=1,
        batch_size: int | None = None,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `DummySource`.

        Args:
            record (Record): The record to use for each batch. This record will appear `batch_size` times in each batch returned by `DummySource.process_batch`.
            batch_size (int, optional): Number of records to include in batch. Defaults to 10.
            delay (int, optional): Seconds to wait before returning a new batch for processing in the pipeline. Simulates I/O lag. Defaults to 1.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
        """
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.record = record
        self.delay = delay

    def process_batch(self, batch: Batch | None) -> Batch:
        time.sleep(self.delay)
        return Batch(
            records=[
                Record(data=self.record.data.copy()) for _ in range(self.batch_size)
            ]
        )

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        pass
