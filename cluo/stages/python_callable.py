from typing import Callable

from cluo.core import Batch, ErrorHandlingMethod, Record, Stage


class PythonCallableStage(Stage):
    """Create a pipeline stage from an arbitrary Python function."""

    def __init__(
        self,
        process_record: Callable[[Record], Record] | None = None,
        process_batch: Callable[[Batch], Batch] | None = None,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.STOP_PIPELINE,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `PythonCallableStage`. One of `process_record` or `process_batch` must be provided.

        Args:
            process_record (Callable[[Record], Record], optional): Function for processing a record. Defaults to None.
            process_batch (Callable[[Batch], Batch], optional): Function for processing a batch. Defaults to None.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self._set_processor(process_record, process_batch)

    def _set_processor(
        self,
        process_record: Callable[[Record], Record] | None = None,
        process_batch: Callable[[Batch], Batch] | None = None,
    ) -> None:
        if process_record is None and process_batch is None:
            raise ValueError("Must provide either process_record or process_batch.")
        if process_record is not None and process_batch is not None:
            raise ValueError("Cannot provide both process_record and process_batch.")
        if process_record is not None:
            self.process_record = process_record
        else:
            self.process_batch = process_batch
