from cluo.core import Batch, ErrorHandlingMethod, Stage


class EmptyBatchException(Exception):
    """Raised when stage processes an empty batch"""

    pass


class EmptyBatchStage(Stage):
    """Raise an exception on an empty batch."""

    def __init__(self, name: str | None = None, expose_metrics: bool = False) -> None:
        """Initialize the `EmptyBatchStage`.
        WARNING: IN ORDER FOR THIS STAGE TO FUNCTION PROPERLY IT MUST BE IMMEDIATELY DOWNSTREAM OF ITS SOURCE STAGE:
        (source_stage >> empty_batch_stage)

        If you must place this stage upstream of a non-source stage, that stage must be instantiated with the process_empty_batches=True parameter.

        Args:
            name (str, optional): Stage name. Defaults to class name if name = None.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            # always one process: first empty batch
            processes=1,
            # always raise an exception
            error_handling_method=ErrorHandlingMethod.RAISE,
            # force the stage to be called even when the batch is empty
            process_empty_batches=True,
            expose_metrics=expose_metrics,
        )

    def process_batch(self, batch: Batch) -> Batch:
        if len(batch) == 0:
            raise EmptyBatchException()
        return batch
