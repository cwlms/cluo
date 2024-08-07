from typing import Optional

from cluo.core import Batch, ErrorHandlingMethod, Stage


class BatchLimitExceededException(Exception):
    """Raised when pipeline is terminated due to batch limit"""

    pass


class BatchLimitStage(Stage):
    """Raise an exception after a given number of batches."""

    def __init__(
        self,
        name: str | None = None,
        batch_limit: Optional[int] = 10,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `BatchLimitStage`.
        WARNING: IN ORDER FOR THIS STAGE TO FUNCTION PROPERLY IT MUST BE IMMEDIATELY DOWNSTREAM OF ITS SOURCE STAGE:
        (source_stage >> empty_batch_stage)

        If you must place this stage upstream of a non-source stage, that stage must be instantiated with the process_empty_batches=True parameter.

        Args:
            name (str, optional): Stage name. Defaults to class name if name = None.
            batch_limit(int, optional): Number of batches to process before raising BatchLimitExceeded exception.  Defaults to 10.  Does not raise if None or 0
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            # always one process: we want to count all batches
            processes=1,
            # always raise an exception
            error_handling_method=ErrorHandlingMethod.RAISE,
            process_empty_batches=True,
            expose_metrics=expose_metrics,
        )
        self.batch_count = 0
        self.batch_limit = batch_limit
        if self.batch_limit and self.batch_limit < 1:
            raise ValueError("Batch limit must be greater than zero")

    def process_batch(self, batch: Batch) -> Batch:
        self.batch_count += 1
        if self.batch_limit and self.batch_count > self.batch_limit:
            raise BatchLimitExceededException()
        return batch
