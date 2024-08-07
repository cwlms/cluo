from cluo.core import Batch, ErrorHandlingMethod, Sink


class DiscardSink(Sink):
    """Discards all records received."""

    def __init__(
        self,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `DiscardSink`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            name (str, optional): DiscardSink name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this sink. Defaults to False.
        """
        Sink.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )

    def process_batch(self, batch: Batch) -> Batch:
        return batch
