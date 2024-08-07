from cluo.core.exceptions import ErrorHandlingMethod
from cluo.core.stage import Stage


class Sink(Stage):
    """Core `Sink` base class. For outputting data to a final destination."""

    def __init__(
        self,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `Sink`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            name (str, optional): Sink name. Defaults to None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this sink. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
