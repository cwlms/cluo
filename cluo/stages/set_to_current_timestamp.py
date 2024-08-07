import datetime

from cluo.core import Record, Stage
from cluo.core.exceptions import ErrorHandlingMethod


class SetToCurrentTimestampStage(Stage):
    """Set fields to the current timestamp."""

    def __init__(
        self,
        fields: list[str],
        name: str | None = None,
        processes: int = 1,
        error_handling_method=ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `SetToCurrentTimestampStage`.

        Args:
            fields (list[str]): The fields to set to the current timestamp.
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
        self.fields = fields

    def process_record(self, record: Record) -> Record:
        timestamp = datetime.datetime.now()
        for field in self.fields:
            record.data[field] = timestamp
        return record
