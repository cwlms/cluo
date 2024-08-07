from typing import Any, Callable, Union

from cluo.core import ErrorHandlingMethod, Record, Stage


class UpdateFieldsStage(Stage):
    """Update values of fields for each record in batch."""

    def __init__(
        self,
        fields: dict[str, Union[Callable[[Any], Any], Any]],
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `UpdateFieldsStage`.

        Args:
            fields (dict[str, Union[Callable[[Any], Any], Any]]): Mapping from keys to new value or callable that takes old value as an argument and returns new value. Key must be found in original record if using callable.
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
        for key, value in self.fields.items():
            record.data[key] = value(record.data[key]) if callable(value) else value
        return record
