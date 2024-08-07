from cluo.core import ErrorHandlingMethod, Record, Stage


class RenameFieldsStage(Stage):
    """Renames fields in each record in batch."""

    def __init__(
        self,
        mapping: dict[str, str],
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `RenameFieldsStage`.

        Args:
            mapping (dict[str, str]): Mapping from old field names to new field names.
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

        self.mapping = mapping

    def process_record(self, record: Record) -> Record:
        record.data = {self.mapping.get(k, k): v for k, v in record.data.items()}
        return record
