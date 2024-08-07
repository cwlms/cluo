from cluo.core import ErrorHandlingMethod, Record, Stage


class KeepFieldsStage(Stage):
    """Keeps only provided fields from each record in batch."""

    def __init__(
        self,
        keep_fields: set[str],
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `KeepFieldsStage`.

        Args:
            keep_fields (set[str]): Fields to keep.
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
        self.keep_fields = keep_fields

    def process_record(self, record: Record) -> Record:
        record.data = {k: v for k, v in record.data.items() if k in self.keep_fields}
        return record
