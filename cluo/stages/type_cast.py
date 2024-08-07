from typing import Type, Union

from cluo.core import ErrorHandlingMethod, Record, Stage


class TypecastStage(Stage):
    """Cast the data types of values for each record in batch."""

    def __init__(
        self,
        type_mapping: dict[str, Type[Union[int, float, str, bool, bytes]]],
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.RAISE,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `TypecastStage`.

        Args:
            type_mapping (dict[str, Type[object]]): Mapping from record keys to cast function or object constructor values (e.g. str, int, float, etc.).
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
        if not all(
            isinstance(type_constructor, type)
            for type_constructor in type_mapping.values()
        ):
            raise ValueError("type_mapping values must be types.")
        self.type_mapping = type_mapping

    def process_record(self, record: Record) -> Record:
        for key, type_constructor in self.type_mapping.items():
            record.data[key] = type_constructor(record.data[key])
        return record
