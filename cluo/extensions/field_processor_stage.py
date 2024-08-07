from abc import ABC, abstractmethod
from typing import Any, Iterable

from cluo.core import ErrorHandlingMethod, Record, Stage
from cluo.core.exceptions import StageError


class FieldProcessorStage(ABC, Stage):
    """Extension of 'stage' to be implemented by stages that require operations on the 'fields' array only"""

    def __init__(
        self,
        fields: Iterable[str] | None = None,
        allow_nulls: bool = True,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `EmptyStringNullifierStage`.

        Args:
            fields (Iterable[str], optional): Fields to operate on. Will operate on all fields if not set (None).
            allow_nulls (bool, optional): Allow null values/fields if True, otherwise use error_handling_method
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
        self.allow_nulls = allow_nulls

    def process_record(self, record: Record) -> Record:
        for field in self.fields if self.fields is not None else record.data.keys():
            field_value = record.data.get(field)
            if field not in record.data.keys():
                continue
            elif field_value is not None:
                record.data[field] = self.process_field(field, field_value)
            elif not self.allow_nulls:
                raise StageError(
                    stage_name=self.name,
                    message=f"null value for field: {field}",
                    handling_method=self.error_handling_method,
                )
        return record

    @abstractmethod
    def process_field(self, field: Any, field_value: Any) -> Any:
        raise NotImplementedError("process_field must be implemented in subclass")
