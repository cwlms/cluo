from typing import Any, Iterable

from cluo.core import ErrorHandlingMethod
from cluo.extensions.field_processor_stage import FieldProcessorStage


class CleanCurrencyStage(FieldProcessorStage):
    """Cleans currency values. Strips whitespace and converts to uppercase."""

    def __init__(
        self,
        fields: Iterable[str] | None = None,
        allow_nulls: bool = True,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize `CleanCurrencyStage`.

        Args:
            fields (Iterable[str], optional): Fields to operate on. Will operate on all fields if not set (None).
            allow_nulls (bool, optional): Allow null values/fields if True, otherwise use error_handling_method
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        FieldProcessorStage.__init__(
            self,
            fields=fields,
            allow_nulls=allow_nulls,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )

    def process_field(self, field: Any, field_value: Any) -> Any:
        return str.upper(field_value.strip())
