import abc
from types import TracebackType
from typing import Type

from cluo.core.exceptions import ErrorHandlingMethod
from cluo.core.stage import Stage


class Source(Stage, metaclass=abc.ABCMeta):
    """Core `Source` base class. For outputting data to final destination."""

    def __init__(
        self,
        batch_size: int | None = 10,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """
        Initialize the `Source`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            batch_size (int, optional): Max number of records to obtain within a single batch. Fewer will be obtained if there are not enough records available to meet batch_size. Defaults to 10.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        if batch_size is None:
            batch_size = 10
        elif batch_size < 1:
            raise ValueError("batch_size must be greater than 0")
        self.batch_size = batch_size

    @abc.abstractmethod
    def __enter__(self) -> None:
        """Anything that needs to be done when entering a source context (aka when the 'with' scope starts)"""
        pass

    @abc.abstractmethod
    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Anything that needs to be done when leaving a source context (aka when the 'with' scope ends)"""
        pass
