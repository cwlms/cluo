import abc
from enum import Enum

from cluo.core.record import Record


class ErrorHandlerFailureAction(Enum):
    """Action to perform when error handler experiences an error."""

    RAISE = "RAISE"
    LOG = "LOG"
    IGNORE = "IGNORE"


class BaseErrorHandler(abc.ABC):
    """Base class for all error handlers to inherit from."""

    def __init__(
        self,
        failure_action: ErrorHandlerFailureAction = ErrorHandlerFailureAction.RAISE,
    ) -> None:
        super().__init__()
        # in case we get None
        self.failure_action = failure_action or ErrorHandlerFailureAction.RAISE

    @abc.abstractmethod
    def process_error_record(self, record: Record) -> None:
        """Process the errored record.

        Args:
            record (Record): The errored record to be processed.

        """
        pass
