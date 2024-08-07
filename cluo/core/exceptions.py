import datetime as dt
from dataclasses import dataclass, field
from enum import Enum, auto
from traceback import format_tb
from typing import Any


def _get_traceback_str_from_exception(e: BaseException) -> str:
    """Returns the str representation of a traceback object.

    Args:
        e (BaseException): Anything inheriting from BaseException

    Returns:
        str: A string representation of the errors traceback.

    """
    og_traceback_list = format_tb(e.__traceback__)
    traceback_str = "".join(og_traceback_list)
    return traceback_str


class ErrorHandlingMethod(Enum):
    """All types of Errors that the pipeline class is configured to handle."""

    RAISE = auto()
    SEND_TO_ERROR = auto()
    STOP_PIPELINE = auto()
    DEFAULT = STOP_PIPELINE


@dataclass
class StageError(Exception):
    """Base exception for an error occurring when a stage is called.

    Args:
        stage_name (str): Name of the stage where the error occurred.
        message (str): Error message.
        handling_method (ErrorHandlingMethod): Enum value that tells the pipeline how to handle the error.
        error_dt (datetime.datetime): Time the error occured.

    Attributes:
        __cause__ (Exception): The exception that was originally raised by the stage.



    """

    stage_name: str
    message: str
    handling_method: ErrorHandlingMethod
    error_dt: dt.datetime = field(default_factory=lambda: dt.datetime.utcnow())

    def as_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            dict[str, Any]: Dictionary representing the error.

        """
        out_dict: dict[str, Any] = {
            "stage_name": self.stage_name,
            "message": self.message,
            "handling_method": self.handling_method.name,
            "error_dt": self.error_dt.isoformat(),
            "traceback": _get_traceback_str_from_exception(self),
            "cause": None,
        }
        if self.__cause__ is not None:
            out_dict["cause"] = {
                "error_type": type(self.__cause__).__name__,
                "message": str(self.__cause__),
                "traceback": _get_traceback_str_from_exception(self.__cause__),
            }
        return out_dict


class RecordError(StageError):
    """Exception class used when process_record fails on a stage."""

    pass


class BatchError(StageError):
    """Exception class used when process_batch fails on a stage."""

    pass


class SourceError(StageError):
    """Exception raised when a source fails to pick up a batch."""

    pass


class SinkError(StageError):
    """Exception raised when a sink fails to send a batch."""

    pass


class SocialMediaUrlError(StageError):
    """Exception raised when a invalid Social Media url is passed in"""

    pass
