from cluo._config_state import config_state  # type: ignore
from cluo.run_mode import RunMode
from cluo.utils.log import LoggingMixin


def set_logging_level(level: int | str) -> None:
    """Sets logging level for all of cluo."""

    config_state.LOG_LEVEL = level
    LoggingMixin.reset_level(level)  # type: ignore


def set_run_mode(mode: RunMode) -> None:
    """Sets run mode for all of cluo."""
    config_state.RUN_MODE = mode


__all__ = ["config_state", "RunMode", "set_logging_level", "set_run_mode"]
