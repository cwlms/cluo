import datetime
import json
import logging
import sys

from rich.logging import RichHandler

from cluo._config_state import config_state  # type: ignore


class LoggingMixin:
    """Provides logging functionality to Stages."""

    _log = logging.getLogger(__name__)
    err_handler = logging.StreamHandler(sys.stderr)
    err_handler.setLevel(logging.ERROR)
    logging.basicConfig(
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(), err_handler],
    )
    _log.setLevel(config_state.LOG_LEVEL)

    @property
    def log(self) -> logging.Logger:
        """Main logging property. Call log.info (etc.) to log."""
        return self._log

    @classmethod
    def reset_level(cls, level: int) -> None:
        """Reset the logging level."""
        cls._log.setLevel(level)

    @classmethod
    def serialize(cls, source_dict: dict) -> str:
        def default(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            else:
                return str(o)

        return json.dumps(source_dict, default=default)
