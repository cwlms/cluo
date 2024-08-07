import logging
from typing import Any

from cluo.utils.log.logging_mixin import LoggingMixin

__all__ = ["LoggingMixin"]

# https://stackoverflow.com/questions/1977362/how-to-create-module-wide-variables-in-python
BATCH_LEVEL_NUMBER = 9
logging.addLevelName(BATCH_LEVEL_NUMBER, "BATCH")
logging.BATCH = BATCH_LEVEL_NUMBER  # type: ignore


def _batch_logger(self, message: Any, *args, **kws) -> None:
    if self.isEnabledFor(BATCH_LEVEL_NUMBER):
        # Yes, logger takes its '*args' as 'args'.
        self._log(BATCH_LEVEL_NUMBER, message, args, **kws)


logging.Logger.record = _batch_logger  # type: ignore
