from cluo.core import Record
from cluo.core.error_handler import BaseErrorHandler, ErrorHandlerFailureAction
from cluo.utils.log import LoggingMixin


class LoggingHandler(BaseErrorHandler, LoggingMixin):
    """This will log any records passed to it to the logging.ERROR"""

    def process_error_record(self, record: Record) -> None:
        """Send record to logging.ERROR.

        Args:
            record (Record): Record to be logged.

        """
        try:
            self.log.error(self.serialize(record.as_dict()))
        except Exception as ex:
            if self.failure_action != ErrorHandlerFailureAction.IGNORE:
                raise ex
