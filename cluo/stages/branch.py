from typing import Callable

from cluo.core import ErrorHandlingMethod, Record, Stage


class NoChannelSelectedException(Exception):
    """Exception raised when a record is not assigned to any channels."""

    pass


class BranchStage(Stage):
    """Stage that supports routing records to one of more channels."""

    def __init__(
        self,
        channel_mapping: dict[str, Callable[[Record], bool]],
        single_channel: bool = True,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ):
        """Initialize the `BranchStage`.

        Args:
            channel_mapping (dict[str], Callable[[Record]], bool]): A dictionary of functions where the key is a channel name and the value is a function that returns a boolean representing whether a record is sent to that channel or not.
            single_channel(bool): If this flag is True the a record will only be sent to a single channel instead of all channels whose function returns true. This will be done in order `channel_mapping` since dictionaries are now sortable in Python 3.6+.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        super().__init__(
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.downstreams: dict[str, list[Stage]] = {
            k: list() for k in channel_mapping.keys()
        }
        self.channel_mappings = channel_mapping
        self.single_channel = single_channel

    def process_record(self, record: Record):
        """Implements the identity map on records."""
        return record

    def get_channels(self, record: Record) -> list[str]:
        """Maps the record to it's downstream channels."""
        if self.single_channel:
            return self._get_first_channel(record)
        else:
            return self._get_all_channels(record)

    def _get_first_channel(self, record: Record) -> list[str]:
        """Method for when we only want ta record to go into a single channel."""
        for channel, mapper in self.channel_mappings.items():
            if mapper(record):
                return [channel]
        raise NoChannelSelectedException(
            f"This record was not assigned to a channel: {record}"
        )

    def _get_all_channels(self, record: Record) -> list[str]:
        """Method for when we want to allow a record to go into multiple channels."""
        record_channels = [
            channel
            for channel, mapper in self.channel_mappings.items()
            if mapper(record)
        ]
        if len(record_channels) == 0:
            raise NoChannelSelectedException(
                f"This record was not assigned to a channel: {record}"
            )
        return record_channels
