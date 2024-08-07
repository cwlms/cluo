from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from .exceptions import ErrorHandlingMethod, StageError

# Type aliases
FieldName = str
RecordData = dict[FieldName, Any]
StageName = str


@dataclass
class Record:
    """Container class for all record data and metadata."""

    data: RecordData = field(default_factory=dict)
    # Metadata
    record_uuid: UUID = field(default_factory=uuid4)
    exceptions: list[StageError] = field(default_factory=list)
    channel_choices: dict[str, list[str]] = field(default_factory=dict)

    def add_channel_choices(self, stage_name: str, channels: list[str]) -> None:
        self.channel_choices[stage_name] = channels

    def add_exception(self, error: StageError):
        """Attach an error to the record"""
        self.exceptions.append(error)

    @property
    def error_count(self) -> int:
        """The number of errors attached to the record."""
        return len(self.exceptions)

    @property
    def most_recent_error(self) -> StageError | None:
        """The most recently raised error attached to the record."""
        return self.exceptions[0] if self.error_count > 0 else None

    @property
    def error_handling_method(self) -> ErrorHandlingMethod | None:
        """The error handling method of the most recent exception."""
        recent_error = self.most_recent_error
        return recent_error.handling_method if recent_error is not None else None

    def as_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "data": self.data,
            "record_uuid": str(self.record_uuid),
            "exceptions": [x.as_dict() for x in self.exceptions],
        }
