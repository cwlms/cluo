import datetime as dt
from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property
from typing import Union

from cluo.utils.log.logging_mixin import LoggingMixin

# type aliases
PossibleOffsetType = Union[str, int, dt.datetime]


class OffsetType(Enum):
    STR = "string"
    INT = "int"
    DATETIME = "datetime"


class OffsetManager(ABC, LoggingMixin):
    def __init__(self, offset_type: OffsetType | None):
        """Initialize the `OffsetManager`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            offset_type (OffsetType, optional): Enum that represents the type of offset. Defaults to "int".
        """
        self.offset_type = offset_type or OffsetType.INT
        self._key: str | None = None

    @cached_property
    def initial_offset(self) -> PossibleOffsetType | None:
        if self.offset_type == OffsetType.STR:
            return ""
        elif self.offset_type == OffsetType.INT:
            return 0
        elif self.offset_type == OffsetType.DATETIME:
            return dt.datetime(1970, 1, 1)

    def get(self) -> PossibleOffsetType | None:
        value_str = self._get()
        if value_str:
            return self._offset_from_str(value_str)
        return None

    @abstractmethod
    def _get(self) -> str | None:
        pass

    def set(self, value: PossibleOffsetType) -> bool:
        return self._set(self._offset_to_str(value))

    @abstractmethod
    def _set(self, value: str) -> bool:
        pass

    def set_key(self, key: str) -> None:
        """Set the key to use for the offset.

        Args:
            key (str): The key.
        """
        self._key = key
        self._page = f"{key}_page"

    def _offset_to_str(self, value: PossibleOffsetType) -> str:
        if self.offset_type == OffsetType.INT and isinstance(value, int):
            return str(value)
        elif self.offset_type == OffsetType.STR and isinstance(value, str):
            return value
        elif self.offset_type == OffsetType.DATETIME and isinstance(value, dt.datetime):
            return value.isoformat()
        raise ValueError(
            f"{type(value)}: {value} is not supported by this manager {self.offset_type}."
        )

    def _offset_from_str(self, value: str) -> PossibleOffsetType:
        if self.offset_type == OffsetType.STR:
            return value
        elif self.offset_type == OffsetType.INT:
            return int(value)
        elif self.offset_type == OffsetType.DATETIME:
            return dt.datetime.fromisoformat(value)
        raise ValueError(f"{type(value)} is not a supported type.")

    def increment(self, amount: int) -> bool:
        """Increment the current offset value by `amount`.

        Args:
            amount (int): Amount by which to increment.

        Returns:
            bool: Whether set was successful.
        """
        if self.offset_type != OffsetType.INT:
            raise ValueError("increment is only available for int type offsets.")
        offset = self.get()
        return self.set(offset + amount)  # type: ignore

    @property
    def key(self) -> str:
        if self._key is None:
            raise ValueError("Key not set. Call `set_key()` first.")
        return self._key
