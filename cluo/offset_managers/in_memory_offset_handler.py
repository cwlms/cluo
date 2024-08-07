from cluo.core.offset_manager import OffsetManager, OffsetType


class InMemoryOffsetManager(OffsetManager):
    def __init__(self, offset_type: OffsetType):
        """Initialize the `InMemoryOffsetManager`.

        Args:
            offset_type (PossibleOffsetType): Enum that represents the type of offset value. Defaults to "int".
        """
        self.offset: str | None = None
        super().__init__(offset_type=offset_type)

    def _get(self) -> str | None:
        return self.offset

    def _set(self, value: str | None) -> bool:
        self.offset = value
        return True
