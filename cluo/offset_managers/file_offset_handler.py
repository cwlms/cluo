from pathlib import Path

from cluo.core.offset_manager import OffsetManager, OffsetType


class FileOffsetManager(OffsetManager):
    def __init__(
        self,
        filepath: str | Path,
        offset_type: OffsetType | None,
    ):
        """Initialize the `FileOffsetManager`.

        Args:
            filepath (str | Path): The path to the file where the offset will be stored.
            offset_type (PossibleOffsetType): Enum that represents the type of offset value. Defaults to "int".
        """
        super().__init__(offset_type=offset_type)
        self.filepath = filepath if isinstance(filepath, Path) else Path(filepath)

    def _get(self) -> str | None:
        with open(self.filepath, "r") as f:
            return f.read()

    def _set(self, value: str) -> bool:
        with open(self.filepath, "w") as f:
            f.write(value)
        return True
