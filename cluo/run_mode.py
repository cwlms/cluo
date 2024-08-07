from enum import Enum


class RunMode(Enum):
    """Modes in which pipelines can be run. Analogous to StreamSets preview behavior."""

    WRITE = "WRITE"
    PREVIEW = "PREVIEW"
