from __future__ import annotations

import multiprocessing
import time
from typing import TYPE_CHECKING, Sequence

from cluo.core.batch import Batch
from cluo.core.exceptions import BatchError, ErrorHandlingMethod, RecordError
from cluo.core.record import Record
from cluo.utils.log import LoggingMixin

if TYPE_CHECKING:
    from cluo.core import Pipeline


# Type aliases
Channel = str


class Stage(LoggingMixin):
    """Core `Stage` base class. Subclasses should implement the following .process_record or .process_batch methods."""

    __next_id = 1

    @classmethod
    def _next_id(cls) -> int:
        nxt = cls.__next_id
        cls.__next_id += 1
        return nxt

    def __init__(
        self,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        process_empty_batches: bool = False,
        expose_metrics: bool = False,
    ) -> None:
        """
        Initialize the `Stage`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether to expose metrics for this stage. Defaults to False.

        Raises:
            ValueError: Raised if processes > 1 and .process_record method is not implemented.
        """
        self.id = self._next_id()
        self._pipeline: Pipeline | None = None

        if name is None:
            name = self.__class__.__name__
        self.name = name
        self.error_handling_method = error_handling_method
        self.downstreams: dict[str, list[Stage]] = {"default": []}
        if processes > 1 and not hasattr(self, "process_record"):
            raise ValueError(
                "Can only automatically use multiprocessing when .process_record method is implemented."
            )
        else:
            self.processes = processes
        self.process_empty_batches = process_empty_batches
        self.expose_metrics = expose_metrics

    @property
    def unique_name(self) -> str:
        """Unique identifier for this stage. Combines stage name, stage id, pipeline name, and pipeline id."""
        return (
            self.name
            + "_"
            + str(self.id)
            + "_"
            + str(self.pipeline.name)
            + "_"
            + str(self.pipeline.id)
        )

    @property
    def pipeline(self) -> "Pipeline":
        """The pipeline to which this stage belongs."""
        if self._pipeline is None:
            raise ValueError("This stage is not part of a pipeline.")
        return self._pipeline

    def _set_pipeline(self, pipeline: "Pipeline") -> None:
        self._pipeline = pipeline
        pipeline._validated = False

    def __call__(self, batch: Batch) -> Batch:
        """Process a batch of data.

        Args:
            batch (Batch): Batch of data to process.

        Raises:
            NotImplementedError: Raised if neither .process_record and .process_batch methods are not implemented.

        Returns:
            Batch: Processed version of input batch.
        """
        self.log.info(
            f"Processing batch with {self.__class__.__name__} stage: {self.name}"
        )
        self.log.record(batch)  # type: ignore
        __start = time.time()
        if hasattr(self, "process_record"):
            if self.processes == 1:
                batch.records = [
                    self._process_record(record) for record in batch.records
                ]
            else:
                with multiprocessing.Pool(processes=self.processes) as pool:
                    batch.records = pool.map(self.process_record, batch.records)
        elif hasattr(self, "process_batch"):
            batch = self._process_batch(batch)
        else:
            raise NotImplementedError(
                "Stage {} has neither process_record nor process_batch methods.".format(
                    self.name
                )
            )
        __end = time.time()
        # Increment & Time records processed metric
        if self.expose_metrics and self.pipeline.expose_metrics:  # pragma: no cover
            self.pipeline.records_processed.labels(
                pipeline_name=self.pipeline.name, stage_name=self.name
            ).inc(len(batch.records))
            self.pipeline.stage_processing_time.labels(stage_name=self.name).set(
                __end - __start
            )

        elif self.expose_metrics:
            self.log.warning(
                "Pipeline metrics are set to False, but stage metrics are set to True. Stage metrics will not be exposed."
            )

        return batch

    def _process_batch(self, batch: Batch) -> Batch:
        try:
            batch = self.process_batch(batch)  # type: ignore
            for record in batch.records:
                record.add_channel_choices(self.name, self.get_channels(record))
        except Exception as e:
            # Increment records errored metric
            if self.expose_metrics and self.pipeline.expose_metrics:  # pragma: no cover
                self.pipeline.records_errored.labels(
                    pipeline_name=self.pipeline.name, stage_name=self.name
                ).inc(len(batch.records))
            elif self.expose_metrics:
                self.log.warning(
                    "Pipeline metrics are set to False, but stage metrics are set to True. Stage metrics will not be exposed."
                )
            if self.error_handling_method == ErrorHandlingMethod.RAISE:
                raise e
            for record in batch.records:
                batch_error = BatchError(
                    stage_name=self.name,
                    message=f"Stage {self.name} encountered when affecting the entire batch.",
                    handling_method=self.error_handling_method,
                )
                batch_error.__cause__ = e
                record.add_exception(batch_error)

        return batch

    def _process_record(self, record: Record) -> Record:
        try:
            record = self.process_record(record)  # type: ignore
            record.add_channel_choices(self.name, self.get_channels(record))
        except Exception as e:
            # Increment records errored metric
            if self.expose_metrics and self.pipeline.expose_metrics:  # pragma: no cover
                self.pipeline.records_errored.labels(
                    pipeline_name=self.pipeline.name, stage_name=self.name
                ).inc()
            elif self.expose_metrics:
                self.log.warning(
                    "Pipeline metrics are set to False, but stage metrics are set to True. Stage metrics will not be exposed."
                )
            if self.error_handling_method == ErrorHandlingMethod.RAISE:
                raise e
            new_exception = RecordError(
                stage_name=self.name,
                message=f"Stage {self.name} encountered an error when processing this record.",
                handling_method=self.error_handling_method,
            )
            new_exception.__cause__ = e
            record.add_exception(new_exception)
        return record

    def add_downstream(
        self,
        other: Stage | Sequence[Stage] | dict[str, Stage | Sequence[Stage]],
        channel: str = "default",
    ):
        """Adds a stage/s to the downstream channels for this stage.

        In the case of a single stage or a list of stages they are subscribed to the channel listed in the arguement. In the case of a dict, we then use the keys as the channels and recursively call this function.

        Args:
            other (Stage | Sequence[Stage] | dict[str, Stage | Sequence[Stage]]): This is what is being added downstream. In the case of a list or dictionary we recursively call this function.
            channel: the channel that a stage is subscribing to. This is default unles the stage is a branching stage.

        """
        if isinstance(other, Stage):
            if channel not in self.channels:
                if self.channels == ["default"]:
                    raise Exception(
                        "This stage does not support multiple output channels."
                    )
                else:
                    raise Exception(
                        f"This stage as configured only supports the following channels {self.channels}"
                    )
            self.downstreams[channel].append(other)
            if self._pipeline is not None:
                other._set_pipeline(self._pipeline)
        elif isinstance(other, Sequence):
            for downstream in other:
                self.add_downstream(downstream, channel=channel)
        elif isinstance(other, dict):
            for key, value in other.items():
                self.add_downstream(value, channel=key)
        else:
            raise ValueError(f"Invalid downstream(s): {other}")

    def add_upstream(self, other: Stage | Sequence[Stage], channel: str = "default"):
        """Add this stage to the downstreams of other stages.

        Args:
            other (Stage | Sequence[Stage]): the upstream stage/s to add this stage to.
            channel (str): The channel this stage will subscribe to in the upstream stage.
        """
        if isinstance(other, Stage):
            other.add_downstream(self, channel=channel)
        elif isinstance(other, Sequence):
            for upstream in other:
                upstream.add_downstream(self, channel=channel)
        else:
            raise ValueError(f"Invalid upstream(s): {other}")

    def get_downstreams(self, channel: str = "default") -> list[Stage]:
        """Get all downstream stages for a particular channel.

        Args:
            channel(str): the channel we are subscribing to.
        """
        return self.downstreams[channel]

    @property
    def channels(self) -> list[str]:
        """The channels that a stage exposes for downstreams to subscribe to."""
        return list(self.downstreams.keys())

    def get_channels(self, record: Record) -> list[str]:
        """Maps a record to the downstream channels."""
        return ["default"]

    def __str__(self):
        """Implements __str__ for Stage."""

        def construct_name(stage: Stage) -> str:
            return f"{stage.name} ({stage.__class__.__name__} {stage.id})"

        def indented_dfs(
            stage: Stage, indent: int = 0, in_channel: str = "default"
        ) -> str:
            downstreams_strs = []
            for channel, downstreams in stage.downstreams.items():
                for downstream in downstreams:
                    downstreams_strs.append(
                        indented_dfs(downstream, indent + 4, channel)
                    )
            inner = "".join(downstreams_strs)
            return f"{' ' * indent} -> [{in_channel}]{construct_name(stage)}\n{inner}"

        return indented_dfs(self)

    def __lshift__(self, other: Stage | Sequence[Stage]):
        """Implements Stage << Stage"""
        self.add_upstream(other)
        return other

    def __rshift__(
        self, other: Stage | Sequence[Stage] | dict[str, Stage | Sequence[Stage]]
    ):
        """Implements Stage >> Stage"""
        self.add_downstream(other)
        return other

    def __rrshift__(self, other: Stage | Sequence[Stage]):
        """Called for Stage >> [Stage] because list does not have __rshift__ operators."""
        self.__lshift__(other)
        return self

    def __rlshift__(
        self, other: Stage | Sequence[Stage] | dict[str, Stage | Sequence[Stage]]
    ):
        """Called for Stage << [Stage] because list does not have __lshift__ operators."""
        self.__rshift__(other)
        return self
