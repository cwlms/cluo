import functools
import time
from copy import deepcopy
from typing import Any, Callable

import networkx as nx
from networkx import DiGraph
from prometheus_client import Counter, Gauge, start_http_server

from cluo.core.batch import Batch
from cluo.core.error_handler import BaseErrorHandler
from cluo.core.exceptions import ErrorHandlingMethod
from cluo.core.sink import Sink
from cluo.core.source import Source
from cluo.core.stage import Stage
from cluo.utils.log import LoggingMixin


class Pipeline(LoggingMixin):
    """Core `Pipeline` class. Orchestrates stages within a single data pipeline."""

    __next_id = 1

    @classmethod
    def _next_id(cls) -> int:
        nxt = cls.__next_id
        cls.__next_id += 1
        return nxt

    def __init__(
        self, name: str | None = None, interval: float = 0, expose_metrics: bool = False
    ) -> None:
        """
        Args:
            name (str, optional): Pipeline name. Defaults to "Pipeline".
            interval (float, optional): Pipeline execution interval. Defaults to 0.
            expose_metrics (bool, optional): Whether to expose metrics for the pipeline. Defaults to False.
        """
        if name is None:
            name = self.__class__.__name__
        self.name = name
        self.interval = interval
        self.id = self._next_id()
        self.sources: list[Source] = []
        self.error_handlers: list[BaseErrorHandler] = []
        self._validated: bool = False
        self.expose_metrics = expose_metrics
        if self.expose_metrics:
            self._expose_metrics()

    def _expose_metrics(self) -> None:  # pragma: no cover
        """Expose metrics for the pipeline."""

        # Start Prometheus client
        start_http_server(9090)  # In the future, this should be configurable

        # Instantiate default metrics for runs, batches, and records
        self.runs_processed = Counter(
            "cluo_runs_processed",
            "Number of runs processed by the pipeline",
            ["pipeline_name"],
        )

        self.records_processed = Counter(
            "cluo_records_processed",
            "Number of records processed by the pipeline",
            ["pipeline_name", "stage_name"],
        )

        self.records_errored = Counter(
            "cluo_records_errored",
            "Number of records errored by the pipeline",
            ["pipeline_name", "stage_name"],
        )

        self.pipeline_processing_time = Gauge(
            "cluo_pipeline_processing_time",
            "Seconds taken to process pipeline",
            ["pipeline_name"],
        )

        self.stage_processing_time = Gauge(
            "cluo_stage_processing_time",
            "Seconds taken to process a stage",
            ["stage_name"],
        )

    def add_source(self, source: Source) -> None:
        """Add a source to pipeline. Also sets .pipeline reference to this pipeline on the source and all downstream stages.

        Args:
            source (Stage): Source to add.
        """
        self._set_all_downstream_stage_pipeline_refs(source)
        self.sources.append(source)

    def add_error_handler(self, handler: BaseErrorHandler) -> None:
        """Add an error handler to the pipeline.

        Args:
            handler (BaseErrorHandler): a handler that will process all messages 'SENT_TO_ERROR'.

        """
        self.error_handlers.append(handler)

    @staticmethod
    def _validate(func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator to make it easy to add validation before running a function."""

        @functools.wraps(func)
        def inner_func(self, *args, **kwargs):
            self.validate_pipeline()
            return func(self, *args, **kwargs)

        return inner_func

    def validate_pipeline(self) -> None:
        """Validates a pipeline and raises an error if it does not pass.

        Currently we check for the following things when validating:
        * cycles in the graph.
        * channels which have no downstreams
        * stages which are not sinks and have no downstreams

        """
        if self._validated is True:
            self.log.debug(
                f"Pipeline ({self.name}) is in a validated state. Skipping validation."
            )
            return

        self.log.debug(f"Validating Pipeline {self.name}")

        self._validate_no_cycles()
        self._validate_no_unmonitored_channels()
        self._validate_positive_interval()

        # once all check have run we set validated to true.
        self._validated = True

    def _validate_positive_interval(self) -> None:
        if self.interval < 0:
            raise ValueError("interval must be greater than or equal to 0")

    def _validate_no_cycles(self) -> None:
        """Raise an error if there is a cycle in the pipeline graph."""
        graph = self.generate_pipeline_graph()
        if not nx.is_directed_acyclic_graph(graph):
            raise PipelineValidationError("A cycle was detected in this pipeline.")

    def _validate_no_unmonitored_channels(self) -> None:
        """Raise and error if there is a channel with no downstreams on a non-sink."""
        stage_queue: list[Stage] = list(self.sources)
        while stage_queue:
            cur_stage = stage_queue.pop(0)
            channels = cur_stage.channels
            for channel in channels:
                channel_downstreams = cur_stage.get_downstreams(channel=channel)
                unmonitored_flag = len(channel_downstreams) == 0 and not isinstance(
                    cur_stage, Sink
                )

                if unmonitored_flag:
                    raise PipelineValidationError(
                        f"Stage {cur_stage} is not a sink and {channel} does not have any downstream stages."
                    )

                stage_queue.extend(iter(channel_downstreams))

    def generate_pipeline_graph(self) -> DiGraph:
        graph = DiGraph()
        stage_queue: list[Stage] = list(self.sources)
        added_stages = set()
        while stage_queue:
            current_stage = stage_queue.pop(0)
            if current_stage.unique_name not in graph.nodes:
                graph.add_node(current_stage.unique_name)
                added_stages.add(current_stage)
            for channel in current_stage.channels:
                channel_stages = current_stage.get_downstreams(channel=channel)
                for channel_stage in channel_stages:
                    if channel_stage not in added_stages:
                        stage_queue.append(channel_stage)
                        graph.add_node(channel_stage.unique_name)
                        added_stages.add(channel_stage)
                    graph.add_edge(current_stage.unique_name, channel_stage.unique_name)
        return graph

    @_validate
    def run(self) -> None:
        """Run the pipeline. Enters `while True` loop and runs all stages in pipeline."""
        while True:
            __start = time.time()
            for source in self.sources:
                with source:
                    self._single_run(source, Batch([{}]))  # type: ignore
            __end = time.time()
            __wait = self.interval - (__end - __start)
            if __wait > 0:
                time.sleep(__wait)
            if self.expose_metrics:
                self.runs_processed.labels(pipeline_name=self.name).inc()
                self.pipeline_processing_time.labels(pipeline_name=self.name).set(
                    __end - __start
                )

    @_validate
    def _single_run(self, stage: Stage, batch: Batch) -> None:
        if (
            len(batch) == 0
            and not isinstance(stage, Source)
            and not stage.process_empty_batches
        ):
            return
        batch = stage(batch)

        valid_batch, errored_batch = self._filter_errored_records(batch=batch)

        self._process_errored_records(batch=errored_batch)

        self._process_valid_records(stage=stage, batch=valid_batch)

    @staticmethod
    def _filter_errored_records(batch: Batch) -> tuple[Batch, Batch]:
        """Filter out errored records from valid records.

        Args:
            batch (Batch): Batch to be filtered.

        Returns:
            valid_batch (Batch): Batch with the good records.
            errored_batch (Batch): Batch with the errored records.
        """
        errored_batch = Batch()
        valid_batch = Batch()
        for record in batch.records:
            if record.error_count == 0:
                valid_batch.records.append(record)
            else:
                errored_batch.records.append(record)
        return valid_batch, errored_batch

    def _process_errored_records(self, batch: Batch) -> None:
        """Either send errored records to handlers or stop pipeline."""
        for record in batch.records:
            if record.error_handling_method != ErrorHandlingMethod.SEND_TO_ERROR:
                self.log.error(f"Stop pipeline error on record {record}")
                raise record.exceptions[0]
            for handler in self.error_handlers:
                handler.process_error_record(record)

    def _process_valid_records(self, stage: Stage, batch: Batch) -> None:
        """Send non-errored records to the appropriate downstream stage."""

        # sort into the appropriate channel
        channel_batch_dict = {x: Batch() for x in stage.channels}
        for record in batch.records:
            # record_channels: list[str] = stage.get_channels(record)
            record_channels: list[str] = record.channel_choices[stage.name]
            num_channels = len(record_channels)
            for channel in record_channels:
                record = record if num_channels == 1 else deepcopy(record)
                channel_batch_dict[channel].records.append(record)

        # Send to downstream stages
        for channel, batch in channel_batch_dict.items():
            batch_downstreams = stage.get_downstreams(channel)
            num_downstreams = len(batch_downstreams)
            for downstream_stage in batch_downstreams:
                if num_downstreams == 1:
                    self._single_run(stage=downstream_stage, batch=batch)
                else:
                    self._single_run(stage=downstream_stage, batch=deepcopy(batch))

    @_validate
    def __str__(self) -> str:
        sources = "\n".join([str(source) for source in self.sources])
        return f"{self.__class__.__name__} ({self.name}) \n {sources}"

    def _set_all_downstream_stage_pipeline_refs(self, source: Source) -> None:
        stack: list[Stage] = [source]
        added_stages = set()  # added to avoid an infinite loop when a cycle exists.
        while stack:
            cur_stage = stack.pop()
            cur_stage._set_pipeline(self)
            added_stages.add(cur_stage)
            for downstream_list in cur_stage.downstreams.values():
                stack.extend([x for x in downstream_list if x not in added_stages])


class PipelineValidationError(BaseException):
    pass
