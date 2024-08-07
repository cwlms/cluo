from cluo.core import Batch, Stage


class DedupeBatchStage(Stage):
    """Removes duplicate records from batch based on specified key."""

    def __init__(
        self,
        unique_key: list[str],
        first_record: bool = False,
        name: str | None = None,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `DedupeBatchStage`.

        Args:
            unique_key (list[str]): List of keys to use for deduplication.
            first_record (bool, optional): Pick first record in order if True, else pick last record in order
            name (str, optional): Stage name. Defaults to class name if name = None.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            # always one process: only used if .process_record method is implemented
            processes=1,
            expose_metrics=expose_metrics,
        )
        self.unique_key = unique_key
        self.first_record = first_record

    def process_batch(self, batch: Batch) -> Batch:
        return batch.deduplicate(
            unique_key=self.unique_key, first_record=self.first_record
        )
