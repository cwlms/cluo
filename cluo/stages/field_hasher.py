from cluo.core import ErrorHandlingMethod, Record, Stage
from cluo.utils.hashing import hash_nested_primitive
from cluo.utils.streamsets_hashing import make_hash


class FieldHasherStage(Stage):
    """Attach hash of 1 or more fields to record."""

    def __init__(
        self,
        hash_destination: str,
        hash_fields: list[str] | None = None,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        streamsets_compatible: bool = False,
        expose_metrics: bool = False,
    ) -> None:
        """
        Initialize the `FieldHasherStage`.

        Args:
            hash_destination (str): Record field in which to store the hash. Will overwrite if field already exists.
            hash_fields (list[str], optional): Fields to use in hashing. Defaults to None. Will hash entire record if None.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            streamsets_compatible (bool, optional): generate streamsets compatible hashes.  Use for pipelines that replace existing streamsets pipelines.  Defaults to False.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.hash_fields: list[str] | None = hash_fields
        self.hash_destination: str = hash_destination
        self.streamsets_compatible = streamsets_compatible

    def _make_hash(self, record: Record) -> str:
        if self.streamsets_compatible:
            return make_hash(record, self.hash_fields)
        hash_fields = (
            set(record.data.keys())
            if self.hash_fields is None
            else set(self.hash_fields)
        )
        data = [v for k, v in record.data.items() if k in hash_fields]
        try:
            return hash_nested_primitive(data)
        except (TypeError, OverflowError) as e:
            self._log.error(f"Record is not hashable: {record}")
            raise e

    def process_record(self, record: Record) -> Record:
        hash_value = self._make_hash(record)
        record.data[self.hash_destination] = hash_value
        return record
