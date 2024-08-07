from cluo.core import ErrorHandlingMethod, Record, Stage
from cluo.utils.hashing import hash_nested_primitive
from cluo.utils.streamsets_hashing import make_hashes as make_streamsets_hashes


class ArrayFieldHasherStage(Stage):
    """Attach array of hashes of 1 or more fields to record."""

    def __init__(
        self,
        hash_fields: list[list[str]],
        hash_destination: str = "array_hash",
        hash_name_destination: str | None = "array_hash_name",
        name: str | None = None,
        hash_nulls: bool = False,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        streamsets_compatible: bool = False,
        expose_metrics: bool = False,
    ) -> None:
        """
        Initialize the `ArrayFieldHasherStage`.

        Args:
            hash_fields (list[list[str]]): Fields from which to create an array of hashes. Hashes in output hash array correspond one-to-one with inner lists of fields.
            hash_destination (str, optional): Record field in which to store the hashes. Will overwrite if field already exists. Defaults to "array_hash".
            hash_name_destination (str | None, optional): Record field in which to store the array hash names. Defaults to "array_hash_name".
            hash_nulls (bool, optional): Create hash when fields contain null values. Defaults to False.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.hash_fields = hash_fields
        self.hash_destination = hash_destination
        self.hash_name_destination = hash_name_destination
        self.streamsets_compatible = streamsets_compatible
        self.hash_nulls = hash_nulls

    def _make_hash_names(self, record: Record) -> list[str]:
        hash_names = []
        for single_hash_fields in self.hash_fields:
            inputs_ = [record.data[field] for field in single_hash_fields]
            if self.hash_nulls or None not in inputs_:
                hash_names.append("_".join(single_hash_fields) + "_hash")
        return hash_names

    def _make_hashes(self, record: Record) -> list[str | None]:
        hashes: list[str | None] = []
        # generate streamsets hashes with nulls
        # we will only pull out the ones we want
        streamsets_hashes: list[str | None] = (
            make_streamsets_hashes(record, self.hash_fields, allow_nulls=True)
            if self.streamsets_compatible
            else []
        )
        for hash_index, single_hash_fields in enumerate(self.hash_fields):
            inputs_ = [record.data[field] for field in single_hash_fields]
            if self.hash_nulls or None not in inputs_:
                if self.streamsets_compatible:
                    hashes.append(streamsets_hashes[hash_index])
                else:
                    hashes.append(hash_nested_primitive(inputs_))
        return hashes

    def process_record(self, record: Record) -> Record:
        hashes = self._make_hashes(record)
        record.data[self.hash_destination] = hashes
        if self.hash_name_destination is not None:
            record.data[self.hash_name_destination] = self._make_hash_names(record)
        return record
