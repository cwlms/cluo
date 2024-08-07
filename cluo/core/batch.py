from __future__ import annotations

from dataclasses import dataclass, field
from pprint import pformat
from typing import Any

from cluo.core.offset_manager import PossibleOffsetType
from cluo.core.record import Record


@dataclass
class Batch:
    """Batch of data. Contains records.

    Attributes:
        records (list[Record]): Records in the batch.
    """

    records: list[Record] = field(default_factory=list)

    def __repr__(self) -> str:
        start = "Batch("
        formatted_records = "\n\t".join([pformat(record) for record in self.records])
        return "".join([start, "\n\t", formatted_records, "\n)"])

    def __len__(self) -> int:
        return len(self.records)

    def has_same_record_data(self, other: Batch) -> bool:
        """Checks to see if the batches contain records with the same data.

        Note that this does depend on the order of records in the Batch.

        Args:
            other (Batch): Batch we are comparing against.

        Returns:
            bool: True if they have the same record data.

        """
        if len(self) != len(other):
            return False
        for record_1, record_2 in zip(self.records, other.records):
            if record_1.data != record_2.data:
                return False
        return True

    def data_as_list(self) -> list[dict[str, Any]]:
        """Creates list of record data

        Returns:
            list[dict[str, Any]]: Data from records in batch

        """
        return [record.data for record in self.records]

    def deduplicate(self, unique_key: list[str], first_record: bool = False) -> Batch:
        """Returns batch deduplicated by key.  When duplicates
        found pick last record in order.  When first_record = True,
        pick first record in order. Batch is unchanged if no key
        defined or batch is empty / None.

        Args:
            unique_key (list[str]): List of fields to use as unique key
            first_record (bool, optional): Pick first record in order if True, else pick last record in order

        Returns:
            batch: deduplicated batch
        """

        def _record_key(record: Record, unique_key: list[str]) -> tuple:
            return tuple(record.data.get(key) for key in unique_key)

        def _record_order(records: list[Record], _rev: bool) -> list[Record]:
            return records if _rev else list(reversed(records))

        if not unique_key:
            return self
        included_records = []
        seen = set()
        for record in _record_order(self.records, first_record):
            record_key = _record_key(record, unique_key)
            if record_key in seen:
                continue
            seen.add(record_key)
            included_records.append(record)
        # reassign the records from the existing batch reference
        # ensures any existing batch attributes are preserved
        # _record_order call required to ensure batch preserves order (see tests)
        self.records = _record_order(included_records, first_record)
        return self

    def max_offset(self, offset_field) -> PossibleOffsetType | None:
        """Returns max offset value in the batch.

        Args:
            offset_field (str): The field being used as the offset.

        Returns:
            PossibleOffsetType: Max offset value in batch

        """
        if self.records:
            return max(self.records, key=lambda x: x.data[offset_field]).data[
                offset_field
            ]
        else:
            return None
