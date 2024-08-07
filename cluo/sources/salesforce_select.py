import datetime
from datetime import timezone
from types import TracebackType
from typing import Any, Type

from simple_salesforce import format_soql

from cluo.config import RunMode, config_state
from cluo.connections import SalesforceConnectionMixin
from cluo.core import (
    Batch,
    ErrorHandlingMethod,
    OffsetManager,
    PossibleOffsetType,
    Record,
    Source,
)
from cluo.core.offset_manager import OffsetType


class SalesforceSelectSource(Source, SalesforceConnectionMixin):
    """Returns a batch of data from a Salesforce instance. Records return are from index [offset, offset + batch_size)."""

    def __init__(
        self,
        entity: str,
        fields: list[str],
        username: str,
        password: str,
        security_token: str,
        domain: str,
        offset_manager: OffsetManager,
        offset_field: str = "LastModifiedDate",
        offset_nonunique: bool = False,
        batch_size: int = 10,
        name: str | None = None,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        include_deleted: bool = True,
        expose_metrics: bool = False,
        api_version: str | None = None,
    ) -> None:
        """Initialize the `SalesforceSelectSource`.

        Args:
            entity (str): The entity to query.
            fields (list[str]): The fields to select.
            batch_size (int, optional): Number of records to include in each batch. Defaults to 10.
            username (str): Username.
            password (str): Password.
            security_token (str): Security token.
            domain (str): Domain.
            offset_manager (OffsetManager): Offset manager for managing the current offset.
            offset_field (str): Datetime field from Salesforce that is used for offset. Defaults to LastModifiedDate.
            offset_nonunique (bool): Your offset field is non-unique.  Defaults to False.
            name (str, optional): Stage name. Defaults to class name if name = None.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            include_deleted (bool, optional): include soft-deleted records in results.  Defaults to True.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
            api_version (str, optional): Salesforce API version.
        """

        self.query = "SELECT {:literal} FROM {:literal} WHERE {:literal} > {:literal} ORDER BY {:literal} LIMIT {} OFFSET {}"
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=1,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        SalesforceConnectionMixin.__init__(
            self,
            username=username,
            password=password,
            security_token=security_token,
            domain=domain,
            version=api_version,
        )
        self.source_offset_manager = offset_manager
        if self.batch_size <= 1:
            raise ValueError("Salesforce batch_size must be greater than 1")
        if offset_field not in fields:
            raise ValueError("Offset field must be in fields list")
        if "Id" not in fields:
            raise ValueError("Id field must be in fields list")
        self.entity = entity
        self.fields = fields
        self.current_offset = offset_manager.initial_offset
        self.offset_field = offset_field
        self.offset_nonunique = offset_nonunique
        self.max_id = None
        self.include_deleted = include_deleted
        self._last_processed_offset: PossibleOffsetType | None = None

    def _prepare_soql(self) -> str:
        if self.max_id:
            # If offset is non-unique, we need to use the Id field to ensure deterministic ordering/batches
            if self.source_offset_manager.offset_type == OffsetType.STR:
                self.query = "SELECT {:literal} FROM {:literal} WHERE ({:literal} > {}) OR ({:literal} = {} AND Id > {}) ORDER BY {:literal}, Id LIMIT {}"
            else:
                self.query = "SELECT {:literal} FROM {:literal} WHERE ({:literal} > {:literal}) OR ({:literal} = {:literal} AND Id > {}) ORDER BY {:literal}, Id LIMIT {}"

            query = format_soql(
                self.query,
                ", ".join(field for field in self.fields),
                self.entity,
                self.offset_field,
                self._format_offset(self.current_offset),
                self.offset_field,
                self._format_offset(self.current_offset),
                self.max_id,
                self.offset_field,
                self.batch_size,
            )
            return query
        else:
            if self.source_offset_manager.offset_type == OffsetType.STR:
                self.query = "SELECT {:literal} FROM {:literal} WHERE {:literal} > {} ORDER BY {:literal}, Id LIMIT {}"
            else:
                self.query = "SELECT {:literal} FROM {:literal} WHERE {:literal} > {:literal} ORDER BY {:literal}, Id LIMIT {}"

            query = format_soql(
                self.query,
                ", ".join(field for field in self.fields),
                self.entity,
                self.offset_field,
                self._format_offset(self.current_offset),
                self.offset_field,
                self.batch_size,
            )
            return query

    def _process_query_results(self, response: dict[str, Any]) -> list[dict]:
        records = [dict(record) for record in response["records"]]
        if records:
            for record in records:
                if "attributes" in record:
                    del record["attributes"]
                # Convert offset to datetime if offset is datetime
                if self.source_offset_manager.offset_type == OffsetType.DATETIME:
                    record[self.offset_field] = datetime.datetime.fromisoformat(
                        record[self.offset_field]
                    )
        return records

    def _execute_query(self) -> dict:
        query = self._prepare_soql()
        return self.sf.query_all(
            query,
            include_deleted=self.include_deleted,
        )

    def process_batch(self, batch: Batch | None) -> Batch:
        batch = Batch(
            records=[
                Record(data=dictionary)
                for dictionary in self._process_query_results(self._execute_query())
            ]
        )

        if self.offset_nonunique:
            # If we have a full batch and offset is non-unique, store the last Id from the previous batch.
            if len(batch.records) == self.batch_size:
                self.max_id = batch.records[-1].data.get("Id")
            else:
                # If we have a partial or empty batch, we are done paging.  Use current max.
                self.max_id = None

        self.current_offset = batch.max_offset(self.offset_field)

        return batch

    def _set_offset_key(self) -> None:
        self.source_offset_manager.set_key(self.unique_name)

    def __enter__(self) -> None:
        if config_state.RUN_MODE == RunMode.WRITE:
            self._set_offset_key()
            self.current_offset = self.source_offset_manager.get()
            if self.current_offset is None:
                self.current_offset = self.source_offset_manager.initial_offset
        elif config_state.RUN_MODE == RunMode.PREVIEW:
            self.current_offset = self.source_offset_manager.get()
        else:
            raise Exception("Unknown run mode.")

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        has_exception = (
            exc_type is not None or exc_value is not None or exc_tb is not None
        )
        if not has_exception and config_state.RUN_MODE == RunMode.WRITE:
            self._set_offset_key()
            if self.current_offset:
                self.source_offset_manager.set(self.current_offset)
            return True
        return False

    def _format_offset(self, offset: PossibleOffsetType | None) -> str:
        if isinstance(offset, datetime.datetime):
            return (
                offset.replace(microsecond=0)
                .replace(tzinfo=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        return str(offset)
