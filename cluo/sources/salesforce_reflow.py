import urllib.parse
from types import TracebackType
from typing import Any, List, Optional, Type

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2._psycopg import connection
from simple_salesforce import format_soql

from cluo.config import RunMode, config_state
from cluo.connections import PostgresConnectionPool, SalesforceConnectionMixin
from cluo.core import (
    Batch,
    ErrorHandlingMethod,
    OffsetManager,
    PossibleOffsetType,
    Record,
    Source,
)

SALESFORCE_MAX_OFFSET = 2000


class SalesforceReflowSource(Source, SalesforceConnectionMixin):
    """
    Returns a batch of data from a Salesforce instance.
    Records are from a list of ids pulled from a postgres reflow table
    postgres reflow table records are from index [offset, offset + batch_size).
    """

    def __init__(
        self,
        entity: str,
        fields: list[str],
        username: str,
        password: str,
        security_token: str,
        domain: str,
        connection_pool: PostgresConnectionPool,
        offset_manager: OffsetManager,
        reflow_schema: str,
        reflow_table: str,
        reflow_id_field: str,
        reflow_offset_field: str,
        id_field: str = "Id",
        reflow_where_string: str | None = None,
        batch_size: int | None = None,
        name: str | None = None,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        include_deleted: bool = True,
        api_version: str | None = None,
    ) -> None:
        """Initialize the `SalesforceReflowSource`.

        Args:
            entity (str): The salesforce entity to query.
            fields (list[str]): The fields to select.
            username (str): Username.
            password (str): Password.
            security_token (str): Security token.
            domain (str): Domain.
            connection_pool (PostgresConnectionPool): Postgres Connection pool for accessing the reflow table.
            offset_manager (OffsetManager): Offset manager for managing the current offset.
            reflow_schema (str): Schema containing the reflow table.
            reflow_table (str): Reflow table name.
            reflow_id_field (str): The reflow table field that contains the entity id value.
            reflow_offset_field (str): The reflow table field to use as the offset.
            id_field: (str, optional): the salesforce id field.  Defaults to Id.
            reflow_where_string (str, optional): A SQL WHERE clause to use for select on the reflow table. Defaults to no WHERE clause.
            batch_size (int, optional): Number of records to include in each batch from the reflow table. Defaults to 10.
            name (str, optional): Stage name. Defaults to class name if name = None.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            include_deleted (bool): include soft-deleted records in results.  Defaults to True.
            api_version (str, optional): Salesforce API version.
        """
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=1,
            error_handling_method=error_handling_method,
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
        self.entity = entity
        self.fields = fields
        self.connection_pool = connection_pool
        self.reflow_schema = reflow_schema
        self.reflow_table = reflow_table
        self.reflow_id_field = reflow_id_field
        self.reflow_offset_field = reflow_offset_field
        self.id_field = id_field
        self.reflow_where_string = reflow_where_string
        self.current_offset: PossibleOffsetType | None = None
        self.salesforce_query = (
            "SELECT {:literal} FROM {:literal} WHERE {:literal} in ({:literal})"
        )
        self.include_deleted = include_deleted
        self.id_list: List[Optional[Any]] = []
        # calculate maximum batch size based on soql query URL length
        effective_batch_size = self._get_effective_batch_size(self.entity, self.fields)
        self.batch_size: int = (
            batch_size
            if batch_size and int(batch_size) < effective_batch_size
            else effective_batch_size
        )

    def _get_effective_batch_size(self, entity: str, fields: list[str]) -> int:
        # rest api uri limit
        max_query_length = 16_384
        field_list_str = ",".join(fields)
        remaining_chars = max_query_length - len(
            urllib.parse.quote(f"SELECT {field_list_str} FROM {entity} WHERE Id in ()")
        )
        # salesforce 18 character id + 3 characters for delimiters (url encoded) = 27 but 24 proves to be the correct size in practice
        return remaining_chars // 24

    def _construct_reflow_query(self) -> sql.SQL:
        complete_where_clause = sql.SQL("WHERE ")
        if self.reflow_where_string:
            complete_where_clause += (
                sql.SQL("(")
                + sql.SQL(self.reflow_where_string)
                + sql.SQL(")")
                + sql.SQL(" AND ")
            )

        offset_string = sql.SQL("{} > {}").format(
            sql.Identifier(self.reflow_offset_field),
            sql.Literal(self.current_offset),
        )
        complete_where_clause += offset_string

        return sql.SQL("SELECT {} FROM {}.{} {} ORDER BY {} LIMIT {};").format(
            sql.SQL(",").join(
                map(sql.Identifier, [self.reflow_id_field, self.reflow_offset_field])
            ),
            sql.Identifier(self.reflow_schema),
            sql.Identifier(self.reflow_table),
            complete_where_clause,
            sql.Identifier(self.reflow_offset_field),
            sql.Literal(self.batch_size),
        )

    def _execute_reflow_query(self, conn: connection) -> list[dict]:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = self._construct_reflow_query()
        cursor.execute(query)
        return [dict(real_dict_row) for real_dict_row in cursor.fetchall()]

    def _process_query_results(self, response: dict[str, Any]) -> list[dict]:
        records = response.get("records", [])

        if not records:
            return []

        for record in records:
            if "attributes" in record:
                del record["attributes"]
        return records

    def _execute_query(self) -> dict:
        return (
            self.sf.query_all(
                format_soql(
                    self.salesforce_query,
                    ", ".join(self.fields),
                    self.entity,
                    self.id_field,
                    ",".join([f"'{id}'" for id in self.id_list]),
                ),
                include_deleted=self.include_deleted,
            )
            if self.id_list
            else {}
        )

    def process_batch(self, batch: Batch | None) -> Batch:
        conn = self.connection_pool.getconn()
        reflow_records = []
        try:
            # get id list from postgres offset table
            reflow_records = self._execute_reflow_query(conn)
            self.id_list = [
                dictionary.get(self.reflow_id_field)
                for dictionary in reflow_records
                if dictionary.get(self.reflow_id_field)
            ]
            records = Batch(
                records=[
                    Record(data=dictionary)
                    for dictionary in self._process_query_results(self._execute_query())
                ]
            )
        finally:
            self.connection_pool.putconn(conn)
        self.current_offset = (
            max(reflow_records, key=lambda x: x.get(self.reflow_offset_field, 0)).get(
                self.reflow_offset_field, 0
            )
            if reflow_records
            else self.current_offset
        )
        return records

    def _set_offset_key(self) -> None:
        self.source_offset_manager.set_key(self.unique_name)

    def __enter__(self) -> None:
        if config_state.RUN_MODE == RunMode.WRITE:
            self._set_offset_key()
            self.current_offset = self.source_offset_manager.get()
            if self.current_offset is None:
                self.current_offset = self.source_offset_manager.initial_offset
        elif config_state.RUN_MODE == RunMode.PREVIEW:
            self.current_offset = self.source_offset_manager.initial_offset
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
            if self.current_offset:
                # batch is not empty
                self.source_offset_manager.set(self.current_offset)
                self.current_offset = None
            return True
        return False
