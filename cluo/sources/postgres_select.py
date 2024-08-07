from types import TracebackType
from typing import Type

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2._psycopg import connection

from cluo.config import RunMode, config_state
from cluo.connections import PostgresConnectionPool
from cluo.core import (
    Batch,
    ErrorHandlingMethod,
    OffsetManager,
    PossibleOffsetType,
    Record,
    Source,
)


class PostgresSelectSource(Source):
    """Returns a batch of data from a Postgres database. Records return are from index [offset, offset + batch_size)."""

    def __init__(
        self,
        schema: str,
        table: str,
        fields: list[str],
        offset_field: str,
        offset_manager: OffsetManager,
        connection_pool: PostgresConnectionPool,
        where_string: str | None = None,
        batch_size: int | None = None,
        name: str | None = None,
        processes: int = 1,
        use_timestamp_offset: bool = False,
        order_by: list[str] | None = None,
        offset_nonunique: bool = False,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `PostgresSelectSource`.

        Args:
            schema (str): Schema containing the table.
            table (str): The table to which to append.
            fields (list[str]): The fields to select.
            offset_field (str): The field to use as the offset.
            offset_manager (OffsetManager): Offset manager for managing the current offset.
            connection_pool (PostgresConnectionPool): Connection pool to use connections from for lookup.
            where_string (str, optional): A SQL WHERE clause to use for select. Defaults to no WHERE clause.
            batch_size (int, optional): Number of records to include in each batch. Defaults to 10.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            order_by (list[str], optional): Order by fields.  Defaults to [offset_field].
            offset_nonunique (bool): Your offset field is non-unique.  Defaults to False.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this source. Defaults to False.
        """
        Source.__init__(
            self,
            batch_size=batch_size,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.source_offset_manager = offset_manager
        self.connection_pool = connection_pool
        self.schema = schema
        self.table = table
        self.fields = fields
        self.offset_field = offset_field
        self.where_string = where_string
        self.current_offset: PossibleOffsetType | None = None
        self.order_by: list[str] = order_by or [offset_field]
        self.offset_nonunique = offset_nonunique
        self.page = 0
        if offset_field not in self.order_by:
            raise ValueError("offset field must be in order_by list")
        if offset_field not in self.fields:
            raise ValueError("offset field must be in fields list")

    def _construct_query(self) -> sql.SQL:
        complete_where_clause = sql.SQL("WHERE ")
        if self.where_string:
            complete_where_clause += (
                sql.SQL("(")
                + sql.SQL(self.where_string)
                + sql.SQL(")")
                + sql.SQL(" AND ")
            )

        offset_string = sql.SQL("{} > {}").format(
            sql.Identifier(self.offset_field),
            sql.Literal(self.current_offset),
        )
        complete_where_clause += offset_string

        return sql.SQL(
            "SELECT {} FROM {}.{} {} ORDER BY {} LIMIT {} OFFSET {};"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, self.fields)),
            sql.Identifier(self.schema),
            sql.Identifier(self.table),
            complete_where_clause,
            sql.SQL(",").join(map(sql.Identifier, self.order_by)),
            sql.Literal(self.batch_size),
            sql.Literal(self.page * self.batch_size),
        )

    def _execute_query(self, conn: connection) -> list[dict]:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = self._construct_query()
        cursor.execute(query)
        return [dict(real_dict_row) for real_dict_row in cursor.fetchall()]

    def process_batch(self, batch: Batch | None) -> Batch:
        conn = self.connection_pool.getconn()
        try:
            batch = Batch(
                [Record(data=dictionary) for dictionary in self._execute_query(conn)]
            )
        finally:
            self.connection_pool.putconn(conn)
        # If we have a full batch and offset is non-unique, increment the page
        if self.offset_nonunique and len(batch.records) == self.batch_size:
            self.page += 1
        else:
            self.current_offset = batch.max_offset(self.offset_field)
            self.page = 0
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
            if self.current_offset:
                # batch is not empty
                self.source_offset_manager.set(self.current_offset)
                self.current_offset = None
            return True
        return False
