from typing import Any, Literal

import psycopg2
import psycopg2.extras
from psycopg2._psycopg import connection

from cluo.connections.postgres_connection_pool import PostgresConnectionPool
from cluo.core import ErrorHandlingMethod, Record, RecordData, Stage


class PostgresLookupStage(Stage):
    """Appends fields to each record in a batch based on a lookup query."""

    def __init__(
        self,
        query: str,
        mappings: dict[str, str],
        connection_pool: PostgresConnectionPool,
        on_zero_rows: Literal["raise", "ignore", "preserve"] = "ignore",
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `PostgresLookupStage`.

        Args:
            query (str): SQL query to run. Applied for each batch. Must return exactly one row. Uses %s for placeholders, with placeholder names corresponding to record dictionary keys. Example: `"SELECT * FROM master.companies WHERE city = %(city)s LIMIT 1;"`
            mappings (dict[str, str]): Map of how fields keys (columns) returned by query should be mapped to a field in each record on the batch. Example: `{'id': 'data_source_id', 'name': 'company_name'}`.
            connection_pool (PostgresConnectionPool): Connection pool to use connections from for lookup.
            on_zero_rows (Literal["ignore", "raise", "preserve"], optional): What to do if zero rows are returned by lookup. Defaults to "ignore", which will set the lookup field to None. "raise" will raise an exception.  "preserve" preserves original record data.
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

        self.query = query
        self.mappings = mappings
        self.on_zero_rows = on_zero_rows
        self.connection_pool = connection_pool

    def _validate_lookup_results(
        self, results: list[psycopg2.extras.RealDictRow]
    ) -> None:
        if len(results) > 1 or self.on_zero_rows == "raise" and not results:
            raise Exception(
                f"Lookup query must return exactly one row. Got {len(results)} rows."
            )

    def _process_lookup_results(
        self,
        results: list[psycopg2.extras.RealDictRow],
        colnames: list[str],
        record_data: RecordData,
    ) -> dict[str, Any]:
        self._validate_lookup_results(results)
        results = [dict(result) for result in results]
        if not results and self.on_zero_rows == "ignore":
            return {col: None for col in colnames}
        elif not results and self.on_zero_rows == "preserve":
            return {value: record_data.get(key) for key, value in self.mappings.items()}
        return dict(results[0])

    def _do_lookup(
        self, conn: connection, query: str, record_data: RecordData
    ) -> dict[str, Any]:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, record_data)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        return self._process_lookup_results(results, colnames, record_data)

    def process_record(self, record: Record) -> Record:
        conn = self.connection_pool.getconn()
        try:
            lookup_results = self._do_lookup(conn, self.query, record.data)
        finally:
            self.connection_pool.putconn(conn)
        filtered_results = {k: lookup_results[v] for k, v in self.mappings.items()}
        record.data.update(filtered_results)
        return record
