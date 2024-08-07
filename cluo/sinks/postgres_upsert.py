from typing import Any

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import create_engine

from cluo.config import RunMode, config_state
from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, ErrorHandlingMethod, Record, Sink


class PostgresUpsertSink(Sink):
    """For upserting rows to a Postgres table."""

    def __init__(
        self,
        schema: str,
        table: str,
        primary_key: list[str],
        connection_pool: PostgresConnectionPool,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.RAISE,
        expose_metrics: bool = False,
        exclude_columns: list[str] = [],
    ) -> None:
        """Initialize the `PostgresUpsertSink`.

        Args:
            schema (str): Schema containing the table.
            table (str): The table to which to upsert.
            primary_key List[str]: List of fields representing table primary key.
            connection_pool (PostgresConnectionPool): Connection pool to use connections from for upsert.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this sink. Defaults to False.
            exclude_columns (list[str], optional): List of columns to exclude from upsert. Defaults to [].
        """
        Sink.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.schema = schema
        self.table = table
        self.primary_key = primary_key
        self.connection_pool = connection_pool
        self.exclude_columns = exclude_columns
        self._table: Table | None
        self._columns: list[str]

    def process_batch(self, batch: Batch) -> Batch:
        """Write the batch to the database.

        Args:
            batch (Batch): The batch.

        Returns:
            Batch: The batch.
        """

        def _insert_dict(record: Record) -> dict[str, Any]:
            return {k: record.data.get(k) for k in self._columns if k in record.data}

        if config_state.RUN_MODE == RunMode.WRITE:
            self._engine = create_engine(
                "postgresql+psycopg2://", creator=self.connection_pool.getconn
            )
            metadata_obj = MetaData(self._engine, self.schema)
            self._table = Table(self.table, metadata_obj, autoload_with=self._engine)
            self._columns = [
                c.name
                for c in self._table.columns
                if c.name not in self.exclude_columns
            ]
            update_fields = [x for x in self._columns if x not in self.primary_key]
            for record in batch.records:
                update_dict = {k: record.data.get(k) for k in update_fields}
                # remove attributes that do not exist in target to avoid Unconsumed Column exception
                # don't include null values in insert to resolve issues with defaults
                insert_stmt = (
                    insert(self._table)
                    .values(_insert_dict(record))
                    .on_conflict_do_update(
                        index_elements=self.primary_key, set_=dict(update_dict)
                    )
                )

                insert_stmt.execute()

        return batch
