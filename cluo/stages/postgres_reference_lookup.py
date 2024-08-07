from psycopg2 import sql

from cluo.connections import PostgresConnectionPool
from cluo.core import ErrorHandlingMethod
from cluo.stages import PostgresLookupStage


class PostgresReferenceLookupStage(PostgresLookupStage):
    """Convience constructor for `PostgresLookupBuilderStage` for doing reference table lookups."""

    def __new__(  # type: ignore
        self,
        table: str,
        table_match_field: str,
        record_match_field: str,
        table_get_field: str,
        record_set_field: str,
        connection_pool: PostgresConnectionPool,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
    ) -> PostgresLookupStage:
        query = sql.SQL(
            f"SELECT {table_get_field} AS {record_set_field} FROM reference.{table} WHERE {table_match_field} = %({record_match_field})s;"
        ).format(
            table_get_field=sql.Identifier(table_get_field),
            record_set_field=sql.Identifier(record_set_field),
            table=sql.Identifier(table),
            table_match_field=sql.Identifier(table_match_field),
        )
        mappings = {record_set_field: record_set_field}
        return PostgresLookupStage(
            query,
            mappings,
            connection_pool=connection_pool,
            on_zero_rows="ignore",
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
        )
