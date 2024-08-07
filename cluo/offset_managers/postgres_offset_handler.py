from functools import wraps
from typing import Callable

from psycopg2.sql import SQL, Identifier

from cluo.connections import PostgresConnectionPool
from cluo.core.offset_manager import OffsetManager, OffsetType


class PostgresOffsetManager(OffsetManager):
    """Manager for offset values stored in a Postgres database."""

    def __init__(
        self,
        connection_pool: PostgresConnectionPool,
        schema: str,
        table: str,
        key_field: str,
        offset_field: str,
        offset_type: OffsetType | None,
    ) -> None:
        """Initialize the `PostgresOffsetManager`.

        Args:
            connection_pool (PostgresConnectionPool): The connection pool to use.
            schema (str): The database schema name.
            table (str): The database table name.
            key_field (str): The field name which contains the offset key.
            offset_field (str): The field name which  contains the offset value.
            offset_type (PossibleOffsetType): Enum that represents the type of offset value. Defaults to "int".
        """
        super().__init__(offset_type=offset_type)

        self.connection_pool = connection_pool
        self.schema = schema
        self.table = table
        self.key_field = key_field
        self.offset_field = offset_field

        # validate offset data structure
        self._validate()

    def _connect(*args) -> Callable:
        """Decorator function to get and put a Postgres connection from the connection pool and return a cursor."""

        def wrap(f):
            @wraps(f)
            def wrapper(self, *args, **kwargs):
                try:
                    # Setup postgres connection
                    connection = self.connection_pool.getconn()
                    cursor = connection.cursor()

                    # Call function passing in cursor
                    return_val = f(self, cursor, *args, **kwargs)

                finally:
                    # Close connection
                    self.connection_pool.putconn(connection)

                return return_val

            return wrapper

        return wrap

    @_connect()
    def _validate(self, cursor) -> None:
        try:
            cursor.execute(
                SQL("SELECT 1 FROM {schema}.{table} LIMIT 1").format(
                    schema=Identifier(self.schema), table=Identifier(self.table)
                ),
            )
        except Exception as e:
            raise Exception(
                f"Error validating offset table {self.schema}.{self.table}: {e}"
            ) from e

        try:
            cursor.execute(
                SQL("SELECT {key_field} FROM {schema}.{table} LIMIT 1").format(
                    key_field=Identifier(self.key_field),
                    schema=Identifier(self.schema),
                    table=Identifier(self.table),
                )
            )
        except Exception as e:
            raise Exception(
                f"Error validating offset key_field {self.key_field}: {e}"
            ) from e

        try:
            cursor.execute(
                SQL("SELECT {offset_field} FROM {schema}.{table} LIMIT 1").format(
                    offset_field=Identifier(self.offset_field),
                    schema=Identifier(self.schema),
                    table=Identifier(self.table),
                )
            )
        except Exception as e:
            raise Exception(
                f"Error validating offset_field {self.offset_field}: {e}"
            ) from e
        pass

    @_connect()
    def _get(self, cursor) -> str | None:
        """Get the current offset value.

        Returns:
            str: The current offset value.
        """
        cursor.execute(
            SQL(
                "SELECT {offset_field} FROM {schema}.{table} WHERE {key_field} = %s"
            ).format(
                offset_field=Identifier(self.offset_field),
                schema=Identifier(self.schema),
                table=Identifier(self.table),
                key_field=Identifier(self.key_field),
            ),
            [self.key],
        )
        offset = cursor.fetchone()
        if offset is None:
            return None

        return str(offset[0])

    @_connect()
    def _set(self, cursor, value: str) -> bool:
        """Set a new offset

        Args:
            value (str): Value to set to.

        Raises:
            Exception: If error is returned from Postgres.

        Returns:
            bool: Whether set was successful.
        """
        cursor.execute(
            SQL(
                "UPDATE {schema}.{table} SET {offset_field} = %s WHERE {key_field} = %s;"
            ).format(
                schema=Identifier(self.schema),
                table=Identifier(self.table),
                offset_field=Identifier(self.offset_field),
                key_field=Identifier(self.key_field),
            ),
            [value, self.key],
        )
        if cursor.rowcount == 0:
            cursor.execute(
                SQL(
                    "INSERT INTO {schema}.{table} ({key_field}, {offset_field}) VALUES(%s, %s);"
                ).format(
                    schema=Identifier(self.schema),
                    table=Identifier(self.table),
                    key_field=Identifier(self.key_field),
                    offset_field=Identifier(self.offset_field),
                ),
                [self.key, value],
            )
        cursor.connection.commit()

        return True
