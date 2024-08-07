from functools import cached_property

import psycopg2
import sqlalchemy


class PostgresConnectionMixin:
    """Mixin class for providing acceess to a Postgres database via `psycopg2` and `sqlalchemy`."""

    def __init__(
        self,
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize the `PostgresConnectionMixin`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            host (str): Database host address.
            port (str): Connection port number.
            dbname (str): Database name.
            user (str): User name to authenticate.
            password (str): Password for user name.
        """
        self._process_args(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

    @cached_property
    def connection(self) -> psycopg2.extensions.connection:
        """psycopg2 connection object. Generated on first access, then cached."""
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port,
            host=self.host,
        )

    @cached_property
    def sql_alchemy_engine(self) -> sqlalchemy.engine.Engine:
        """sqlalchemy engine. Generated on first access, then cached."""
        return sqlalchemy.create_engine(
            "postgresql://", creator=lambda: self.connection
        )

    def _process_args(
        self,
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self.host = "localhost" if host is None else host
        self.port = 5432 if port is None else port
        self.dbname = "public" if dbname is None else dbname
        self.user = "postgres" if user is None else user
        self.password = "password" if password is None else password
