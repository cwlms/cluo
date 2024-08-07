from typing import Any

from psycopg2.extensions import connection
from psycopg2.pool import ThreadedConnectionPool


class PostgresConnectionPool(ThreadedConnectionPool):
    def __init__(
        self,
        minconn: int = 1,
        maxconn: int = 1,
        host: str | None = None,
        port: int = 5432,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        application_name: str | None = None,
    ) -> None:
        """
        Initialize the `PostgresConnectionPool`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            host (str): Database host address.
            port (str): Connection port number.
            dbname (str): Database name.
            user (str): User name to authenticate.
            password (str): Password for user name.
            minconn (int): Minimum connections for pool.
            maxconn (int): Maximum connections for pool.
            application_name (str): Name of application using pool.
        """
        self.minconn = minconn
        self.maxconn = maxconn
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.application_name = application_name
        self.init = False

    def _check_init(self) -> None:
        if not self.init:
            ThreadedConnectionPool.__init__(
                self,
                minconn=self.minconn,
                maxconn=self.maxconn,
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                application_name=self.application_name,
            )
            self.init = True

    def getconn(self, key: Any | None = ...) -> connection:
        self._check_init()
        return super().getconn(key)

    def putconn(
        self, conn: Any | None = ..., key: Any | None = ..., close: bool = False
    ) -> None:
        self._check_init()
        return super().putconn(conn, key, close)
