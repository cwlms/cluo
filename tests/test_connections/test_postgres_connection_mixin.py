import psycopg2
import psycopg2.extras
import sqlalchemy

from cluo.connections import PostgresConnectionMixin


def test_postgres_connection_mixin():
    pgconn_mixin = PostgresConnectionMixin(
        host="", port="", dbname="", user="", password=""
    )
    cursor = pgconn_mixin.connection.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    cursor.execute("SELECT 1;")
    assert cursor.fetchone() == psycopg2.extras.RealDictRow([("?column?", 1)])

    assert isinstance(pgconn_mixin.sql_alchemy_engine, sqlalchemy.engine.Engine)
