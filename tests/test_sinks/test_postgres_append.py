import psycopg2
import pytest

from cluo.config import RunMode, set_run_mode
from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, Record
from cluo.sinks import PostgresAppendSink

pytestmark = [pytest.mark.postgres_append]


@pytest.fixture()
def get_connection_pool(dbname="", user="", password="", port="", host=""):
    return PostgresConnectionPool(
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        host=host,
        minconn=2,
        maxconn=2,
    )


@pytest.fixture()
def db_with_test_table(get_connection_pool):
    conn = get_connection_pool.getconn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("CREATE TABLE public.test_table (a int, b int);")
    cur.execute("CREATE TABLE public.test_table_default (a serial4 NOT NULL, b int);")
    yield cur
    cur.execute("DROP TABLE public.test_table;")
    cur.execute("DROP TABLE public.test_table_default;")


def test_postgres_append_sink_one_record_write(db_with_test_table, get_connection_pool):
    set_run_mode(RunMode.WRITE)
    s = PostgresAppendSink(
        schema="public",
        table="test_table",
        connection_pool=get_connection_pool,
    )
    batch = Batch([Record(data={"a": 1, "b": 2})])
    s(batch)
    cur = db_with_test_table
    cur.execute("SELECT * FROM public.test_table;")
    assert dict(cur.fetchone()) == {"a": 1, "b": 2}


def test_postgres_append_sink_one_record_preview(
    db_with_test_table, get_connection_pool
):
    set_run_mode(RunMode.PREVIEW)
    s = PostgresAppendSink(
        schema="public",
        table="test_table",
        connection_pool=get_connection_pool,
    )
    batch = Batch([Record(data={"a": 1, "b": 2})])
    s(batch)
    cur = db_with_test_table
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchone() is None


def test_postgres_append_sink_multiple_records_write(
    db_with_test_table, get_connection_pool
):
    set_run_mode(RunMode.WRITE)
    s = PostgresAppendSink(
        schema="public",
        table="test_table",
        connection_pool=get_connection_pool,
    )
    batch = Batch([Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})])
    s(batch)
    cur = db_with_test_table
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchall() == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_postgres_append_sink_multiple_records_write_auto_id(
    db_with_test_table, get_connection_pool
):
    set_run_mode(RunMode.WRITE)
    s = PostgresAppendSink(
        schema="public",
        table="test_table_default",
        connection_pool=get_connection_pool,
    )
    batch = Batch([Record(data={"b": 2}), Record(data={"b": 4})])
    s(batch)
    cur = db_with_test_table
    cur.execute("SELECT * FROM public.test_table_default;")
    assert cur.fetchall() == [{"a": 1, "b": 2}, {"a": 2, "b": 4}]


def test_postgres_append_sink_multiple_records_preview(
    db_with_test_table, get_connection_pool
):
    set_run_mode(RunMode.PREVIEW)
    s = PostgresAppendSink(
        schema="public",
        table="test_table",
        connection_pool=get_connection_pool,
    )
    batch = Batch([Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})])
    s(batch)
    cur = db_with_test_table
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchall() == []
