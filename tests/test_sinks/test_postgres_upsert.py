import psycopg2
import pytest

from cluo.config import RunMode, set_run_mode
from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, Record
from cluo.sinks import PostgresUpsertSink


@pytest.fixture()
def get_connection_pool(dbname="", user="", password="", port="", host=""):
    """Return a PostgresConnectionPool."""
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
    """Create a test table and return a cursor."""
    conn = get_connection_pool.getconn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "CREATE TABLE public.test_table (a int PRIMARY KEY, b int, c _text NULL);"
    )
    yield cur
    cur.execute("DROP TABLE public.test_table;")


def test_preview_single_record(db_with_test_table, get_connection_pool):
    """Test preview mode with a single record."""
    set_run_mode(RunMode.PREVIEW)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key="a",
        connection_pool=get_connection_pool,
    )
    batch = Batch(records=[Record(data={"a": 1, "b": 2})])
    s(batch)
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchone() is None


def test_preview_multiple_records(db_with_test_table, get_connection_pool):
    """Test preview mode with multiple records."""
    set_run_mode(RunMode.PREVIEW)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch = Batch(
        records=[Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})]
    )
    s(batch)
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchall() == []


def test_insert_single_record(db_with_test_table, get_connection_pool):
    """Test insert mode with a single record."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch = Batch(records=[Record(data={"a": 1, "b": 2})])
    s(batch)
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchone() == {"a": 1, "b": 2, "c": None}


def test_insert_multiple_records(db_with_test_table, get_connection_pool):
    """Test insert mode with multiple records."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch = Batch(
        records=[Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})]
    )
    s(batch)
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchall() == [
        {"a": 1, "b": 2, "c": None},
        {"a": 3, "b": 4, "c": None},
    ]


def test_update_single_record(db_with_test_table, get_connection_pool):
    """Test update mode with a single record."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch1 = Batch(records=[Record(data={"a": 1, "b": 2})])
    batch2 = Batch(records=[Record(data={"a": 1, "b": 3})])
    select = "SELECT * FROM public.test_table;"
    s(batch1)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": 2, "c": None}
    s(batch2)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": 3, "c": None}


def test_update_multiple_records(db_with_test_table, get_connection_pool):
    """Test update mode with multiple records."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch1 = Batch(
        records=[Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})]
    )
    batch2 = Batch(records=[Record(data={"a": 3, "b": 5})])
    select = "SELECT * FROM public.test_table;"
    s(batch1)
    cur.execute(select)
    assert cur.fetchall() == [
        {"a": 1, "b": 2, "c": None},
        {"a": 3, "b": 4, "c": None},
    ]
    s(batch2)
    cur.execute(select)
    assert cur.fetchall() == [
        {"a": 1, "b": 2, "c": None},
        {"a": 3, "b": 5, "c": None},
    ]


def test_array(db_with_test_table, get_connection_pool):
    """Test the array data type."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch = Batch(records=[Record(data={"a": 1, "b": 2, "c": ["1st", "2nd"]})])
    s(batch)
    cur.execute("SELECT * FROM public.test_table;")
    assert cur.fetchone() == {"a": 1, "b": 2, "c": ["1st", "2nd"]}


def test_unnecessary_column(db_with_test_table, get_connection_pool):
    """Test unnecessary column to ensure it is not considered for the upsert."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
    )
    batch1 = Batch(records=[Record(data={"a": 1, "b": 2, "d": 12})])
    batch2 = Batch(records=[Record(data={"a": 1, "b": 3, "d": 14})])
    select = "SELECT * FROM public.test_table;"
    s(batch1)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": 2, "c": None}
    s(batch2)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": 3, "c": None}


def test_exclude_columns(db_with_test_table, get_connection_pool):
    """Test exclude columns to ensure they are not considered for the upsert."""
    set_run_mode(RunMode.WRITE)
    cur = db_with_test_table
    s = PostgresUpsertSink(
        schema="public",
        table="test_table",
        primary_key=["a"],
        connection_pool=get_connection_pool,
        exclude_columns=["b"],
    )
    batch1 = Batch(records=[Record(data={"a": 1, "b": 2})])
    batch2 = Batch(records=[Record(data={"a": 1, "b": 3})])
    select = "SELECT * FROM public.test_table;"
    s(batch1)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": None, "c": None}
    cur.execute("UPDATE public.test_table SET b = 2 WHERE a = 1;")
    s(batch2)
    cur.execute(select)
    assert cur.fetchone() == {"a": 1, "b": 2, "c": None}
