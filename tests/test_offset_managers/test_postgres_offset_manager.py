import datetime

import psycopg2
import psycopg2.extras
import pytest

from cluo.connections import PostgresConnectionPool
from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import PostgresOffsetManager

pytestmark = [pytest.mark.postgres_offset_manager]


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
def db_setup_and_teardown(get_connection_pool):
    connection = get_connection_pool.getconn()
    cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "CREATE TABLE public.offsets (key VARCHAR(255) NOT NULL, offset_int INT, offset_datetime TIMESTAMP, offset_str VARCHAR(100));"
    )

    yield cur
    cur.execute("DROP TABLE public.offsets;")


def test_first_run_no_offset_value(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_int",
        offset_type=OffsetType.INT,
    )
    offset_manager.set_key("test_first_run_no_offset")
    assert offset_manager.get() is None


def test_first_run_no_offset(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_int",
        offset_type=OffsetType.INT,
    )
    offset_manager.set_key("test_first_run_no_offset")
    offset_manager.set(1)

    assert offset_manager.get() == 1


def test_datetime_offset(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_datetime",
        offset_type=OffsetType.DATETIME,
    )
    offset_manager.set_key("test_datetime_offset")
    offset_manager.set(datetime.datetime(2021, 1, 1, 0, 0, 0))

    assert offset_manager.get() == datetime.datetime(2021, 1, 1, 0, 0, 0)


def test_str_offset(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_str",
        offset_type=OffsetType.STR,
    )
    offset_manager.set_key("test_datetime_offset")
    offset_manager.set("A")

    assert offset_manager.get() == "A"


def test_offset_update(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_str",
        offset_type=OffsetType.STR,
    )
    offset_manager.set_key("test_string_offset")
    offset_manager.set("A")

    assert offset_manager.get() == "A"

    cur = db_setup_and_teardown
    cur.execute(
        "SELECT offset_str FROM public.offsets WHERE key = 'test_string_offset';"
    )
    assert cur.fetchone() == {"offset_str": "A"}, "offset was set in table"
    offset_manager.set("B")

    assert offset_manager.get() == "B"


def test_type_error(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_str",
        offset_type=OffsetType.STR,
    )
    offset_manager.set_key("test_value_error_offset")

    with pytest.raises(ValueError):
        offset_manager.set(1)


def test_key_error(
    get_connection_pool,
    db_setup_and_teardown,
):
    offset_manager = PostgresOffsetManager(
        connection_pool=get_connection_pool,
        schema="public",
        table="offsets",
        key_field="key",
        offset_field="offset_str",
        offset_type=OffsetType.STR,
    )

    with pytest.raises(ValueError):
        offset_manager.set("B")


def test_table_validate_error(
    get_connection_pool,
    db_setup_and_teardown,
):
    with pytest.raises(Exception):
        PostgresOffsetManager(
            connection_pool=get_connection_pool,
            schema="public",
            table="bad_table",
            key_field="key",
            offset_field="offset_str",
            offset_type=OffsetType.STR,
        )


def test_key_validate_error(
    get_connection_pool,
    db_setup_and_teardown,
):
    with pytest.raises(Exception):
        PostgresOffsetManager(
            connection_pool=get_connection_pool,
            schema="public",
            table="offsets",
            key_field="bad_key",
            offset_field="offset_str",
            offset_type=OffsetType.STR,
        )


def test_offset_validate_error(
    get_connection_pool,
    db_setup_and_teardown,
):
    with pytest.raises(Exception):
        PostgresOffsetManager(
            connection_pool=get_connection_pool,
            schema="public",
            table="offsets",
            key_field="key",
            offset_field="offset_bad",
            offset_type=OffsetType.STR,
        )
