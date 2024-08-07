import psycopg2
import pytest

from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import PostgresReferenceLookupStage
from tests.conftest import parametrize_processes


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
def db_with_companies_table(get_connection_pool):
    connection = get_connection_pool.getconn()
    cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("CREATE SCHEMA reference;")
    cur.execute("CREATE TABLE reference.currency_type (currency_type VARCHAR);")
    currencies = ["USD", "EUR", "CAD", "GBP", "CDN", "AUD", "SGD"]
    for currency in currencies:
        cur.execute(
            f"INSERT INTO reference.currency_type (currency_type) VALUES ('{currency}');"
        )
    yield cur
    cur.execute("DROP TABLE reference.currency_type;")
    cur.execute("DROP SCHEMA reference;")


@parametrize_processes(PostgresReferenceLookupStage)
def test_call(db_with_companies_table, get_connection_pool, processes):
    stage = PostgresReferenceLookupStage(
        table="currency_type",
        table_match_field="currency_type",
        record_match_field="native_currency",
        table_get_field="currency_type",
        record_set_field="native_currency",
        connection_pool=get_connection_pool,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"native_currency": "USD"})])
    actual = stage(input_)
    expected = Batch(records=[Record(data={"native_currency": "USD"})])
    assert actual.has_same_record_data(expected)

    input_ = Batch(records=[Record(data={"native_currency": "USD    "})])
    actual = stage(input_)
    expected = Batch(records=[Record(data={"native_currency": None})])
    assert actual.has_same_record_data(expected)

    input_ = Batch(records=[Record(data={"native_currency": "cad"})])
    actual = stage(input_)
    expected = Batch(records=[Record(data={"native_currency": None})])
    assert actual.has_same_record_data(expected)
