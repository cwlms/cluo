import psycopg2
import pytest

from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import PostgresLookupStage
from tests.conftest import parametrize_processes

pytestmark = [
    pytest.mark.postgres_lookup,
]


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
    conn = get_connection_pool.getconn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "CREATE TABLE public.companies (id int, city varchar, crunchbase_url varchar);"
    )
    yield cur
    cur.execute("DROP TABLE public.companies;")


@parametrize_processes(PostgresLookupStage)
def test_postgres_lookup_stage(db_with_companies_table, get_connection_pool, processes):
    cur = db_with_companies_table

    # setup
    cur.execute(
        "INSERT INTO public.companies (id, city, crunchbase_url) VALUES (1234, 'Lima', 'https://www.crunchbase.com/organization/facebook');"
    )

    stage = PostgresLookupStage(
        "SELECT * FROM public.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        connection_pool=get_connection_pool,
        processes=processes,
    )

    assert stage.process_record(Record(data={"city": "Lima", "x": 1})).data == {
        "city": "Lima",
        "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
        "x": 1,
    }
    test_batch = Batch(records=[Record(data={"city": "Lima", "x": 1})])
    ref_batch = Batch(
        records=[
            Record(
                data={
                    "city": "Lima",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                    "x": 1,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    assert processed_batch.has_same_record_data(ref_batch)


@parametrize_processes(PostgresLookupStage)
def test_postgres_lookup_failure(
    db_with_companies_table, get_connection_pool, processes
):
    stage = PostgresLookupStage(
        "SELECT * FROM master.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        processes=processes,
        connection_pool=get_connection_pool,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    test_batch = Batch(records=[Record(data={"city": "Lima", "x": 1})])
    with pytest.raises(psycopg2.errors.UndefinedTable):
        stage(test_batch)


@parametrize_processes(PostgresLookupStage)
def test_postgres_lookup_stage_with_no_companies_table2(
    db_with_companies_table, get_connection_pool, processes
):
    cur = db_with_companies_table
    # setup
    cur.execute(
        "INSERT INTO public.companies (id, city, crunchbase_url) VALUES (1234, 'Lima', 'https://www.crunchbase.com/organization/facebook');"
    )

    stage = PostgresLookupStage(
        "SELECT * FROM public.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        connection_pool=get_connection_pool,
        processes=processes,
    )

    assert stage.process_record(Record(data={"city": "Lima", "x": 1})).data == {
        "city": "Lima",
        "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
        "x": 1,
    }
    test_batch = Batch(records=[Record(data={"city": "Lima", "x": 1})])
    ref_batch = Batch(
        records=[
            Record(
                data={
                    "city": "Lima",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                    "x": 1,
                }
            )
        ]
    )
    processed_batch = stage(test_batch)
    assert processed_batch.has_same_record_data(ref_batch)


@parametrize_processes(PostgresLookupStage)
def test_postgres_lookup_stage_on_zero_rows(
    db_with_companies_table, get_connection_pool, processes
):
    cur = db_with_companies_table
    # setup
    cur.execute(
        "INSERT INTO public.companies (id, city, crunchbase_url) VALUES (1234, 'Not Lima', 'https://www.crunchbase.com/organization/facebook');"
    )

    # default on_zero_rows = 'ignore'
    stage = PostgresLookupStage(
        "SELECT * FROM public.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        connection_pool=get_connection_pool,
        processes=processes,
    )
    assert stage.process_record(Record(data={"city": "Lima", "x": 1})).data == {
        "city": "Lima",
        "crunchbase_url": None,
        "x": 1,
    }
    input_ = Batch(records=[Record(data={"city": "Lima", "x": 1})])
    expected = Batch(
        records=[
            Record(
                data={
                    "city": "Lima",
                    "crunchbase_url": None,
                    "x": 1,
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)

    # default on_zero_rows = 'raise'
    stage = PostgresLookupStage(
        "SELECT * FROM public.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        connection_pool=get_connection_pool,
        processes=processes,
        on_zero_rows="raise",
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    with pytest.raises(Exception):
        stage.process_record(Record(data={"city": "Lima", "x": 1}))

    with pytest.raises(Exception):
        stage(input_)

    # default on_zero_rows = 'preserve'
    stage = PostgresLookupStage(
        "SELECT * FROM public.companies WHERE city = %(city)s LIMIT 1;",
        {"crunchbase_url": "crunchbase_url"},
        connection_pool=get_connection_pool,
        processes=processes,
        on_zero_rows="preserve",
    )
    assert stage.process_record(
        Record(data={"city": "Lima", "x": 1, "crunchbase_url": "raw_crunchbase_value"})
    ).data == {
        "city": "Lima",
        "crunchbase_url": "raw_crunchbase_value",
        "x": 1,
    }
    input_ = Batch(
        records=[
            Record(
                data={"city": "Lima", "x": 1, "crunchbase_url": "raw_crunchbase_value"}
            )
        ]
    )
    expected = Batch(
        records=[
            Record(
                data={
                    "city": "Lima",
                    "crunchbase_url": "raw_crunchbase_value",
                    "x": 1,
                }
            )
        ]
    )
    actual = stage(input_)
    assert actual.has_same_record_data(expected)
