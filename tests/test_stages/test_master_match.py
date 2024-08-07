import datetime

import psycopg2
import pytest

from cluo.connections import PostgresConnectionPool
from cluo.core import Record
from cluo.stages import MasterMatchStage
from tests.conftest import parametrize_processes

pytestmark = [
    pytest.mark.master_match,
    pytest.mark.matching,
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
    cur.execute("CREATE SCHEMA clean;")
    cur.execute("CREATE SCHEMA master;")
    companies_clean_ddl = """
        CREATE TABLE clean.companies (id int, company_name varchar, website varchar, brief_description varchar,
        array_hash text[], data_source_key varchar, ts_status varchar, ts_updated timestamp, date_field date,
        int_field int
        )
    """
    cur.execute(companies_clean_ddl)
    cur.execute(
        "CREATE TABLE master.companies (id int, company_name varchar, website varchar, data_source_keys text[], ts_status varchar, ts_updated timestamp, date_field date);"
    )
    yield cur
    cur.execute("DROP TABLE clean.companies;")
    cur.execute("DROP TABLE master.companies;")
    cur.execute("DROP SCHEMA clean;")
    cur.execute("DROP SCHEMA master;")


@parametrize_processes(MasterMatchStage)
def test_master_match_stage(db_with_companies_table, get_connection_pool, processes):
    cur = db_with_companies_table

    # setup
    cur.execute(
        "INSERT INTO clean.companies (id, company_name, website, array_hash, data_source_key, ts_updated) VALUES (1234, 'Test Company', 'testcompany.com', '{\"hash1\"}', 'test_id1', '2020-01-01 01:01:01.000'::timestamp);"
    )
    cur.execute(
        "INSERT INTO master.companies (id, company_name, website, data_source_keys) VALUES (1234, 'Test Company', 'testcompany.com', '{\"test_id1\"}');"
    )

    stage = MasterMatchStage(
        master_table="companies",
        connection_pool=get_connection_pool,
        match_ratio=0.25,
        processes=processes,
    )

    assert stage.process_record(
        Record(
            data={
                "id": "4321",
                "company_name": "Test Company Rebranded",
                "website": "testcompany.com",
                "array_hash": ["hash1", "hash2"],
                "data_source_key": "test_id1",
            }
        )
    ).data == {
        "id": "4321",
        "company_name": "Test Company Rebranded",
        "website": "testcompany.com",
        "array_hash": ["hash1", "hash2"],
        "data_source_key": "test_id1",
        "matches": [
            {
                "id": 1234,
                "company_name": "Test Company",
                "website": "testcompany.com",
                "brief_description": None,
                "array_hash": ["hash1"],
                "data_source_key": "test_id1",
                "ts_status": None,
                "ts_updated": datetime.datetime(2020, 1, 1, 1, 1, 1, 0),
                "date_field": None,
                "int_field": None,
            }
        ],
    }


@parametrize_processes(MasterMatchStage)
def test_master_match_timestamp_date_parse(
    db_with_companies_table, get_connection_pool, processes
):
    cur = db_with_companies_table

    # setup
    cur.execute(
        "INSERT INTO clean.companies (id, company_name, website, array_hash, data_source_key, ts_updated, date_field) VALUES (1234, 'Test Company', 'testcompany.com', '{\"hash1\"}', 'test_id1', '2022-09-19 14:25:56.092'::timestamp, '1996-10-31'::date);"
    )
    cur.execute(
        "INSERT INTO master.companies (id, company_name, website, data_source_keys) VALUES (1234, 'Test Company', 'testcompany.com', '{\"test_id1\"}');"
    )

    stage = MasterMatchStage(
        master_table="companies",
        connection_pool=get_connection_pool,
        match_ratio=0.25,
        processes=processes,
        timestamp_fields=["ts_updated"],
        date_fields=["date_field"],
    )

    assert stage.process_record(
        Record(
            data={
                "id": "4321",
                "company_name": "Test Company Rebranded",
                "website": "testcompany.com",
                "array_hash": ["hash1", "hash2"],
                "data_source_key": "test_id1",
            }
        )
    ).data == {
        "id": "4321",
        "company_name": "Test Company Rebranded",
        "website": "testcompany.com",
        "array_hash": ["hash1", "hash2"],
        "data_source_key": "test_id1",
        "matches": [
            {
                "id": 1234,
                "company_name": "Test Company",
                "website": "testcompany.com",
                "brief_description": None,
                "array_hash": ["hash1"],
                "data_source_key": "test_id1",
                "ts_status": None,
                "ts_updated": datetime.datetime(2022, 9, 19, 14, 25, 56, 92000),
                "date_field": datetime.date(1996, 10, 31),
                "int_field": None,
            }
        ],
    }


@parametrize_processes(MasterMatchStage)
def test_master_match_order_by_updated(
    db_with_companies_table, get_connection_pool, processes
):
    cur = db_with_companies_table
    cur.execute(
        """
            INSERT INTO clean.companies (id, company_name, website, array_hash, data_source_key, ts_updated) VALUES
                (1234, 'Test Company',   'testcompany.com',  '{\"hash1\"}', 'test_id1', '2020-01-01 01:01:01.000'::timestamp),
                (1235, 'Test Company 2', 'testcompany2.com', '{\"hash2\"}', 'test_id2', '2020-01-01 01:01:01.001'::timestamp)
        """
    )
    cur.execute(
        "INSERT INTO master.companies (id, company_name, website, data_source_keys) VALUES (1234, 'Test Company', 'testcompany.com', '{\"test_id1\", \"test_id2\"}');"
    )

    stage = MasterMatchStage(
        master_table="companies",
        connection_pool=get_connection_pool,
        match_ratio=0.25,
        processes=processes,
    )

    assert stage.process_record(
        Record(
            data={
                "id": "4321",
                "company_name": "Test Company Rebranded",
                "website": "testcompany.com",
                "array_hash": ["hash1", "hash2"],
                "data_source_key": "test_id1",
            }
        )
    ).data == {
        "id": "4321",
        "company_name": "Test Company Rebranded",
        "website": "testcompany.com",
        "array_hash": ["hash1", "hash2"],
        "data_source_key": "test_id1",
        "matches": [
            {
                "id": 1235,
                "company_name": "Test Company 2",
                "website": "testcompany2.com",
                "brief_description": None,
                "array_hash": ["hash2"],
                "data_source_key": "test_id2",
                "ts_status": None,
                "ts_updated": datetime.datetime(2020, 1, 1, 1, 1, 1, 1000),
                "date_field": None,
                "int_field": None,
            },
            {
                "id": 1234,
                "company_name": "Test Company",
                "website": "testcompany.com",
                "brief_description": None,
                "array_hash": ["hash1"],
                "data_source_key": "test_id1",
                "ts_status": None,
                "ts_updated": datetime.datetime(2020, 1, 1, 1, 1, 1, 0),
                "date_field": None,
                "int_field": None,
            },
        ],
    }


@parametrize_processes(MasterMatchStage)
def test_master_match_no_convert_no_sort(
    db_with_companies_table, get_connection_pool, processes
):
    cur = db_with_companies_table
    cur.execute(
        """
            INSERT INTO clean.companies (id, company_name, website, array_hash, data_source_key, ts_updated) VALUES
                (1234, 'Test Company',   'testcompany.com',  '{\"hash1\"}', 'test_id1', '2020-01-01 01:01:01.000'::timestamp),
                (1235, 'Test Company 2', 'testcompany2.com', '{\"hash2\"}', 'test_id2', '2020-01-01 01:01:01.001'::timestamp)
        """
    )
    cur.execute(
        "INSERT INTO master.companies (id, company_name, website, data_source_keys) VALUES (1234, 'Test Company', 'testcompany.com', '{\"test_id1\", \"test_id2\"}');"
    )

    stage = MasterMatchStage(
        master_table="companies",
        connection_pool=get_connection_pool,
        match_ratio=0.25,
        processes=processes,
        timestamp_fields=None,
        sort_fields=None,
    )

    assert stage.process_record(
        Record(
            data={
                "id": "4321",
                "company_name": "Test Company Rebranded",
                "website": "testcompany.com",
                "array_hash": ["hash1", "hash2"],
                "data_source_key": "test_id1",
            }
        )
    ).data == {
        "id": "4321",
        "company_name": "Test Company Rebranded",
        "website": "testcompany.com",
        "array_hash": ["hash1", "hash2"],
        "data_source_key": "test_id1",
        "matches": [
            {
                "id": 1234,
                "company_name": "Test Company",
                "website": "testcompany.com",
                "brief_description": None,
                "array_hash": ["hash1"],
                "data_source_key": "test_id1",
                "ts_status": None,
                "ts_updated": "2020-01-01T01:01:01",
                "date_field": None,
                "int_field": None,
            },
            {
                "id": 1235,
                "company_name": "Test Company 2",
                "website": "testcompany2.com",
                "brief_description": None,
                "array_hash": ["hash2"],
                "data_source_key": "test_id2",
                "ts_status": None,
                "ts_updated": "2020-01-01T01:01:01.001",
                "date_field": None,
                "int_field": None,
            },
        ],
    }


@pytest.mark.parametrize(
    "sort_reverse_param",
    [True, False],
)
@parametrize_processes(MasterMatchStage)
def test_master_match_order_by_custom(
    db_with_companies_table, get_connection_pool, processes, sort_reverse_param
):
    cur = db_with_companies_table
    cur.execute(
        """
            INSERT INTO clean.companies (id,array_hash, data_source_key, brief_description, int_field) VALUES
                (1, '{\"hash1\"}', 'test_id1', null, null),
                (2, '{\"hash1\"}', 'test_id2', 'c_bd', null),
                (3, '{\"hash1\"}', 'test_id3', 'b_bd', null),
                (4, '{\"hash1\"}', 'test_id4', 'a_bd', 9),
                (5, '{\"hash1\"}', 'test_id5', 'c_bd', null),
                (6, '{\"hash1\"}', 'test_id6', null, 9),
                (7, '{\"hash1\"}', 'test_id7', null, 10),
                (8, '{\"hash1\"}', 'test_id8', 'a_bd', 10)
        """
    )
    cur.execute(
        "INSERT INTO master.companies (id, data_source_keys) VALUES (1, '{\"test_id1\"}');"
    )

    stage = MasterMatchStage(
        master_table="companies",
        connection_pool=get_connection_pool,
        match_ratio=0.25,
        processes=processes,
        timestamp_fields=None,
        sort_fields=["missing_field", "brief_description", "website", "int_field"],
        sort_reverse=sort_reverse_param,
    )

    def _match_fixture(id, descr, int_f):
        return {
            "id": id,
            "brief_description": descr,
            "int_field": int_f,
            "array_hash": ["hash1"],
            "data_source_key": f"test_id{id}",
            "company_name": None,
            "website": None,
            "ts_status": None,
            "ts_updated": None,
            "date_field": None,
        }

    # expected sort orders by sorted_reverse parameter nulls ALWAYS come last
    matches_lists = {
        True: [
            _match_fixture(2, "c_bd", None),
            _match_fixture(5, "c_bd", None),
            _match_fixture(3, "b_bd", None),
            _match_fixture(8, "a_bd", 10),
            _match_fixture(4, "a_bd", 9),
            _match_fixture(7, None, 10),
            _match_fixture(6, None, 9),
            _match_fixture(1, None, None),
        ],
        False: [
            _match_fixture(4, "a_bd", 9),
            _match_fixture(8, "a_bd", 10),
            _match_fixture(3, "b_bd", None),
            _match_fixture(2, "c_bd", None),
            _match_fixture(5, "c_bd", None),
            _match_fixture(6, None, 9),
            _match_fixture(7, None, 10),
            _match_fixture(1, None, None),
        ],
    }

    assert stage.process_record(
        Record(
            data={
                "id": "1",
                "array_hash": ["hash1"],
                "data_source_key": "test_id1",
            }
        )
    ).data == {
        "id": "1",
        "array_hash": ["hash1"],
        "data_source_key": "test_id1",
        "matches": matches_lists[sort_reverse_param],
    }
