import datetime

import psycopg2
import psycopg2.extras
import pytest

from cluo import config
from cluo.config import RunMode
from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, Pipeline, Record
from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import InMemoryOffsetManager
from cluo.sources import PostgresSelectSource
from tests.conftest import parametrize_processes

pytestmark = [
    pytest.mark.postgres_select,
]
DEBUG = False


def setup_module(module):
    # set up a vs code remote debugging connection when DEBUG = True
    if DEBUG:
        import ptvsd

        ptvsd.enable_attach()
        print("WAITING FOR REMOTE DEBUGGER ATTACH")
        ptvsd.wait_for_attach(timeout=30)


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
        "CREATE TABLE public.companies (id int, city varchar, crunchbase_url varchar);"
    )
    city_url = ("Lander", "https://www.crunchbase.com/organization/facebook")
    args_str = ",".join(
        cur.mogrify("(%s,%s,%s)", (i, *city_url)).decode("utf-8") for i in range(1, 101)
    )
    cur.execute(
        "INSERT INTO public.companies (id, city, crunchbase_url) VALUES " + args_str
    )

    yield cur
    cur.execute("DROP TABLE public.companies;")


@pytest.fixture()
def db_setup_timestamp_offset(get_connection_pool):
    connection = get_connection_pool.getconn()
    cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "CREATE TABLE public.companies (id varchar, last_modified_date varchar null, ts_updated timestamp null)"
    )
    yield cur

    cur.execute("DROP TABLE public.companies")


def test_first_run_no_offset(
    db_setup_and_teardown,
    get_connection_pool,
):
    initial_offset = 0
    batch_size = 10
    config.set_run_mode(RunMode.WRITE)
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    pipeline = Pipeline(name="test_pipeline")
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "city", "crunchbase_url"],
        offset_field="id",
        batch_size=batch_size,
        processes=1,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    expected_batch = Batch(
        records=[
            Record(
                data={
                    "id": i,
                    "city": "Lander",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                }
            )
            for i in range(
                initial_offset + 1, initial_offset + 1 + batch_size
            )  # range: (includes record after initial_offset and batch_size more)
        ]
    )

    with source:
        assert source.process_batch(Batch()).has_same_record_data(expected_batch)

    assert offset_manager.get() == 10


@pytest.mark.parametrize("initial_offset", [0, 1, 2, 10])
@pytest.mark.parametrize("batch_size", [1, 7, 8, 20])
@parametrize_processes(PostgresSelectSource)
def test_select_with_variable_initial_offset_and_batch_size(
    db_setup_and_teardown,
    get_connection_pool,
    initial_offset,
    batch_size,
    processes,
):
    config.set_run_mode(RunMode.WRITE)
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    offset_manager.set(initial_offset)
    pipeline = Pipeline(name="test_pipeline")
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "city", "crunchbase_url"],
        offset_field="id",
        batch_size=batch_size,
        processes=processes,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    expected_batch = Batch(
        records=[
            Record(
                data={
                    "id": i,
                    "city": "Lander",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                }
            )
            for i in range(
                initial_offset + 1, initial_offset + 1 + batch_size
            )  # range: (includes record after initial_offset and batch_size more)
        ]
    )
    offset_manager.set(initial_offset)

    with source:
        assert source.process_batch(Batch()).has_same_record_data(expected_batch)

    assert offset_manager.get() == initial_offset + batch_size


@pytest.mark.parametrize("run_mode", [RunMode.WRITE, RunMode.PREVIEW])
@parametrize_processes(PostgresSelectSource)
def test_select_with_variable_run_mode(
    db_setup_and_teardown, get_connection_pool, run_mode, processes
):
    config.set_run_mode(run_mode)
    pipeline = Pipeline(name="test_pipeline")
    initial_offset = 22
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    offset_manager.set(initial_offset)
    batch_size = 10
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "city", "crunchbase_url"],
        offset_field="id",
        batch_size=batch_size,
        processes=processes,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    expected_batch = Batch(
        records=[
            Record(
                data={
                    "id": i,
                    "city": "Lander",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                }
            )
            for i in range(initial_offset + 1, initial_offset + 1 + batch_size)
        ]
    )

    with source:
        assert source.process_batch(Batch()).has_same_record_data(expected_batch)

    if run_mode == RunMode.WRITE:
        assert offset_manager.get() == initial_offset + batch_size
    elif run_mode == RunMode.PREVIEW:
        assert offset_manager.get() == initial_offset
    else:
        raise Exception("Unknown run mode. Tests need to be updated.")


@parametrize_processes(PostgresSelectSource)
def test_select_with_exception_during_run(
    db_setup_and_teardown, get_connection_pool, processes
):
    config.set_run_mode(RunMode.WRITE)
    pipeline = Pipeline(name="test_pipeline")
    initial_offset = 0
    batch_size = 1
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    offset_manager.set(initial_offset)
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "city", "crunchbase_url"],
        offset_field="id",
        batch_size=batch_size,
        processes=processes,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    expected_batch = Batch(
        records=[
            Record(
                data={
                    "id": i,
                    "city": "Lander",
                    "crunchbase_url": "https://www.crunchbase.com/organization/facebook",
                }
            )
            for i in range(initial_offset + 1, initial_offset + batch_size + 1)
        ]
    )
    offset_manager.set(initial_offset)

    try:
        with source:
            source.process_batch(Batch()) == expected_batch
            raise Exception
    except Exception:
        assert offset_manager.get() == initial_offset


"""
    multi_batch_test_cases are run with a batch size of two records unless otherwise indicated
    each set of numbers below, represents a record set to feed through the source
    test records are loaded with a last modified timestamp of midnight UTC on january 1st, 2020.
    the numbers in the test cases below represent seconds of offset past midnight for the test record.
    test cases pass if all the records in the group are ingested.
"""

multi_batch_test_cases = {
    "one record": {"data": [1]},
    "two records different timestamps": {"data": [1, 2]},
    "two records same timestamps": {"data": [1, 1]},
    "two batches different timestamps": {"data": [1, 2, 3, 4]},
    "first batch same offset": {"data": [1, 1, 2, 3, 4]},
    "multiple timestamps on batch boundary": {"data": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]},
    "multiple timestamps on batch boundary - one not": {
        "data": [1, 1, 2, 2, 3, 3, 3, 4, 4, 5, 5]
    },
    "second batch same timestamp offset two": {"data": [1, 1, 1, 2, 3]},
    "duplicate dates span multiple batches": {
        "data": [1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 6, 6, 6, 6, 6, 7],
    },
    "second batch will reprocess one record": {"data": [1, 2, 2, 2]},
    "multiple spans": {"data": [1, 2, 3, 3, 3, 4, 4, 5]},
    "batch size four, reprocess several records": {
        "data": [1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4],
        "batch_size": 4,
    },
}


@pytest.mark.parametrize(
    "test_case", multi_batch_test_cases.values(), ids=multi_batch_test_cases.keys()
)
def test_multiple_batches(db_setup_timestamp_offset, get_connection_pool, test_case):
    """test multiple batches to ensure records are not skipped when mutiple records have the same offset timestamp."""

    if test_case.get("disabled", False):
        pytest.skip()

    batch_size = test_case.get("batch_size", 2)
    base_timestamp = datetime.datetime(
        year=2020,
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=datetime.timezone.utc,
    )
    timestamp_offsets = test_case.get("data", [])
    input_records = [
        {
            "id": str(seq),
            "ts_updated": (
                base_timestamp + datetime.timedelta(seconds=timestamp_offsets[seq])
            ),
            # ).strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        }
        for seq in range(len(timestamp_offsets))
    ]
    # Load test records into company table
    db_setup_timestamp_offset.executemany(
        "INSERT INTO public.companies(id, ts_updated) VALUES (%(id)s, %(ts_updated)s)",
        input_records,
    )
    pipeline = Pipeline()
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set(
        datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=datetime.timezone.utc,
        )
    )
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "ts_updated"],
        offset_field="ts_updated",
        batch_size=batch_size,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        offset_nonunique=True,
        order_by=["ts_updated", "id"],
    )
    pipeline.add_source(source)

    collected_batch_records = []
    empty_batch = False
    batch_limit = 100
    batch_count = 0
    while not empty_batch and batch_count < batch_limit:
        with source:
            current_batch = source.process_batch(Batch())
        current_records = [dict(x.data) for x in current_batch.records]
        collected_batch_records += current_records
        empty_batch = not current_records
        batch_count += 1

    expected_record_ids = {rec.get("id") for rec in input_records}
    actual_record_ids = {rec.get("id") for rec in collected_batch_records}
    assert (
        expected_record_ids == actual_record_ids
    ), f"actual ids {actual_record_ids} match expected ids {expected_record_ids}"


def test_batch_boundary_offset_progression(
    db_setup_timestamp_offset, get_connection_pool
):
    """
    test the edge case where timestamps change on batch boundaries
    to ensure the date offset is progressed incrementally
    this prevents numeric offset inflation
    """

    timestamp_offsets = [
        1,
        1,
        2,
        2,
        3,
        3,
        3,
        4,
        4,
        4,
        5,
        5,
        6,
    ]
    base_timestamp = datetime.datetime(
        year=2020,
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=datetime.timezone.utc,
    )
    expected_offsets = [
        base_timestamp,
        base_timestamp,
        base_timestamp,
        base_timestamp,
        base_timestamp,
        base_timestamp,
        datetime.datetime(year=2020, month=1, day=1, hour=0, minute=0, second=6),
    ]

    batch_size = 2
    input_records = [
        {
            "id": str(seq),
            "ts_updated": (
                base_timestamp + datetime.timedelta(seconds=timestamp_offsets[seq])
            ).strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        }
        for seq in range(len(timestamp_offsets))
    ]
    # Load test records into company table
    db_setup_timestamp_offset.executemany(
        "INSERT INTO public.companies(id, ts_updated) VALUES (%(id)s, %(ts_updated)s)",
        input_records,
    )
    pipeline = Pipeline()
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set(base_timestamp)
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "ts_updated"],
        offset_field="ts_updated",
        batch_size=batch_size,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        offset_nonunique=True,
        order_by=["ts_updated", "id"],
    )
    pipeline.add_source(source)
    source._set_offset_key()

    collected_batch_records = []
    actual_offsets = []
    empty_batch = False
    batch_limit = 100
    batch_count = 0
    while not empty_batch and batch_count < batch_limit:
        with source:
            current_batch = source.process_batch(Batch())
        current_records = [dict(x.data) for x in current_batch.records]
        collected_batch_records += current_records
        empty_batch = not current_records
        if not empty_batch:
            actual_offsets.append(offset_manager.get())
        batch_count += 1

    expected_record_ids = {rec.get("id") for rec in input_records}
    actual_record_ids = {rec.get("id") for rec in collected_batch_records}
    assert (
        expected_record_ids == actual_record_ids
    ), f"actual ids {actual_record_ids} match expected ids {expected_record_ids}"
    assert actual_offsets == expected_offsets, "offset sequence matches expected"


def test_offset_field_in_order_by(get_connection_pool):
    """postgres_select source should throw and exception when offset field not in order by clause"""
    with pytest.raises(ValueError) as ve:
        offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
        PostgresSelectSource(
            schema="public",
            table="companies",
            fields=["id", "ts_updated"],
            offset_field="ts_updated",
            connection_pool=get_connection_pool,
            offset_manager=offset_manager,
            order_by=["id"],
        )
    expected_exception = "offset field must be in order_by list"
    actual_exception = str(ve)
    assert expected_exception in actual_exception, "raises expected exception"


def test_offset_field_in_select(get_connection_pool):
    """postgres_select source should throw and exception when offset field not in select list"""
    with pytest.raises(ValueError) as ve:
        offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
        PostgresSelectSource(
            schema="public",
            table="companies",
            fields=["ts_updated"],
            offset_field="id",
            connection_pool=get_connection_pool,
            offset_manager=offset_manager,
            order_by=["id"],
        )
    expected_exception = "offset field must be in fields list"
    actual_exception = str(ve)
    assert expected_exception in actual_exception, "raises expected exception"


def test_offset_integer_conversion(db_setup_timestamp_offset, get_connection_pool):
    input_records = [
        {
            "id": "record_1",
            "last_modified_date": "2000-01-01T00:00:00.000+0000",
        },
        {
            "id": "record_2",
            "last_modified_date": "2000-01-03T00:00:00.000+0000",
        },
    ]
    pipeline = Pipeline()
    db_setup_timestamp_offset.executemany(
        "INSERT INTO public.companies(id, last_modified_date) VALUES (%(id)s, %(last_modified_date)s)",
        input_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.STR)
    source = PostgresSelectSource(
        schema="public",
        table="companies",
        fields=["id", "last_modified_date"],
        offset_field="last_modified_date",
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        use_timestamp_offset=True,
        order_by=["last_modified_date", "id"],
    )
    pipeline.add_source(source)

    source._set_offset_key()
    # set the offset as integer
    offset_manager.set("2")

    expected_batch = Batch(
        records=[
            Record(data=input_records[0]),
            Record(data=input_records[1]),
        ]
    )
    with source:
        processed_batch = source.process_batch(Batch())
    assert processed_batch.has_same_record_data(
        expected_batch
    ), "actual batch data matches expected"
    assert (
        offset_manager.get() == "2000-01-03T00:00:00.000+0000"
    ), "updated offset is set in new format"
