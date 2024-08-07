import psycopg2
import psycopg2.extras
import pytest
import redis
from simple_mockforce import mock_salesforce
from simple_salesforce import Salesforce

from cluo import config
from cluo.config import RunMode
from cluo.connections import PostgresConnectionPool
from cluo.core import Batch, Pipeline
from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import InMemoryOffsetManager
from cluo.sources.salesforce_reflow import SalesforceReflowSource
from tests.helpers import dicts_from_batch

pytestmark = pytest.mark.salesforce_reflow

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
def db_services(get_connection_pool):
    # ALWAYS SET RUN MODE TO WRITE AT BEGINNING OF TEST
    config.set_run_mode(RunMode.WRITE)
    redis_client = redis.Redis(host="", port=0)
    redis_client.flushall()
    connection = get_connection_pool.getconn()
    cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "CREATE TABLE public.sfdc_ids (id int, entity_type varchar, entity_id varchar);"
    )

    yield cur

    redis_client.flushall()
    cur.execute("DROP TABLE public.sfdc_ids;")


@pytest.fixture()
def salesforce_credentials():
    return {
        "username": "test",
        "password": "test",
        "security_token": "123",
        "domain": "mock",
    }


@pytest.fixture()
def salesforce_fields():
    return ["id__c", "value_field_1"]


salesforce_record_sets = {
    "record_set_1": [
        {"id__c": f"{seq_id:018d}", "value_field_1": f"value_field_{seq_id}"}
        for seq_id in range(1, 11)
    ],
    "extra_records": [
        {"id__c": f"{seq_id:018d}", "value_field_1": f"value_field_{seq_id}"}
        for seq_id in range(11, 16)
    ],
    "different_entity": [
        {"id__c": f"{seq_id:018d}", "value_field_1": f"value_field_{seq_id}"}
        for seq_id in range(11, 21)
    ],
}

reflow_record_sets = {
    "record_set_1": [
        {
            "id": seq_id,
            "entity_type": "Company_Event__c",
            "entity_id": f"{seq_id:018d}",
        }
        for seq_id in range(1, 11)
    ],
    "extra_records": [
        {
            "id": seq_id,
            "entity_type": "Company_Event__c",
            "entity_id": f"{seq_id:018d}",
        }
        for seq_id in range(11, 16)
    ],
    "different_entity": [
        {
            "id": seq_id,
            "entity_type": "Different_Entity__c",
            "entity_id": f"{seq_id:018d}",
        }
        for seq_id in range(11, 21)
    ],
}


@mock_salesforce
@pytest.mark.parametrize("run_mode", [RunMode.WRITE, RunMode.PREVIEW])
def test_happy_path(
    run_mode,
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
):
    initial_offset = 0
    salesforce_records = salesforce_record_sets["record_set_1"]
    reflow_records = reflow_record_sets["record_set_1"]

    # insert reflow records
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records,
    )

    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    # Load test records using simple-mockforce
    for record in salesforce_records:
        salesforce.Company_Event__c.create(record)

    # Initialize pipeline
    config.set_run_mode(run_mode)
    pipeline = Pipeline(name="test_pipeline")

    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=10,
    )

    pipeline.add_source(source)
    source._set_offset_key()
    # set initial offset so we can test that it does not change in preview mode
    offset_manager.set(initial_offset)

    expected_dicts = salesforce_records

    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    expected_offset = 10 if run_mode == RunMode.WRITE else initial_offset
    assert actual_offset == expected_offset, "offset matches expected"


@mock_salesforce
def test_select_past_current_offset(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
):
    # INITIAL OFFSET IS PAST AVAILABLE DATA IN REFLOW TABLE
    initial_offset = 15
    salesforce_records = salesforce_record_sets["record_set_1"]
    reflow_records = reflow_record_sets["record_set_1"]
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    for record in salesforce_records:
        salesforce.Company_Event__c.create(record)

    pipeline = Pipeline(name="test_pipeline")
    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=20,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    # set initial offset so we can test that it does not change
    offset_manager.set(initial_offset)
    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    # EXPECT TO RETURN AN EMPTY BATCH
    expected_dicts = []
    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    # OFFSET DOES NOT CHANGE: NO RECORDS RETURNED
    expected_offset = initial_offset
    assert actual_offset == expected_offset, "offset matches expected"


batch_size_test_cases = {
    "batch smaller than limit": {"input": 5, "expected": 5},
    # calculated limit for test entity and field list is 679
    "batch larger than limit": {"input": 5000, "expected": 679},
}


@mock_salesforce
@pytest.mark.parametrize(
    "test_case", batch_size_test_cases.values(), ids=batch_size_test_cases.keys()
)
def test_effective_batch_size(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
    test_case,
):
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    """salesforce reflow source should raise exception with batch size > 189"""
    reflow_source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=test_case.get("input"),
    )
    assert (
        test_case.get("expected") == reflow_source.batch_size
    ), "final batch size matches expected"


@mock_salesforce
@pytest.mark.parametrize("include_deleted", [True, False])
def test_ingest_deleted_records(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
    include_deleted,
):
    salesforce_records = salesforce_record_sets["record_set_1"]
    reflow_records = reflow_record_sets["record_set_1"]
    # insert reflow records
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    # salesforce_id_map key = record id__c, value = assigned salesforce id
    salesforce_id_map: dict[str, str] = {}
    for record in salesforce_records:
        create_response = salesforce.Company_Event__c.create(record)
        salesforce_id_map[record.get("id__c")] = create_response.get("id")
    # delete first and last salesforce record
    deleted_ids = [
        salesforce_records[0].get("id__c"),
        salesforce_records[-1].get("id__c"),
    ]
    for deleted_id in deleted_ids:
        salesforce.Company_Event__c.delete(salesforce_id_map[deleted_id])
    expected_dicts = (
        salesforce_records
        if include_deleted
        else [
            sf_rec
            for sf_rec in salesforce_records
            if sf_rec.get("id__c") not in deleted_ids
        ]
    )

    pipeline = Pipeline(name="test_pipeline")
    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=20,
        include_deleted=include_deleted,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    expected_offset = 10
    assert actual_offset == expected_offset, "offset matches expected"


@mock_salesforce
def test_only_ids_in_list(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
):
    salesforce_records = salesforce_record_sets["record_set_1"]
    extra_salesforce_records = salesforce_record_sets["extra_records"]
    reflow_records = reflow_record_sets["record_set_1"]
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    for record in salesforce_records + extra_salesforce_records:
        salesforce.Company_Event__c.create(record)

    pipeline = Pipeline(name="test_pipeline")
    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=20,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    expected_dicts = salesforce_records
    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    expected_offset = 10
    assert actual_offset == expected_offset, "offset matches expected"


@mock_salesforce
def test_ids_missing_from_salesforce(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
):
    salesforce_records = salesforce_record_sets["record_set_1"]
    reflow_records = reflow_record_sets["record_set_1"]
    extra_reflow_records = reflow_record_sets["extra_records"]
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records + extra_reflow_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    for record in salesforce_records:
        salesforce.Company_Event__c.create(record)

    pipeline = Pipeline(name="test_pipeline")
    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=20,
    )
    pipeline.add_source(source)
    source._set_offset_key()
    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    expected_dicts = salesforce_records
    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    # offset 15 because we added 5 extra records
    expected_offset = 15
    assert actual_offset == expected_offset, "offset matches expected"


@mock_salesforce
def test_custom_where_clause(
    db_services,
    get_connection_pool,
    salesforce_fields,
    salesforce_credentials,
):
    # add extra records to reflow table with different entity type
    reflow_records = (
        reflow_record_sets["record_set_1"] + reflow_record_sets["different_entity"]
    )
    db_services.executemany(
        "INSERT INTO public.sfdc_ids(id, entity_type, entity_id) VALUES (%(id)s, %(entity_type)s, %(entity_id)s)",
        reflow_records,
    )
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    salesforce = Salesforce(**salesforce_credentials)
    salesforce_records = salesforce_record_sets["record_set_1"]
    extra_salesforce_records = salesforce_record_sets["different_entity"]
    # note we are adding them all to the same entity type cuz we always select from the same entity in salesforce
    for record in salesforce_records + extra_salesforce_records:
        salesforce.Company_Event__c.create(record)

    pipeline = Pipeline(name="test_pipeline")
    source = SalesforceReflowSource(
        entity="Company_Event__c",
        fields=salesforce_fields,
        **salesforce_credentials,
        connection_pool=get_connection_pool,
        offset_manager=offset_manager,
        reflow_schema="public",
        reflow_table="sfdc_ids",
        reflow_id_field="entity_id",
        reflow_offset_field="id",
        id_field="id__c",
        batch_size=40,
        reflow_where_string="entity_type='Company_Event__c'",
    )
    pipeline.add_source(source)
    source._set_offset_key()
    with source:
        actual_batch = source.process_batch(Batch())
        actual_dicts = dicts_from_batch(actual_batch)

    expected_dicts = salesforce_records
    assert actual_dicts == expected_dicts, "processed records match expected"
    actual_offset = offset_manager.get()
    expected_offset = 10
    assert actual_offset == expected_offset, "offset matches expected"
