import json
import os
from typing import Any, Callable, Generator, Type
from unittest.mock import Mock

import avro.schema
import backoff
import psycopg2
import pytest
import redis
import requests
from _pytest.mark.structures import MarkDecorator
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from pytest_mock import MockerFixture
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cluo.core import ErrorHandlingMethod, Record, Stage
from cluo.stages import PythonCallableStage


@pytest.fixture(scope="session")
def kafka_test_servers(docker_ip, docker_services) -> str:
    port = docker_services.port_for("kafka", 9092)
    return f"{docker_ip}:{port}"


@pytest.fixture(scope="session")
def registry_test_server(docker_ip, docker_services) -> str:
    port = docker_services.port_for("registry", 8081)
    return f"http://{docker_ip}:{port}"


@pytest.fixture(scope="module")
def wait_for_registry(registry_test_server) -> None:
    """
    Wait for the confluent schema registry service to become available
    DO NOT use this fixture standalone
    registry_test_schema_id fixture creates a separate registry schema entry by test name
    that will be required to make your tests work
    """
    request_session = requests.Session()
    Retry.DEFAULT_BACKOFF_MAX = 1.5
    retries = Retry(
        total=200, backoff_factor=0.75, status_forcelist=[500, 502, 503, 504]
    )
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    request_session.get(f"{registry_test_server}/schemas/types")


@pytest.fixture(scope="function")
def kafka_test_topic(request) -> str:
    return request.node.name


@pytest.fixture(scope="function")
def registry_test_subject(request) -> str:
    return request.node.name


@pytest.fixture(scope="function")
def registry_test_schema() -> dict[Any, Any]:
    schema_definition = {
        "type": "record",
        "name": "RegistryTestSchema",
        "namespace": "com.techstars.edp.kafka",
        "fields": [
            {"name": "test_field_1", "type": "string"},
            {"name": "test_field_2", "type": "int"},
        ],
    }
    return schema_definition


@pytest.fixture(scope="function")
def registry_test_schema_avro(registry_test_schema) -> dict[Any, Any]:
    return avro.schema.parse(json.dumps(registry_test_schema))


@pytest.fixture(scope="function")
def kafka_test_consumer_group(request) -> str:
    return request.node.name


@backoff.on_exception(
    backoff.constant, KafkaException, jitter=None, max_tries=10, interval=3
)
def create_kafka_topic(admin_client, topics):
    """backoff-decorated wrapper on create topics to force wait on kafka server startup"""
    admin_client.create_topics(topics)
    return True


@pytest.fixture(scope="function")
def kafka_admin_client(
    kafka_test_servers, kafka_test_topic, wait_for_registry
) -> Generator[AdminClient, None, None]:
    admin_conf = {"bootstrap.servers": kafka_test_servers}
    admin_client = AdminClient(admin_conf)
    topics = [NewTopic(kafka_test_topic, 1, 1)]
    create_kafka_topic(admin_client, topics)
    yield admin_client
    admin_client.delete_topics(topics)


@pytest.fixture(scope="function")
def registry_test_schema_id(
    registry_test_subject, registry_test_server, registry_test_schema, wait_for_registry
) -> Generator[int, None, None]:
    registry_schema_definition = Schema(
        schema_str=json.dumps(registry_test_schema), schema_type="AVRO"
    )
    registry_client = SchemaRegistryClient({"url": registry_test_server})
    schema_id = registry_client.register_schema(
        subject_name=registry_test_subject, schema=registry_schema_definition
    )
    yield schema_id


@pytest.fixture(scope="function")
def kafka_consumer(
    kafka_admin_client: AdminClient,
    kafka_test_servers: str,
    kafka_test_topic: str,
    kafka_test_consumer_group: str,
) -> Generator[Consumer, None, None]:
    # requests kafka_admin_client to make sure topics
    # are created before / deleted after the consumer
    consumer_conf = {
        "bootstrap.servers": kafka_test_servers,
        "group.id": kafka_test_consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_test_topic])
    yield consumer
    consumer.commit()
    consumer.close()


@pytest.fixture(scope="function")
def kafka_producer(
    kafka_admin_client: AdminClient,
    kafka_test_servers: str,
) -> Generator[Producer, None, None]:
    # requests kafka_admin_client to make sure topics
    # are created before / deleted after the producer
    producer_conf = {"bootstrap.servers": kafka_test_servers}
    yield Producer(producer_conf)


@pytest.fixture(scope="function")
def kafka_avro_producer(
    kafka_admin_client: AdminClient,
    kafka_test_servers: str,
    registry_test_server: str,
    wait_for_registry,
) -> Generator[AvroProducer, None, None]:
    # requests kafka_admin_client to make sure topics
    # are created before / deleted after the producer
    producer_conf = {
        "bootstrap.servers": kafka_test_servers,
        "schema.registry.url": registry_test_server,
    }
    yield AvroProducer(producer_conf)


@backoff.on_exception(
    backoff.constant, psycopg2.OperationalError, jitter=None, max_tries=10, interval=3
)
def get_postgres_db_connection(docker_services, docker_ip):
    db_settings = {
        "database": "postgres",
        "user": "postgres",
        "host": docker_ip,
        "password": "example",
        "port": docker_services.port_for("postgres", 5432),
        "application_name": "test-app",
    }
    db_connection = psycopg2.connect(**db_settings)
    db_connection.autocommit = True
    return db_connection


@pytest.fixture(scope="session")
def postgres_db_connection(
    docker_services, docker_ip
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Uses the pytest-docker plugin fixtures to run Postgres DB service for tests."""
    db_connection = get_postgres_db_connection(docker_services, docker_ip)
    yield db_connection
    db_connection.close()


@pytest.fixture(autouse=True)
def _mock_postgres_db_connection(
    mocker: MockerFixture, postgres_db_connection: psycopg2.extensions.connection
) -> Mock:
    """pytest will use this fixture for tests"""
    return mocker.patch("psycopg2.connect", return_value=postgres_db_connection)


@pytest.fixture(scope="session")
def redis_db_connection(docker_services, docker_ip) -> redis.Redis:
    """Uses the pytest-docker plugin fixtures to run Redis DB service for tests."""
    return redis.Redis(
        host=docker_ip, port=docker_services.port_for("redis", 6379), db=0
    )


@pytest.fixture(autouse=True)
def _mock_redis_db_connection(
    mocker: MockerFixture, redis_db_connection: redis.Redis
) -> Mock:
    """pytest will use this fixture for all tests"""
    return mocker.patch("redis.Redis", return_value=redis_db_connection)


def skip_if_gh_actions(testfunc: Callable) -> Callable:
    return pytest.mark.skipif(
        os.environ.get("GITHUB_ACTIONS") == "true", reason="GitHub Actions"
    )(testfunc)


def parametrize_processes(stage: Type[Stage]) -> MarkDecorator:
    processes = [1, 2] if hasattr(stage, "process_records") else [1]
    return pytest.mark.parametrize("processes", processes)


def _error_func(record: Record) -> Record:
    raise ValueError("A wild error appeared!")


@pytest.fixture(scope="function")
def send_to_error_stage() -> Stage:
    error_stage = PythonCallableStage(
        process_record=lambda x: _error_func(x),
        error_handling_method=ErrorHandlingMethod.SEND_TO_ERROR,
        name="SendToErrorStage",
    )
    return error_stage


@pytest.fixture(scope="function")
def stop_pipeline_stage() -> Stage:
    error_stage = PythonCallableStage(
        process_record=lambda x: _error_func(x),
        error_handling_method=ErrorHandlingMethod.STOP_PIPELINE,
        name="StopPipelineStage",
    )
    return error_stage


@pytest.fixture
def redis_cleanup() -> Generator[None, None, None]:
    yield
    redis_client = redis.Redis(host="", port=0)
    redis_client.flushall()
