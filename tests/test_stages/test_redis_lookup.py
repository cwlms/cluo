import pytest

from cluo.connections import RedisClient
from cluo.core import Record
from cluo.stages import RedisLookupStage
from cluo.stages.redis_lookup import LookupError


def test_redis_lookup_success():
    """Test that the RedisLookupStage successfully looks up a key in Redis."""
    redis_client = RedisClient()
    redis_client.set("test_key", "test_value")

    test_record = Record({"lookup": "key"})

    redis_lookup_stage = RedisLookupStage(
        key="lookup", redis_client=redis_client, on_zero_keys="raise", prefix="test_"
    )

    lookup = redis_lookup_stage.process_record(test_record)

    assert lookup.data == {"lookup": "test_value"}


def test_redis_lookup_on_zero_keys_raise():
    """Test that the RedisLookupStage raises an error when the key is not found in Redis."""
    redis_client = RedisClient()
    redis_client.set("test_key", "test_value")

    test_record = Record({"lookup": "key"})
    test_record.data["lookup"] = "foo"

    redis_lookup_stage = RedisLookupStage(
        key="lookup", redis_client=redis_client, on_zero_keys="raise", prefix="test_"
    )

    with pytest.raises(LookupError):
        redis_lookup_stage.process_record(test_record)


def test_redis_lookup_on_zero_keys_ignore():
    """Test that the RedisLookupStage ignores the key when it is not found in Redis."""
    redis_client = RedisClient()
    redis_client.set("test_key", "test_value")

    test_record = Record({"lookup": "key"})
    test_record.data["lookup"] = "foo"

    redis_lookup_stage = RedisLookupStage(
        key="lookup", redis_client=redis_client, on_zero_keys="ignore", prefix="test_"
    )

    lookup = redis_lookup_stage.process_record(test_record)

    assert lookup.data == {"lookup": None}


def test_redis_lookup_on_zero_keys_preserve():
    """Test that the RedisLookupStage preserves the key when it is not found in Redis."""
    redis_client = RedisClient()
    redis_client.set("test_key", "test_value")

    test_record = Record({"lookup": "key"})
    test_record.data["lookup"] = "foo"

    redis_lookup_stage = RedisLookupStage(
        key="lookup", redis_client=redis_client, on_zero_keys="preserve", prefix="test_"
    )

    lookup = redis_lookup_stage.process_record(test_record)

    assert lookup.data == {"lookup": "foo"}
