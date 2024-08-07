import pytest

from cluo.connections import RedisClient
from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import RedisOffsetManager

redis_client = RedisClient()


def test_invalid_redis_client():
    invalid_redis_client = RedisClient(host="", port=0)

    with pytest.raises(Exception):
        RedisOffsetManager(invalid_redis_client, offset_type=OffsetType.INT)


def test_redis_offset_manager_get(redis_cleanup):
    redis_offset_manager = RedisOffsetManager(redis_client, offset_type=OffsetType.INT)
    redis_offset_manager.set_key("test_redis_offset_manager_get")
    assert redis_offset_manager.get() is None


def test_redis_offset_manager_set_get(redis_cleanup):
    redis_offset_manager = RedisOffsetManager(redis_client, offset_type=OffsetType.INT)
    redis_offset_manager.set_key("test_redis_offset_manager_set_get")
    redis_offset_manager.set(1)
    assert redis_offset_manager.get() == 1


def test_redis_offset_manager_set_reset(redis_cleanup):
    redis_offset_manager = RedisOffsetManager(redis_client, offset_type=OffsetType.INT)
    redis_offset_manager.set_key("test_redis_offset_manager_set_reset")
    redis_offset_manager.set(1)
    redis_offset_manager.set(15)
    assert redis_offset_manager.get() == 15
