import redis

from cluo.connections import RedisClient


def test_redis_client():
    """Test that the RedisClient class creates a connection to Redis."""
    redis_client = RedisClient()
    assert redis_client.client is not None
    assert isinstance(
        redis_client.client, (redis.client.Redis, redis.cluster.RedisCluster)
    )
