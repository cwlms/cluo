from typing import Any, Optional, Union

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry


class RedisClient:
    """Create a Redis client.

    Attributes:
        host: The host of the Redis server.
        port: The port of the Redis server.

    Methods:
        get: Get a value from Redis.
        set: Set a value in Redis.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
    ) -> None:
        self.host = host
        self.port = port
        self.client = self._get_client()

    def _get_client(self) -> Union[redis.client.Redis, redis.cluster.RedisCluster]:
        """Get a Redis client.

        Note:
            If the Redis server is a cluster, a RedisCluster client will be returned, otherwise a Redis client will be returned."""
        try:
            return redis.cluster.RedisCluster(
                host=self.host, port=self.port, retry=Retry(ExponentialBackoff(), 6)
            )
        except redis.exceptions.RedisClusterException:
            connection_pool = redis.connection.ConnectionPool(
                host=self.host, port=self.port
            )
            return redis.client.Redis(
                connection_pool=connection_pool, retry=Retry(ExponentialBackoff(), 6)
            )

    def get(self, key: str) -> Optional[Any]:
        """Get a value from Redis.

        Args:
            key (str): The key to get the value for.

        Returns:
            The value for the key (if it exists, otherwise None)."""
        return self.client.get(key)

    def set(self, key: str, value: str) -> bool | None:
        """Set a value in Redis.

        Args:
            key (str): The key to set the value for.
            value (str): The value to set for the key.

        Returns:
            None

        Note:
            This method is only used for testing at this time.

        Todo:
            Allow other value types.
        """
        result = self.client.set(key, value)
        return result
