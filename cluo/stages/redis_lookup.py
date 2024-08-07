from typing import Literal, Union

import redis

from cluo.core import ErrorHandlingMethod, Record, RecordData, Stage


class RedisLookupStage(Stage):
    """Perform a Redis lookup on a record.

    Attributes:
        key: The key to use for the lookup.
        redis_client: The Redis client to use for the lookup.
        on_zero_keys: What to do if the key is not found in Redis. Options are "raise", "ignore", and "preserve".
        prefix: A prefix to add to the key before performing the lookup.
        suffix: A suffix to add to the key before performing the lookup.
        name: The name of the stage.
        processes: The number of processes to use for the stage.
        error_handling_method: The error handling method to use for the stage.
        expose_metrics: Whether or not to expose metrics for this stage.

    Methods:
        process_record: Perform the Redis lookup and update the record data with the result.
    """

    def __init__(
        self,
        key: str,
        redis_client: Union[redis.client.Redis, redis.cluster.RedisCluster],
        on_zero_keys: Literal["raise", "ignore", "preserve"] = "ignore",
        prefix: str = "",
        suffix: str = "",
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the RedisLookupStage.

        Args:
            key (str): The key to use for the lookup.
            redis_client (Union[redis.client.Redis, redis.cluster.RedisCluster]): The Redis client to use for the lookup.
            on_zero_keys (Literal["raise", "ignore", "preserve"], optional): What to do if the key is not found in Redis.
            prefix (str, optional): A prefix to add to the key before performing the lookup.
            suffix (str, optional): A suffix to add to the key before performing the lookup.
            name (str, optional): The name of the stage.
            processes (int, optional): The number of processes to use for the stage.
            error_handling_method (ErrorHandlingMethod, optional): The error handling method to use for the stage.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.key = key
        self.redis_client = redis_client
        self.on_zero_keys = on_zero_keys
        self.prefix = prefix
        self.suffix = suffix

    def _validate_key(self, record_data: RecordData) -> None:
        """Validate that the key exists in the record data."""
        if not self.key:
            raise ValueError("Key must be provided for RedisLookupStage.")
        if self.key not in record_data:
            raise ValueError(f"Key {self.key} not found in record data.")

    def _create_redis_key(self, record_data: RecordData) -> str:
        """Create the Redis key to use for lookup from the value of the key in the record data."""
        self._validate_key(record_data)
        return f"{self.prefix}{record_data[self.key]}{self.suffix}"

    def _process_lookup_result(
        self, result, redis_key: str, record_data: RecordData
    ) -> str | None:
        """Process the result of the redis lookup and decode it."""
        if not result:
            if self.on_zero_keys == "raise":
                raise LookupError(f"Key {redis_key} not found in Redis.")
            elif self.on_zero_keys == "ignore":
                return None
            elif self.on_zero_keys == "preserve":
                return record_data.get(self.key)

        return result.decode("utf-8")

    def _do_lookup(
        self,
        connection: Union[redis.client.Redis, redis.cluster.RedisCluster],
        redis_key: str,
        record_data: RecordData,
    ) -> str | None:
        """Perform the redis lookup."""
        lookup = connection.get(redis_key)
        return self._process_lookup_result(lookup, redis_key, record_data)

    def process_record(self, record: Record) -> Record:
        """Perform the redis lookup and update the record data with the result."""
        redis_key = self._create_redis_key(record.data)
        redis_lookup = self._do_lookup(self.redis_client, redis_key, record.data)
        record.data.update({self.key: redis_lookup})
        return record


class LookupError(Exception):
    """Raised when a lookup fails."""

    pass
