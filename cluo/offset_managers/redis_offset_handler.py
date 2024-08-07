from cluo.connections import RedisClient
from cluo.core.offset_manager import OffsetManager, OffsetType


class RedisOffsetManager(OffsetManager):
    """Manager for offset values stored in a Redis database."""

    def __init__(
        self,
        redis_client: RedisClient,
        offset_type: OffsetType | None,
    ) -> None:
        """Initialize the `RedisOffsetManager`.

        Args:
            redis_client (RedisClient): The Redis client to use.
            offset_type (PossibleOffsetType): Enum that represents the type of offset value. Defaults to "int".
        """
        self.redis_client = redis_client
        self._key: str | None = None
        self._validate()

        super().__init__(offset_type=offset_type)

    def _validate(self) -> None:
        try:
            self.redis_client.get("test")
        except Exception as e:
            raise Exception(f"Error validating Redis client: {e}")

    def _get(self) -> str | None:
        """Get the current offset value.

        Returns:
            str: The current offset value.
        """
        resp = self.redis_client.get(self.key)
        if resp is None:
            return None

        try:
            decoded_response = resp.decode("UTF-8")
        except ValueError as e:
            self._log.error(
                f"Got invalid offset response from Redis for key {self.key}: {resp}"
            )
            raise e
        return decoded_response

    def _set(self, value: str) -> bool:
        """Set a new offset

        Args:
            value (str): Value to set to.

        Raises:
            Exception: If error is returned from Redis.

        Returns:
            bool: Whether set was successful.
        """
        resp = self.redis_client.set(self.key, value)
        if resp is None:
            raise Exception(
                f"Got invalid response `None` from `redis.Redis.set()` for key '{self.key}'"
            )
        return resp


class RedisKeyError(Exception):
    """Exception raised when key is not found in Redis."""

    pass
