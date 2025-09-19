import datetime
from functools import wraps
from typing import Callable, Dict, Optional, Protocol, Sequence, Tuple, Union, cast

import fakeredis
import redis

from corva import cache_adapter


class UserCacheSdkProtocol(Protocol):
    def set(self, key: str, value: str, ttl: int = ...) -> None: ...

    def set_many(
        self, data: Sequence[Union[Tuple[str, str], Tuple[str, str, int]]]
    ) -> None: ...

    def get(self, key: str) -> Optional[str]: ...

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]: ...

    def get_all(self) -> Dict[str, str]: ...

    # TODO: remove asterisk in v2 - it was added for backward compatibility
    def delete(self, *, key: str) -> None: ...

    def delete_many(self, keys: Sequence[str]) -> None: ...

    def delete_all(self) -> None: ...


def ensure_migrated_once(method: Callable) -> Callable:
    """
    Decorator that ensures a specific migration process
     has been executed before invoking the decorated method. Once the
     migration is attempted, the decorator marks it as complete,
     regardless of the outcome, to optimize subsequent calls.

    Args:
        method (Callable): The `UserRedisSdk.method` to be decorated.

    Returns:
        Callable: The wrapped method which ensures that the migration process
        has been attempted before execution.
    """

    @wraps(method)
    def wrapper(self: 'UserRedisSdk', *args, **kwargs):

        if not self._migrated:
            migrator = cache_adapter.HashMigrator(
                hash_name=self._original_hash_name,
                client=self._redis_client,
            )
            try:
                migrator.run()
            finally:
                # Regardless of outcome (True/False), mark as attempted to avoid
                # repeating the check on every call. Subsequent calls operate on
                # the new-hash namespace.
                self._migrated = True

        return method(self, *args, **kwargs)

    return wrapper


class UserRedisSdk:
    """User cache protocol implementation using Redis.

    As AWS Lambda is meant to be stateless, the apps need some mechanism to share the
    data between invokes. Redis provides an in-memory low latency storage for such data.
    """

    SIXTY_DAYS: int = int(datetime.timedelta(days=60).total_seconds())

    def __init__(
        self,
        hash_name: str,
        redis_dsn: str,
        use_fakes: bool = False,
        redis_client: Optional[redis.Redis] = None,
    ):
        # use either provided redis client, or initialize "fake" client
        # (usually used for tests), or initialize real new client
        if use_fakes:
            redis_client = fakeredis.FakeRedis.from_url(
                url=redis_dsn, decode_responses=True
            )
        elif redis_client is None:
            redis_client = redis.Redis.from_url(url=redis_dsn, decode_responses=True)

        # Lazy migration: do not run on init; defer until first read/write/delete
        self._original_hash_name = hash_name
        self._redis_client = cast(redis.Redis, redis_client)
        self._migrated = False

        self.cache_repo = cache_adapter.RedisRepository(
            hash_name=cache_adapter.HashMigrator.NEW_HASH_PREFIX + hash_name,
            client=self._redis_client,
        )

    @ensure_migrated_once
    def set(self, key: str, value: str, ttl: int = SIXTY_DAYS) -> None:
        self.cache_repo.set(key=key, value=value, ttl=ttl)

    @ensure_migrated_once
    def set_many(
        self, data: Sequence[Union[Tuple[str, str], Tuple[str, str, int]]]
    ) -> None:
        prepared_data = [
            cast(Tuple[str, str, int], datum)
            if len(datum) == 3
            else (datum[0], datum[1], self.SIXTY_DAYS)
            for datum in data
        ]
        self.cache_repo.set_many(data=prepared_data)

    @ensure_migrated_once
    def get(self, key: str) -> Optional[str]:
        return self.cache_repo.get(key=key)

    @ensure_migrated_once
    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        return self.cache_repo.get_many(keys=keys)

    @ensure_migrated_once
    def get_all(self) -> Dict[str, str]:
        return self.cache_repo.get_all()

    @ensure_migrated_once
    def delete(self, *, key: str) -> None:
        self.cache_repo.delete(key=key)

    @ensure_migrated_once
    def delete_many(self, keys: Sequence[str]) -> None:
        self.cache_repo.delete_many(keys=keys)

    @ensure_migrated_once
    def delete_all(self) -> None:
        self.cache_repo.delete_all()
