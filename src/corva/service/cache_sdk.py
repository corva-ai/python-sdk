import datetime
from typing import (
    Dict,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
)

import fakeredis
import redis

from corva import cache_adapter
from corva.cache_adapter import HashMigrator


class UserCacheSdkProtocol(Protocol):
    def set(self, key: str, value: str, ttl: int = ...) -> None:
        ...

    def set_many(
        self, data: Sequence[Union[Tuple[str, str], Tuple[str, str, int]]]
    ) -> None:
        ...

    def get(self, key: str) -> Optional[str]:
        ...

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        ...

    def get_all(self) -> Dict[str, str]:
        ...

    # TODO: remove asterisk in v2 - it was added for backward compatibility
    def delete(self, *, key: str) -> None:
        ...

    def delete_many(self, keys: Sequence[str]) -> None:
        ...

    def delete_all(self) -> None:
        ...


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

        self.cache_repo = cache_adapter.RedisRepository(
            hash_name=hash_name,
            client=cast(redis.Redis, redis_client),
        )

        try:
            migrator = HashMigrator(hash_name, redis_client)
            migrator.run()
        except Exception:
            pass

    def set(self, key: str, value: str, ttl: int = SIXTY_DAYS) -> None:
        self.cache_repo.set(key=key, value=value, ttl=ttl)

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

    def get(self, key: str) -> Optional[str]:
        return self.cache_repo.get(key=key)

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        return self.cache_repo.get_many(keys=keys)

    def get_all(self) -> Dict[str, str]:
        return self.cache_repo.get_all()

    def delete(self, *, key: str) -> None:
        self.cache_repo.delete(key=key)

    def delete_many(self, keys: Sequence[str]) -> None:
        self.cache_repo.delete_many(keys=keys)

    def delete_all(self) -> None:
        self.cache_repo.delete_all()






