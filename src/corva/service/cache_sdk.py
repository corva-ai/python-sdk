import datetime
import warnings
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

import fakeredis
import redis

from corva import cache_adapter


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

    def __init__(self, hash_name: str, redis_dsn: str, use_fakes: bool = False):
        use_lua_52 = False
        if use_fakes:
            client = fakeredis.FakeRedis.from_url(url=redis_dsn, decode_responses=True)
            use_lua_52 = True
        else:
            client = redis.Redis.from_url(url=redis_dsn, decode_responses=True)

        self.cache_repo = cache_adapter.RedisRepository(
            hash_name=hash_name, client=client, use_lua_52=use_lua_52
        )
        self.old_cache_repo = cache_adapter.DeprecatedRedisAdapter(
            hash_name=hash_name, client=client
        )

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

    @overload
    def delete(self, *, key: str) -> None:
        ...

    @overload
    def delete(self, keys: List[str], name: Optional[str] = ...) -> int:
        ...

    def delete(
        self,
        keys=None,
        name=None,
        *,
        key=None,
    ):
        if keys is not None:
            warnings.warn(
                "The `keys` and `name` kwargs of `delete` cache method are deprecated "
                "and will be removed from corva in the next major version. "
                "Use `key` kwarg instead.",
                FutureWarning,
            )
            return self.old_cache_repo.hdel(keys=keys, name=name)

        if key is not None:
            self.cache_repo.delete(key=key)
            return None

        raise TypeError("Bad arguments")

    def delete_many(self, keys: Sequence[str]) -> None:
        self.cache_repo.delete_many(keys=keys)

    @overload
    def delete_all(self) -> None:
        ...

    @overload
    def delete_all(self, *names: str):
        ...

    def delete_all(self, *names):
        if names:
            warnings.warn(
                "`delete_all` with `*names` parameter cache method is deprecated and"
                " will be removed from corva in the next major version. Use "
                "`delete_many` cache method instead.",
                FutureWarning,
            )
            return self.old_cache_repo.delete(*names)
        else:
            self.cache_repo.delete_all()

    def store(self, **kwargs):
        warnings.warn(
            "`store` cache method is deprecated and will be removed from corva in the"
            " next major version. Use `set` or `set_many` cache methods instead.",
            FutureWarning,
        )
        return self.old_cache_repo.hset(**kwargs)

    def load(self, **kwargs):
        warnings.warn(
            "`load` cache method is deprecated and will be removed from corva in the"
            " next major version. Use `get` or `get_many` cache methods instead.",
            FutureWarning,
        )
        return self.old_cache_repo.hget(**kwargs)

    def load_all(self, **kwargs):
        warnings.warn(
            "`load_all` cache method is deprecated and will be removed from corva in "
            "the next major version. Use `get_all` cache method instead.",
            FutureWarning,
        )
        return self.old_cache_repo.hgetall(**kwargs)

    def ttl(self, **kwargs):
        warnings.warn(
            "`ttl` cache method is deprecated and will be removed from corva in the"
            " next major version.",
            FutureWarning,
        )
        return self.old_cache_repo.ttl(**kwargs)

    def pttl(self, **kwargs):
        warnings.warn(
            "`pttl` cache method is deprecated and will be removed from corva in the"
            " next major version.",
            FutureWarning,
        )
        return self.old_cache_repo.pttl(**kwargs)

    def exists(self, *names):
        warnings.warn(
            "`exists` cache method is deprecated and will be removed from corva in the"
            " next major version.",
            FutureWarning,
        )
        return self.old_cache_repo.exists(*names)


class InternalCacheSdkProtocol(Protocol):
    def vacuum(self, delete_count: int) -> None:
        ...


class InternalRedisSdk:
    def __init__(self, hash_name: str, redis_dsn: str):
        self.cache_repo = cache_adapter.RedisRepository(
            hash_name=hash_name,
            client=redis.Redis.from_url(url=redis_dsn, decode_responses=True),
        )

    def vacuum(self, delete_count: int) -> None:
        self.cache_repo.vacuum(delete_count=delete_count)


class FakeInternalCacheSdk:
    def __init__(self):
        self.vacuum_called = False

    def vacuum(self, delete_count: int) -> None:
        self.vacuum_called = True
