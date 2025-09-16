from datetime import timedelta
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    overload,
)

import redis


class CacheRepositoryProtocol(Protocol):
    def set(
        self,
        key: str,
        value: str,
        ttl: int,
    ) -> None:
        ...

    def set_many(self, data: Sequence[Tuple[str, str, int]]) -> None:
        ...

    def get(self, key: str) -> Optional[str]:
        ...

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        ...

    def get_all(self) -> Dict[str, str]:
        ...

    def delete(self, key: str) -> None:
        ...

    def delete_many(self, keys: Sequence[str]) -> None:
        ...

    def delete_all(self) -> None:
        ...


class RedisRepository:

    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.client = client

    def set(self, key: str, value: str, ttl: int) -> None:
        self.set_many(data=[(key, value, ttl)])

    def set_many(self, data: Sequence[Tuple[str, str, int]]) -> None:
        pipe = self.client.pipeline()
        for key, value, ttl in data:
            pipe.hset(self.hash_name, key, value)
            pipe.execute_command("HEXPIRE", self.hash_name, ttl, "FIELDS", 1, key)
        pipe.execute()

    def get(self, key: str) -> Optional[str]:
        val = self.client.hget(self.hash_name, key)
        return None if val is None else str(val)

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        if keys:
            values = self.client.hmget(self.hash_name, keys)
            # redis-py returns a list of values where non-existent/expired are None
            return {k: (None if v is None else str(v)) for k, v in zip(keys, values)}
        return {}

    def get_all(self) -> Dict[str, str]:
        raw = self.client.hgetall(self.hash_name)
        return dict(raw)

    def delete(self, key: str) -> None:
        self.client.hdel(self.hash_name, key)

    def delete_many(self, keys: Sequence[str]) -> None:
        if keys:
            self.client.hdel(self.hash_name, *keys)

    def delete_all(self) -> None:
        self.client.delete(self.hash_name)


class DeprecatedRedisAdapter:
    """Expands basic redis functionality

    Serves the purpose of adding custom logic on top of basic redis functions
    (e.g. adding expiry to hset, which is not available out of the box).
    """

    DEFAULT_EXPIRY: timedelta = timedelta(days=60)
    REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]

    def __init__(
        self,
        hash_name: str,
        client: redis.Redis,
    ):
        self.default_name = hash_name
        self.client = client

        self.client.ping()

    @overload
    def hset(
        self,
        name: Optional[str] = ...,
        key: str = ...,
        value: REDIS_STORED_VALUE_TYPE = ...,
        mapping: None = ...,
        expiry: Union[int, timedelta, None] = ...,
    ) -> int:
        ...

    @overload
    def hset(
        self,
        name: Optional[str] = ...,
        key: None = ...,
        value: None = ...,
        mapping: Dict[str, REDIS_STORED_VALUE_TYPE] = ...,
        expiry: Union[int, timedelta, None] = ...,
    ) -> int:
        ...

    def hset(
        self,
        name=None,
        key=None,
        value=None,
        mapping=None,
        expiry=DEFAULT_EXPIRY,
    ) -> int:
        """Stores the data in cache

        params:
         key: key, which will contain the data
         value: data to be saved
         mapping: dict of key:data pairs to be saved
         expiry: time in seconds for when data will be deleted from cache
            expiration is reset with every hset call
            expiration can be disabled by setting expiry to None
        returns: number of inserted elements
        """

        name = name or self.default_name

        n_set = self.client.hset(name=name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl(name=name) > 0:
            self.client.persist(name=name)

        if expiry is not None:
            self.client.expire(name=name, time=expiry)

        return n_set

    def hget(
        self, key: str, name: Optional[str] = None
    ) -> Union[REDIS_STORED_VALUE_TYPE, None]:
        """Loads data from cache

        params:
         key: key to load data from
        returns: stored data
        """

        name = name or self.default_name
        return self.client.hget(name=name, key=key)

    def hgetall(
        self, name: Optional[str] = None
    ) -> Dict[str, Union[REDIS_STORED_VALUE_TYPE]]:
        """Loads all data from cache"""

        name = name or self.default_name
        return self.client.hgetall(name=name)

    def hdel(self, keys: List[str], name: Optional[str] = None) -> int:
        """Deletes some data from cache

        params:
         keys: list of keys to delete
        returns: number of deleted elements
        """

        name = name or self.default_name
        return self.client.hdel(name, *keys)

    def delete(self, *names: List[str]) -> int:
        """Deletes all data from cache"""

        delete_names = names or [self.default_name]
        return self.client.delete(*delete_names)

    def ttl(self, name: Optional[str] = None) -> int:
        """Returns the number of seconds until expiration"""

        name = name or self.default_name
        return self.client.ttl(name=name)

    def pttl(self, name: Optional[str] = None) -> int:
        """Returns the number of milliseconds until expiration"""

        name = name or self.default_name
        return self.client.pttl(name=name)

    def exists(self, *names: List[str]) -> int:
        """Returns whether there is data in cache"""

        exists_names = names or [self.default_name]
        return self.client.exists(*exists_names)
