from datetime import timedelta
from typing import Dict, List, Optional, Union

from redis import ConnectionError, Redis, from_url

REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]


class RedisAdapter(Redis):
    """Expands basic redis functionality

    Serves the purpose of adding custom logic on top of basic redis functions
    (e.g. adding expiry to hset, which is not available out of the box).
    """

    DEFAULT_EXPIRY: timedelta = timedelta(days=60)

    def __init__(
        self,
        default_name: str,
        cache_url: str,
        **kwargs,
    ):
        kwargs.setdefault('decode_responses', True)
        super().__init__(
            connection_pool=from_url(url=cache_url, **kwargs).connection_pool
        )
        self.default_name = default_name
        try:
            self.ping()
        except ConnectionError as exc:
            raise ConnectionError(
                f'Could not connect to Redis with URL: {cache_url}'
            ) from exc

    def hset(
        self,
        name: Optional[str] = None,
        key: Optional[str] = None,
        value: Optional[REDIS_STORED_VALUE_TYPE] = None,
        mapping: Optional[Dict[str, REDIS_STORED_VALUE_TYPE]] = None,
        expiry: Union[int, timedelta, None] = DEFAULT_EXPIRY,
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

        n_set = super().hset(name=name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl(name=name) > 0:
            self.persist(name=name)

        if expiry is not None:
            self.expire(name=name, time=expiry)

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
        return super().hget(name=name, key=key)

    def hgetall(
        self, name: Optional[str] = None
    ) -> Dict[str, Union[REDIS_STORED_VALUE_TYPE]]:
        """Loads all data from cache"""

        name = name or self.default_name
        return super().hgetall(name=name)

    def hdel(self, keys: List[str], name: Optional[str] = None) -> int:
        """Deletes some data from cache

        params:
         keys: list of keys to delete
        returns: number of deleted elements
        """

        name = name or self.default_name
        return super().hdel(name, *keys)

    def delete(self, *names: List[str]) -> int:
        """Deletes all data from cache"""

        names = names or [self.default_name]
        return super().delete(*names)

    def ttl(self, name: Optional[str] = None) -> int:
        """Returns the number of seconds until expiration"""

        name = name or self.default_name
        return super().ttl(name=name)

    def pttl(self, name: Optional[str] = None) -> int:
        """Returns the number of milliseconds until expiration"""

        name = name or self.default_name
        return super().pttl(name=name)

    def exists(self, *names: List[str]) -> int:
        """Returns whether there is data in cache"""

        names = names or [self.default_name]
        return super().exists(*names)
