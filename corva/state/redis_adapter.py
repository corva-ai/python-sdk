from datetime import timedelta
from typing import Optional, List, Dict, Union

from redis import Redis, from_url, ConnectionError

REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]


class RedisAdapter(Redis):
    DEFAULT_EXPIRY: timedelta = timedelta(days=60)

    def __init__(
         self,
         name: str,
         cache_url: str,
         **kwargs
    ):
        kwargs.setdefault('decode_responses', True)
        super().__init__(connection_pool=from_url(url=cache_url, **kwargs).connection_pool)
        self.name = name
        try:
            self.ping()
        except ConnectionError as exc:
            raise ConnectionError(f'Could not connect to Redis with URL: {cache_url}') from exc

    def hset(
         self,
         key: Optional[str] = None,
         value: Optional[REDIS_STORED_VALUE_TYPE] = None,
         mapping: Optional[Dict[str, REDIS_STORED_VALUE_TYPE]] = None,
         expiry: Union[int, timedelta, None] = DEFAULT_EXPIRY
    ) -> int:
        n_set = super().hset(name=self.name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl() > 0:
            self.persist(name=self.name)

        if expiry is not None:
            self.expire(name=self.name, time=expiry)

        return n_set

    def hget(self, key: str) -> Union[REDIS_STORED_VALUE_TYPE, None]:
        return super().hget(name=self.name, key=key)

    def hgetall(self) -> Dict[str, Union[REDIS_STORED_VALUE_TYPE]]:
        return super().hgetall(name=self.name)

    def hdel(self, keys: List[str]) -> int:
        return super().hdel(self.name, *keys)

    def delete(self) -> int:
        return super().delete(self.name)

    def ttl(self) -> int:
        return super().ttl(name=self.name)

    def pttl(self) -> int:
        return super().pttl(name=self.name)

    def exists(self) -> int:
        return super().exists(self.name)
