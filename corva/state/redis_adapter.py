from datetime import timedelta
from logging import Logger, LoggerAdapter
from typing import Optional, List, Dict, Union

from redis import Redis, from_url, ConnectionError

from corva import settings
from corva.types import REDIS_STORED_VALUE_TYPE
from corva.logger import DEFAULT_LOGGER


class RedisAdapter(Redis):
    DEFAULT_EXPIRY: timedelta = timedelta(days=60)

    def __init__(
         self,
         default_name: str,
         cache_url: str = settings.CACHE_URL,
         logger: Union[Logger, LoggerAdapter] = DEFAULT_LOGGER,
         **kwargs
    ):
        kwargs.setdefault('decode_responses', True)
        super().__init__(connection_pool=from_url(url=cache_url, **kwargs).connection_pool)
        self.logger = logger
        self.default_name = default_name
        try:
            self.ping()
        except ConnectionError as exc:
            raise ConnectionError(f'Could not connect to Redis with URL: {cache_url}') from exc

    def hset(
         self,
         name: Optional[str] = None,
         key: Optional[str] = None,
         value: Optional[REDIS_STORED_VALUE_TYPE] = None,
         mapping: Optional[Dict[str, REDIS_STORED_VALUE_TYPE]] = None,
         expiry: Union[int, timedelta, None] = DEFAULT_EXPIRY
    ) -> int:
        name = name or self.default_name

        n_set = super().hset(name=name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl(name=name) > 0:
            self.persist(name=name)

        if expiry is not None:
            self.expire(name=name, time=expiry)

        return n_set

    def hget(self, key: str, name: Optional[str] = None) -> Union[REDIS_STORED_VALUE_TYPE, None]:
        name = name or self.default_name
        return super().hget(name=name, key=key)

    def hgetall(self, name: Optional[str] = None) -> Dict[str, Union[REDIS_STORED_VALUE_TYPE, None]]:
        name = name or self.default_name
        return super().hgetall(name=name)

    def hdel(self, keys: List[str], name: Optional[str] = None) -> int:
        name = name or self.default_name
        return super().hdel(name, *keys)

    def delete(self, *names: List[str]) -> int:
        names = names or [self.default_name]
        return super().delete(*names)

    def ttl(self, name: Optional[str] = None) -> int:
        name = name or self.default_name
        return super().ttl(name=name)

    def pttl(self, name: Optional[str] = None) -> int:
        name = name or self.default_name
        return super().pttl(name=name)

    def exists(self, *names: List[str]) -> int:
        names = names or [self.default_name]
        return super().exists(*names)
