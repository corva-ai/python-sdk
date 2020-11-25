from datetime import timedelta
from typing import Dict, Optional, Union

from redis import Redis, ConnectionError, from_url

from corva.settings import CACHE_URL


class RedisState(Redis):
    DEFAULT_EXPIRY: timedelta = timedelta(days=60)

    def __init__(self, cache_url: str = CACHE_URL, **kwargs):
        super().__init__(connection_pool=from_url(url=cache_url, **kwargs).connection_pool)
        try:
            self.ping()
        except ConnectionError as exc:
            raise ConnectionError(f'Could not connect to Redis with URL: {cache_url}') from exc

    def hset(
         self,
         name: str,
         key: Optional[str] = None,
         value: Optional[Union[bytes, str, int, float]] = None,
         mapping: Optional[Dict[str, Union[bytes, str, int, float]]] = None,
         expiry: Union[int, timedelta, None] = DEFAULT_EXPIRY
    ) -> int:
        n_set = super().hset(name=name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl(name=name) > 0:
            self.persist(name=name)

        if expiry is not None:
            self.expire(name=name, time=expiry)

        return n_set
