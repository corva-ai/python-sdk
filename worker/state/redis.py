import json
from typing import List, Optional

from redis import Redis, ConnectionError

from worker.settings import CACHE_URL
from worker.state.base import BaseState


class RedisState(BaseState):
    def __init__(self, cache_url: str = CACHE_URL, **kwargs):
        super().__init__(**kwargs)
        self.redis: Redis = self._connect(cache_url=cache_url)

    @staticmethod
    def _connect(cache_url: str) -> Redis:
        redis = Redis.from_url(cache_url)
        try:
            redis.ping()
        except ConnectionError as err:
            raise ValueError(f'Could not connect to Redis with URL: {cache_url}') from err
        return redis

    def load(self, state_key: str) -> dict:
        if self.redis.exists(state_key):
            return json.loads(self.redis.get(state_key))
        else:
            return {}

    def save(self, state: dict, state_key: str, px: Optional[int] = None) -> bool:
        return self.redis.set(state_key, json.dumps(state), px=px)

    def delete(self, state_keys: List[str]) -> int:
        return self.redis.delete(*state_keys)
