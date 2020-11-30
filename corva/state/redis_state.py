from logging import Logger, LoggerAdapter
from typing import Union

from corva.logger import DEFAULT_LOGGER
from corva.state.redis_adapter import RedisAdapter


class RedisState:
    """Provides interface to save, load and do other operations with state in redis cache

    As AWS Lambda is meant to be stateless, the apps need some mechanism to share the data between invokes.
    This class provides and interface save, load and do other operation with data in redis cache.
    """

    def __init__(self, redis: RedisAdapter, logger: Union[Logger, LoggerAdapter] = DEFAULT_LOGGER):
        self.redis = redis
        self.logger = logger

    def store(self, **kwargs):
        return self.redis.hset(**kwargs)

    def load(self, **kwargs):
        return self.redis.hget(**kwargs)

    def load_all(self, **kwargs):
        return self.redis.hgetall(**kwargs)

    def delete(self, **kwargs):
        return self.redis.hdel(**kwargs)

    def delete_all(self, *names):
        return self.redis.delete(*names)

    def ttl(self, **kwargs):
        return self.redis.ttl(**kwargs)

    def pttl(self, **kwargs):
        return self.redis.pttl(**kwargs)

    def exists(self, *names):
        return self.redis.exists(*names)
