from corva.state.redis_adapter import RedisAdapter


class RedisState:
    """Provides interface to save, load and do other operations with state in redis cache

    As AWS Lambda is meant to be stateless, the apps need some mechanism to share the data between invokes.
    This class provides and interface save, load and do other operation with data in redis cache.
    """

    def __init__(self, redis: RedisAdapter):
        self.redis = redis

    @property
    def store(self):
        return self.redis.hset

    @property
    def load(self):
        return self.redis.hget

    @property
    def load_all(self):
        return self.redis.hgetall

    @property
    def delete(self):
        return self.redis.hdel

    @property
    def delete_all(self):
        return self.redis.delete

    @property
    def ttl(self):
        return self.redis.ttl

    @property
    def pttl(self):
        return self.redis.pttl

    @property
    def exists(self):
        return self.redis.exists
