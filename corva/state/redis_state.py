from corva.state.custom_redis import CustomRedis


class RedisState:
    def __init__(self, redis: CustomRedis):
        self.redis = redis

    def store(self, **kwargs):
        return self.redis.hset(**kwargs)

    def load(self, **kwargs):
        return self.redis.hget(**kwargs)

    def loadall(self, **kwargs):
        return self.redis.hgetall(**kwargs)

    def rm(self, **kwargs):
        return self.redis.hdel(**kwargs)

    def rmall(self, *names):
        return self.redis.delete(*names)

    def ttl(self, **kwargs):
        return self.redis.ttl(**kwargs)

    def pttl(self, **kwargs):
        return self.redis.pttl(**kwargs)

    def exists(self, *names):
        return self.redis.exists(*names)
