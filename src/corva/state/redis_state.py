from typing import Optional

from corva.state.redis_adapter import RedisAdapter


class RedisState:
    """An interface to save, load and do other operations with data in redis.

    As AWS Lambda is meant to be stateless,
    the apps need some mechanism to share the data between invokes.
    Redis provides an in-memory low latency storage for such data.
    This class provides and interface to save,
    load and do other operations with data in redis.
    """

    def __init__(self, redis: RedisAdapter):
        self.redis = redis

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


def get_cache_key(
    provider: str,
    asset_id: int,
    app_stream_id: int,
    app_key: str,
    app_connection_id: int,
) -> str:
    return (
        f'{provider}/well/{asset_id}/stream/{app_stream_id}/'
        f'{app_key}/{app_connection_id}'
    )


def get_cache(
    asset_id: int,
    app_stream_id: int,
    app_connection_id: int,
    provider: str,
    app_key: str,
    cache_url: str,
    cache_settings: Optional[dict] = None,
) -> RedisState:
    cache_settings = cache_settings or {}

    redis_adapter = RedisAdapter(
        default_name=get_cache_key(
            provider=provider,
            asset_id=asset_id,
            app_stream_id=app_stream_id,
            app_key=app_key,
            app_connection_id=app_connection_id,
        ),
        cache_url=cache_url,
        **cache_settings,
    )

    return RedisState(redis=redis_adapter)
