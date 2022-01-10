import fakeredis
import pytest
from redis import Redis

from corva.configuration import SETTINGS
from corva.service.cache_sdk import UserRedisSdk
from corva.state.redis_adapter import RedisAdapter
from corva.testing import TestClient


@pytest.fixture(scope='function')
def redis_adapter() -> RedisAdapter:
    client = fakeredis.FakeRedis.from_url(url=SETTINGS.CACHE_URL, decode_responses=True)
    return RedisAdapter(hash_name='default_name', client=client)


@pytest.fixture(scope='function')
def redis(redis_adapter: RedisAdapter) -> UserRedisSdk:
    return UserRedisSdk(
        hash_name=redis_adapter.default_name,
        redis_dsn=SETTINGS.CACHE_URL,
        use_fakes=True,
    )


@pytest.fixture(scope='function', autouse=True)
def clean_redis():
    redis_client = Redis.from_url(url=SETTINGS.CACHE_URL)

    redis_client.flushall()

    yield

    redis_client.flushall()


@pytest.fixture(scope='function')
def context():
    return TestClient._context
