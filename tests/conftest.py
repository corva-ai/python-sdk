from typing import Iterable, cast

import pytest
from fakeredis import FakeRedis
from redis import Redis

from corva import cache_adapter
from corva.configuration import SETTINGS
from corva.testing import TestClient
from corva.validate_app_init import read_manifest

from .utils.patch_fakeredis import info  # noqa


@pytest.fixture(scope="function")
def redis_adapter(redis_client: Redis) -> Iterable[cache_adapter.RedisRepository]:
    redis_adapter = cache_adapter.RedisRepository(
        hash_name="test_hash_name", client=redis_client
    )
    yield redis_adapter


@pytest.fixture(scope="function")
def redis_client():
    redis_client = Redis.from_url(url=SETTINGS.CACHE_URL, decode_responses=True)

    redis_client.flushall()

    yield redis_client

    redis_client.flushall()


@pytest.fixture(scope="function", autouse=True)
def clean_redis_clients():
    redis_clients = (
        Redis.from_url(url=SETTINGS.CACHE_URL),
        cast(FakeRedis, FakeRedis.from_url(url=SETTINGS.CACHE_URL)),
    )
    [client.flushall() for client in redis_clients]
    yield

    [client.flushall() for client in redis_clients]


@pytest.fixture(scope="function", autouse=True)
def clean_read_manifest_lru_cache():
    read_manifest.cache_clear()
    yield
    read_manifest.cache_clear()


@pytest.fixture(scope="function")
def context():
    return TestClient._context
