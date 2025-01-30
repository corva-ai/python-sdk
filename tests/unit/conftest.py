from typing import cast

import pytest
from fakeredis import FakeRedis
from redis import Redis

from corva.configuration import SETTINGS
from corva.testing import TestClient


@pytest.fixture(scope='function', autouse=True)
def clean_real_redis():
    redis_client = Redis.from_url(url=SETTINGS.CACHE_URL)

    redis_client.flushall()

    yield

    redis_client.flushall()


@pytest.fixture(scope='function', autouse=True)
def clean_fake_redis():
    redis_client = cast(FakeRedis, FakeRedis.from_url(url=SETTINGS.CACHE_URL))

    redis_client.flushall()

    yield

    redis_client.flushall()


@pytest.fixture(scope='function')
def context():
    return TestClient._context
