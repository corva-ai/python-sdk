import pytest
from redis import Redis

from corva.configuration import SETTINGS
from corva.testing import TestClient


@pytest.fixture(scope="function", autouse=True)
def clean_redis():
    redis_client = Redis.from_url(url=SETTINGS.CACHE_URL)

    redis_client.flushall()

    yield

    redis_client.flushall()


@pytest.fixture(scope="function")
def context():
    return TestClient._context
