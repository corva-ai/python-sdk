import pytest
from redis import Redis

from corva.configuration import SETTINGS
from corva.testing import TestClient
from corva.validate_app_init import read_manifest


@pytest.fixture(scope='function', autouse=True)
def clean_redis():
    redis_client = Redis.from_url(url=SETTINGS.CACHE_URL)

    redis_client.flushall()

    yield

    redis_client.flushall()


@pytest.fixture(scope='function', autouse=True)
def clean_read_manifest_lru_cache():
    read_manifest.cache_clear()
    yield
    read_manifest.cache_clear()


@pytest.fixture(scope='function')
def context():
    return TestClient._context
