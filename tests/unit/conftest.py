import pytest

from corva.configuration import SETTINGS
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState
from corva.testing import TestClient


@pytest.fixture(scope='function')
def redis_adapter():
    return RedisAdapter(default_name='default_name', cache_url=SETTINGS.CACHE_URL)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)


@pytest.fixture(scope='function')
def context():
    return TestClient._context
