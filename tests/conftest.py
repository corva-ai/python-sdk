from types import SimpleNamespace
from unittest.mock import patch

import fakeredis
import pytest

from worker.state.redis import RedisState


@pytest.fixture(scope='function')
def mock_redis():
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeRedis(server=server)
    with patch('redis.client.Redis.from_url', return_value=redis):
        yield SimpleNamespace(server=server, redis=redis)


@pytest.fixture(scope='function')
def redis(mock_redis):
    return RedisState()
