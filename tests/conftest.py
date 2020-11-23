from types import SimpleNamespace
from unittest.mock import patch

import fakeredis
import pytest

from corva.event.base import BaseEvent
from corva.state.redis import RedisState


@pytest.fixture(scope='function', autouse=True)
def mock_redis():
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeRedis(server=server)
    with patch('redis.client.Redis.from_url', return_value=redis):
        yield SimpleNamespace(server=server, redis=redis)


@pytest.fixture(scope='function')
def redis(mock_redis):
    return RedisState()


@pytest.fixture(scope='function')
def patch_base_event():
    with patch.object(BaseEvent, '__abstractmethods__', set()):
        yield
