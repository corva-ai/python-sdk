from unittest.mock import patch

import pytest

from worker.event.base import BaseEvent
from worker.state.redis import RedisState


@pytest.fixture(scope='function')
def redis():
    return RedisState()


@pytest.fixture(scope='function', autouse=True)
def flush_redis(redis):
    redis.redis.flushall()
    yield
    redis.redis.flushall()


@pytest.fixture(scope='function')
def patch_base_event():
    with patch.object(BaseEvent, '__abstractmethods__', set()):
        yield
