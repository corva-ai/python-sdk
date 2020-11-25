from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fakeredis import FakeRedis

from corva.app.base import BaseApp
from corva.event.base import BaseEvent
from corva.state.redis import RedisState


@pytest.fixture(scope='function', autouse=True)
def patch_redis():
    """Patches RedisState

    1. patches RedisState.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    3. patches default cache_url
    """

    redis_path = 'corva.state.redis'

    redis_state_patcher = patch(f'{redis_path}.RedisState.__bases__', (FakeRedis,))
    init_defaults = list(RedisState.__init__.__defaults__)
    init_defaults[0] = 'redis://localhost:6379'
    with redis_state_patcher, \
         patch(f'{redis_path}.from_url', side_effect=FakeRedis.from_url), \
         patch(f'{redis_path}.RedisState.__init__.__defaults__', tuple(init_defaults)):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_state_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis(patch_redis):
    return RedisState()


@pytest.fixture(scope='function')
def patch_base_event():
    with patch.object(BaseEvent, '__abstractmethods__', set()):
        yield


@pytest.fixture(scope='function')
def patch_base_app():
    # mock abstract methods of BaseApp, so we can initialize and test the class
    # mock event_cls attribute of BaseApp, so we can use fake load function
    with patch.object(BaseApp, '__abstractmethods__', set()), \
         patch.object(BaseApp, 'event_cls') as event_cls_mock:
        event_cls_mock.load.side_effect = lambda event: event
        yield SimpleNamespace(event_cls_mock=event_cls_mock)
