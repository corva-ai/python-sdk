from unittest.mock import patch

import pytest
from fakeredis import FakeRedis

from corva.app.base import BaseApp
from corva.app.scheduled import ScheduledApp
from corva.app.stream import StreamApp
from corva.constants import STREAM_EVENT_TYPE
from corva.event.base import BaseEvent
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState

APP_KEY = 'provider.app-name'
SCHEDULED_EVENT_FILE_PATH = 'data/tests/scheduled_event.json'
STREAM_EVENT_FILE_PATH = 'data/tests/stream_event.json'
CACHE_URL = 'redis://localhost:6379'


@pytest.fixture(scope='function', autouse=True)
def patch_redis_adapter():
    """Patches RedisAdapter

    1. patches RedisAdapter.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = patch(f'{redis_adapter_path}.RedisAdapter.__bases__', (FakeRedis,))

    with redis_adapter_patcher, \
         patch(f'{redis_adapter_path}.from_url', side_effect=FakeRedis.from_url):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis_adapter(patch_redis_adapter):
    return RedisAdapter(default_name='default_name', cache_url=CACHE_URL)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)


@pytest.fixture(scope='function')
def patch_base_event():
    """Patches BaseEvent

    1. patches __abstractmethods__, so we can initialize BaseEvent
    2. patches load function
    """

    with patch.object(BaseEvent, '__abstractmethods__', set()), \
         patch.object(BaseEvent, 'load', side_effect=lambda event: event):
        yield


@pytest.fixture(scope='function')
def patch_base_app(patch_base_event):
    """Patches BaseApp

    1. patches __abstractmethods__, so we can initialize BaseApp
    2. patches event_cls with BaseEvent
    """

    with patch.object(BaseApp, '__abstractmethods__', set()), \
         patch.object(BaseApp, 'event_cls', BaseEvent):
        yield


@pytest.fixture(scope='function')
def base_app(patch_base_app):
    return BaseApp(app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='function')
def scheduled_app(redis):
    return ScheduledApp(app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='function')
def stream_app(redis):
    return StreamApp(app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='session')
def scheduled_event_str() -> str:
    with open(SCHEDULED_EVENT_FILE_PATH) as scheduled_event:
        return scheduled_event.read()


@pytest.fixture(scope='session')
def stream_event_str() -> str:
    with open(STREAM_EVENT_FILE_PATH) as stream_event:
        return stream_event.read()


@pytest.fixture(scope='function')
def stream_event(stream_event_str) -> STREAM_EVENT_TYPE:
    return BaseEvent._load(event=stream_event_str)
