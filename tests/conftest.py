from unittest.mock import patch

import pytest
from fakeredis import FakeRedis
from pytest_mock import MockerFixture

from corva.app.base import BaseApp
from corva.app.scheduled import ScheduledApp
from corva.app.stream import StreamApp
from corva.constants import STREAM_EVENT_TYPE
from corva.event.data.scheduled import ScheduledEventData
from corva.event.data.stream import Record, StreamEventData
from corva.event.event import Event
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
def patch_base_app(mocker: MockerFixture):
    """Patches BaseApp

    1. patches __abstractmethods__, so we can initialize BaseApp
    """

    mocker.patch.object(BaseApp, '__abstractmethods__', set())
    yield


@pytest.fixture(scope='function')
def base_app(patch_base_app):
    return BaseApp(app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='function')
def scheduled_app():
    return ScheduledApp(app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='function')
def stream_app():
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
    return Event._load(event=stream_event_str)


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args


@pytest.fixture(scope='session')
def record_factory():
    def _record_factory(**kwargs):
        for key, val in dict(
             timestamp=int(),
             asset_id=int(),
             company_id=int(),
             version=int(),
             data={},
             collection=str()
        ).items():
            kwargs.setdefault(key, val)

        return Record(**kwargs)

    return _record_factory


@pytest.fixture(scope='session')
def stream_event_data_factory(record_factory):
    def _stream_event_data_factory(**kwargs):
        for key, val in dict(
             records=[],
             metadata={},
             asset_id=int(),
             app_connection_id=int(),
             app_stream_id=int(),
             is_completed=False
        ).items():
            kwargs.setdefault(key, val)

        return StreamEventData(**kwargs)

    return _stream_event_data_factory


@pytest.fixture(scope='session')
def scheduled_event_data_factory():
    def _scheduled_event_data_factory(**kwargs):
        for key, val in dict(
             cron_string=str(),
             environment=str(),
             app=int(),
             app_key=str(),
             app_version=None,
             app_connection_id=int(),
             app_stream_id=int(),
             source_type=str(),
             company=int(),
             provider=str(),
             schedule=int(),
             interval=int(),
             schedule_start=int(),
             schedule_end=int(),
             asset_id=int(),
             asset_name=str(),
             asset_type=str(),
             timezone=str(),
             log_type=str()
        ).items():
            kwargs.setdefault(key, val)

        return ScheduledEventData(**kwargs)

    return _scheduled_event_data_factory
