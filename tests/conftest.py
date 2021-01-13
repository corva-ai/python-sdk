import json
from functools import partial
from pathlib import Path
from unittest.mock import patch

import pytest
from fakeredis import FakeRedis, FakeServer

from corva.models import stream
from corva.network.api import Api
from corva.settings import Settings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState

DATA_PATH = Path('tests/test_data')


@pytest.fixture(scope='function', autouse=True)
def patch_redis_adapter():
    """Patches RedisAdapter

    1. patches RedisAdapter.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = patch(f'{redis_adapter_path}.RedisAdapter.__bases__', (FakeRedis,))

    with redis_adapter_patcher, \
         patch(f'{redis_adapter_path}.from_url', side_effect=partial(FakeRedis.from_url, server=FakeServer())):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis_adapter(patch_redis_adapter, settings):
    return RedisAdapter(default_name='default_name', cache_url=settings.CACHE_URL)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)


@pytest.fixture(scope='function')
def api():
    return Api(
        api_url='https://api.localhost.ai',
        data_api_url='https://data.localhost.ai',
        api_key='',
        app_name=''
    )


@pytest.fixture(scope='function')
def settings():
    return Settings(
        APP_KEY='provider.app-name',
        CACHE_URL='redis://localhost:6379',
        API_ROOT_URL='https://api.localhost.ai',
        DATA_API_ROOT_URL='https://data.localhost.ai'
    )


@pytest.fixture(scope='function', autouse=True)
def patch_settings(settings, mocker):
    settings_path = 'corva.settings.SETTINGS'

    mocker.patch.multiple(
        settings_path,
        **settings.dict()
    )
    yield


@pytest.fixture(scope='session')
def raw_stream_event() -> str:
    with open(DATA_PATH / 'stream_event.json') as stream_event:
        return stream_event.read()


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args


class StreamDataMixer:
    @classmethod
    def record_data(cls, **kwargs) -> stream.RecordData:
        default_kwargs = {}
        default_kwargs.update(**kwargs)

        return stream.RecordData(**default_kwargs)

    @classmethod
    def record(cls, **kwargs) -> stream.Record:
        default_kwargs = {
            'asset_id': int(),
            'company_id': int(),
            'version': int(),
            'collection': str(),
            'data': cls.record_data()
        }
        default_kwargs.update(kwargs)

        return stream.Record(**default_kwargs)

    @classmethod
    def app_metadata(cls, **kwargs) -> stream.AppMetadata:
        default_kwargs = {'app_connection_id': int()}
        default_kwargs.update(kwargs)

        return stream.AppMetadata(**default_kwargs)

    @classmethod
    def stream_event_metadata(cls, **kwargs) -> stream.StreamEventMetadata:
        default_kwargs = {
            'app_stream_id': int(),
            'apps': {}
        }
        default_kwargs.update(kwargs)

        return stream.StreamEventMetadata(**default_kwargs)

    @classmethod
    def stream_event(cls, **kwargs) -> stream.StreamEvent:
        default_kwargs = {
            'records': [],
            'metadata': cls.stream_event_metadata()
        }
        default_kwargs.update(kwargs)

        return stream.StreamEvent(**default_kwargs)

    @classmethod
    def to_raw_event(cls, *events: stream.StreamEvent) -> str:
        return json.dumps([event.dict(exclude_defaults=True) for event in events])
