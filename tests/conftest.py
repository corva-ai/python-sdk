from functools import partial
from pathlib import Path
from unittest.mock import patch

import pytest
from fakeredis import FakeRedis, FakeServer

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
        CACHE_URL='redis://localhost:6379'
    )


@pytest.fixture(scope='function', autouse=True)
def patch_settings(settings, mocker):
    settings_path = 'corva.settings.SETTINGS'

    mocker.patch.multiple(
        settings_path,
        APP_KEY=settings.APP_KEY,
        CACHE_URL=settings.CACHE_URL
    )
    yield


@pytest.fixture(scope='session')
def raw_stream_event() -> str:
    with open(DATA_PATH / 'stream_event.json') as stream_event:
        return stream_event.read()


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args
