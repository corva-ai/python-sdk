from functools import partial
from unittest.mock import patch

import pytest
from fakeredis import FakeRedis, FakeServer

from corva.api import Api
from corva.configuration import SETTINGS
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


@pytest.fixture(scope='function', autouse=True)
def patch_redis_adapter():
    """Patches RedisAdapter

    1. patches RedisAdapter.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = patch(
        f'{redis_adapter_path}.RedisAdapter.__bases__', (FakeRedis,)
    )

    server = (
        FakeServer()
    )  # use FakeServer to share cache between different instances of RedisState

    with redis_adapter_patcher, patch(
        f'{redis_adapter_path}.from_url',
        side_effect=partial(FakeRedis.from_url, server=server),
    ):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis_adapter(patch_redis_adapter, patch_settings):
    return RedisAdapter(default_name='default_name', cache_url=SETTINGS.CACHE_URL)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)


# TODO: delete after getting rid of corva/app/base.py
@pytest.fixture(scope='function')
def api():
    return Api(
        api_url='https://api.localhost.ai',
        data_api_url='https://data.localhost.ai',
        api_key='',
        app_name='',
    )


@pytest.fixture(scope='function', autouse=True)
def patch_settings(mocker):
    """replaces values in global corva settings with proper test values"""

    settings_path = 'corva.configuration.SETTINGS'

    mocker.patch.multiple(
        settings_path,
        **{
            'APP_KEY': 'provider.app-name',
            'CACHE_URL': 'redis://localhost:6379',
            'API_ROOT_URL': 'https://api.localhost.ai',
            'DATA_API_ROOT_URL': 'https://data.localhost.ai',
            'API_KEY': '123',
        },
    )
    yield


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args
