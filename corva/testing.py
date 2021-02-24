import contextlib
import functools
import os
from unittest import mock

import fakeredis
import pytest

from corva.configuration import Settings


@pytest.fixture(scope='function')
def corva_patch():
    """Simplifies testing of Corva apps by patching some functionality."""

    with patch_redis_adapter(), patch_settings():
        yield


@contextlib.contextmanager
def patch_redis_adapter():
    """Allows testing Corva apps without running real redis server.

    Internally Corva uses Redis as cache. The fixture allows testing Corva apps
    without running real redis server. It patches RedisAdapter to use fakeredis
    instead of redis. fakeredis is a library that simulates talking to a real redis
    server.

    Fixture patch steps:
      1. patch RedisAdapter.__bases__ to use fakeredis.FakeRedis instead of redis.Redis
      2. patch redis.from_url with fakeredis.FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = mock.patch(
        f'{redis_adapter_path}.RedisAdapter.__bases__', (fakeredis.FakeRedis,)
    )

    server = (
        fakeredis.FakeServer()
    )  # use FakeServer to share cache between different instances of RedisState
    from_url_patcher = mock.patch(
        f'{redis_adapter_path}.from_url',
        side_effect=functools.partial(fakeredis.FakeRedis.from_url, server=server),
    )

    with redis_adapter_patcher, from_url_patcher:
        # stops mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True

        yield


@contextlib.contextmanager
def patch_settings():
    """Sets test env vars in global Corva settings."""

    app_name = 'App Name'
    provider = 'provider'
    env = {
        'API_ROOT_URL': 'https://api.localhost.ai',
        'DATA_API_ROOT_URL': 'https://data.localhost.ai',
        'API_KEY': '123',
        'CACHE_URL': 'redis://localhost:6379',
        'APP_NAME': app_name,
        'PROVIDER': provider,
        'APP_KEY': f'{provider}.{app_name.lower().replace(" ", "-")}',
        **os.environ,  # override env values if provided
    }

    # patch global settings
    with mock.patch.dict(os.environ, env):
        # load settings that contain new env vars
        new_settings = Settings()

        with mock.patch.multiple('corva.configuration.SETTINGS', **new_settings.dict()):
            yield
