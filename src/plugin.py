import functools
import os
from unittest import mock

import fakeredis
import pytest


@pytest.fixture(scope='function')
def app_runner():
    """Returns a function that should be used to run apps in tests."""

    # imports are local to avoid loading packages, on the first plugin run
    from corva.testing import TestClient

    return TestClient.run


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(args, early_config, parser):
    """Sets test environment variables.

    This hook is triggered before loading the packages being tested.
    If packages contain global variables, that read from env,
    they will be able to read env values set by this hook.

    Why does this hook get triggered before loading the packages being tested?
      The current file that contains the hook is registered as a pytest plugin through
      setuptools entry points. Plugins registered like this are loaded before loading
      the packages being tested.
      See load order here:
        https://docs.pytest.org/en/stable/writing_plugins.html#plugin-discovery-order-at-tool-startup
    """

    provider = 'test-provider'
    env = {
        'API_ROOT_URL': 'https://api.localhost.ai',
        'DATA_API_ROOT_URL': 'https://data.localhost.ai',
        'CACHE_URL': 'redis://localhost:6379',
        'APP_KEY': f'{provider}.test-app-name',
        'PROVIDER': provider,
        **os.environ,  # override env values if provided by user
    }
    os.environ.update(env)


@pytest.fixture(scope='function', autouse=True)
def _patch_redis_adapter():
    """Allows testing of Corva apps without running a real Redis server.

    Internally Corva uses Redis as cache. This function patches RedisAdapter to use
    fakeredis instead of redis. fakeredis is a library that simulates talking to a
    real Redis server. This way the function allows testing Corva apps without running
    a real Redis server.

    Patch steps:
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
