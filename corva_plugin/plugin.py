import contextlib
import copy
import functools
import os
import re
import types
from unittest import mock

import fakeredis
import pytest
import requests_mock


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(args, early_config, parser):
    """Sets test environment variables."""

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
def corva_patch():
    """Simplifies testing of Corva apps by patching essential functionality."""

    with patch_redis_adapter(), patch_scheduled(), patch_stream():
        yield


@pytest.fixture(scope='function')
def corva_context(corva_patch):
    """Imitates AWS lambda context expected by Corva."""

    return types.SimpleNamespace(
        client_context=types.SimpleNamespace(env={'API_KEY': '123'})
    )


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
def patch_stream():
    """Patches stream runner."""

    from corva.application import Corva
    from corva.configuration import SETTINGS

    def decorator(func):
        def test_stream(self: Corva, fn, event, *args, **kwargs):
            events = copy.deepcopy(event)
            if not isinstance(events, list):
                events = [events]

            for event in events:
                event.setdefault(
                    'metadata',
                    {
                        'app_stream_id': int(),
                        'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                    },
                )

            return func(self, fn, events, *args, **kwargs)

        return test_stream

    with mock.patch.object(Corva, 'stream', decorator(Corva.stream)):
        yield


@contextlib.contextmanager
def patch_scheduled():
    """Patches scheduled runner."""

    from corva.application import Corva

    def decorator(func):
        def test_scheduled(self: Corva, fn, event, *args, **kwargs):
            events = copy.deepcopy(event)
            if not isinstance(events, list):
                events = [events]
            if not isinstance(events[0], list):
                events = [events]

            for i in range(len(events)):
                for event in events[i]:
                    event.setdefault('app_connection', int())
                    event.setdefault('app_stream', int())

            return func(self, fn, events, *args, **kwargs)

        return test_scheduled

    with mock.patch.object(
        Corva, 'scheduled', decorator(Corva.scheduled)
    ), requests_mock.Mocker() as mocker:
        # patch post request, that sets scheduled task as completed
        # matches url like https://dns/scheduler/123/completed
        mocker.post(re.compile(r'https://.+/scheduler/\d+/completed'))

        yield
