import contextlib
import copy
import functools
import os
import types
from typing import Callable, List, Union
from unittest import mock

import fakeredis
import pytest


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
      See load order here: https://docs.pytest.org/en/stable/writing_plugins.html#plugin-discovery-order-at-tool-startup
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
def _corva_patch():
    """Simplifies testing of Corva apps by patching some internal functionality."""

    with patch_redis_adapter(), patch_stream():
        yield


@pytest.fixture(scope='function')
def corva_context(_corva_patch):
    """Imitates AWS Lambda context expected by Corva."""

    return types.SimpleNamespace(
        client_context=types.SimpleNamespace(env={'API_KEY': '123'})
    )


@contextlib.contextmanager
def patch_redis_adapter():
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


@contextlib.contextmanager
def patch_stream():
    """Patches stream runner."""

    # imports are local to avoid loading packages, on the first plugin run
    from corva.application import Corva
    from corva.configuration import SETTINGS

    def patch_corva(func):
        def _patch_corva(
            self: Corva, fn: Callable, event: Union[dict, List[dict]], *args, **kwargs
        ):
            """Automatically adds essential fields to event in Corva.stream."""

            events = copy.deepcopy(event)

            # cast event to expected type List[dict], if needed
            if not isinstance(events, list):
                # allow users to send event as dict
                events = [events]

            for event in events:
                # do not override the values, if provided by user
                event.setdefault(
                    'metadata',
                    {
                        'app_stream_id': int(),
                        'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                    },
                )

            return func(self, fn, events, *args, **kwargs)

        return _patch_corva

    with mock.patch.object(Corva, 'stream', patch_corva(Corva.stream)):
        yield


@pytest.fixture(scope='function', autouse=True)
def _patch_scheduled(request):
    """Patches scheduled runner."""

    # imports are local to avoid loading packages, on the first plugin run
    from corva.application import Corva

    def patch_corva(func):
        def _patch_corva(
            self: Corva,
            fn: Callable,
            event: Union[dict, List[dict], List[List[dict]]],
            *args,
            **kwargs,
        ):
            """Automatically adds essential fields to event in Corva.scheduled."""

            events = copy.deepcopy(event)

            # cast event to expected type List[List[dict]], if needed
            if not isinstance(events, list):
                # allow users to send event as dict
                events = [events]
            if not isinstance(events[0], list):
                # allow users to send event as List[dict]
                events = [events]

            for i in range(len(events)):
                for event in events[i]:
                    # do not override the values, if provided by user
                    event.setdefault('app_connection', int())
                    event.setdefault('app_stream', int())

            return func(self, fn, events, *args, **kwargs)

        return _patch_corva

    patches = [mock.patch.object(Corva, 'scheduled', patch_corva(Corva.scheduled))]

    # parametrize patching of set_schedule_as_completed:
    #   1. must be always on for user tests
    #   2. must be disabled for some SDK tests
    if getattr(request, 'param', True):
        # avoid POSTing in user tests
        patches.append(mock.patch('corva.runners.scheduled.set_schedule_as_completed'))

    try:
        for patch in patches:
            patch.start()

        yield
    finally:
        mock.patch.stopall()
