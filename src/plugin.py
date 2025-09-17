import os

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
