from unittest.mock import patch

import pytest
from fakeredis import FakeRedis
from corva.app.task import TaskApp
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState
from corva.app.utils.context import TaskContext
from corva.app.utils.task_model import TaskData
from corva.event.data.task import TaskEventData


APP_KEY = 'provider.app-name'
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


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args

@pytest.fixture(scope='function')
def task_app():
    return TaskApp(app_key=APP_KEY, cache_url=CACHE_URL)

@pytest.fixture(scope='session')
def task_event_str() -> str:
    with open('data/tests/task_event.json') as task_event:
        return task_event.read()

@pytest.fixture(scope='session')
def task_event_data_factory():
    def _task_event_data_factory(**kwargs):
        for key, val in dict(
             task_id=str(),
             version=2
        ).items():
            kwargs.setdefault(key, val)

        return TaskEventData(**kwargs)

    return _task_event_data_factory

@pytest.fixture(scope='session')
def task_data_factory():
    def _task_data_factory(**kwargs):
        for key, val in dict(
             id=str(),
             state='running',
             asset_id=int(),
             company_id=int(),
             app_id=int(),
             document_bucket=str(),
             properties={},
             payload={},
        ).items():
            kwargs.setdefault(key, val)

        return TaskData(**kwargs)

    return _task_data_factory

@pytest.fixture(scope='session')
def task_context_factory(task_event_data_factory, task_data_factory):
    def _task_context_factory(**kwargs):
        for key, val in dict(
             event=Event(data=[task_event_data_factory()]),
             task=task_data_factory(),
        ).items():
            kwargs.setdefault(key, val)

        return TaskContext(**kwargs)

    return _task_context_factory
