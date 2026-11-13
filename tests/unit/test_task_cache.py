import functools
import re

import fakeredis
import pytest
import redis
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva import Cache, TaskEvent, get_task_cache_key, task
from corva.cache_adapter import HashMigrator
from corva.configuration import SETTINGS
from corva.models.task import RawTaskEvent


@pytest.fixture
def task_event(mocker: MockerFixture, requests_mock: RequestsMocker):
    """Mocks the surroundings of a successful task app invoke."""

    def factory(event: TaskEvent = TaskEvent(asset_id=int(), company_id=int())):
        mocker.patch.object(RawTaskEvent, 'get_task_event', return_value=event)
        requests_mock.put(re.compile('/v2/tasks/0/success'))

        return RawTaskEvent(task_id='0', version=2).model_dump()

    return factory


def test_cache_round_trip_with_default_hash_name(context, task_event):
    hash_names = []

    @task(cache=True)
    def task_app(event, api, cache):
        hash_names.append(cache.cache_repo.hash_name)
        cache.set(key='key', value='value')
        return cache.get(key='key')

    event = task_event(TaskEvent(asset_id=3, company_id=int()))

    assert task_app(event, context)[0] == 'value'

    expected_hash_name = get_task_cache_key(
        provider=SETTINGS.PROVIDER, asset_id=3, app_key=SETTINGS.APP_KEY
    )
    assert hash_names == [HashMigrator.NEW_HASH_PREFIX + expected_hash_name]


def test_cache_persists_between_invokes(context, task_event):
    @task(cache=True)
    def write_app(event, api, cache):
        cache.set(key='key', value='value')

    @task(cache=True)
    def read_app(event, api, cache):
        return cache.get(key='key')

    event = task_event()

    write_app(event, context)

    assert read_app(event, context)[0] == 'value'


def test_cache_hash_name_builder(context, task_event):
    @task(cache=True, cache_hash_name_builder=lambda e: e.properties['cache_key'])
    def task_app(event, api, cache):
        return cache.cache_repo.hash_name

    event = task_event(
        TaskEvent(
            asset_id=int(), company_id=int(), properties={'cache_key': 'custom/key'}
        )
    )

    assert task_app(event, context)[0] == HashMigrator.NEW_HASH_PREFIX + 'custom/key'


def test_cache_connection_limit(context, task_event, requests_mock: RequestsMocker):
    """
    provided Cache object can't init more than one connection
    """

    @task(cache=True)
    def task_app(event, api, cache):
        pool = cache.cache_repo.client.connection_pool

        # unique single conn
        pool.get_connection()

        # Should be an error here since `max_connections=1` is passed to a Redis client
        pool.get_connection()

    event = task_event()
    requests_mock.put(re.compile('/v2/tasks/0/fail'))

    with pytest.raises(redis.exceptions.ConnectionError):
        task_app(event, context)


def test_cache_true_with_two_argument_app_fails(
    context, task_event, requests_mock: RequestsMocker
):
    @task(cache=True)
    def task_app(event, api):
        return True

    event = task_event()
    put_mock = requests_mock.put(re.compile('/v2/tasks/0/fail'))

    with pytest.raises(TypeError):
        task_app(event, context)

    assert put_mock.called_once


def test_failing_app_reports_fail_status(
    context, mocker: MockerFixture, requests_mock: RequestsMocker
):
    @task(cache=True)
    def task_app(event, api, cache):
        raise Exception('test_user_app_raises')

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=int(), company_id=int()),
    )
    put_mock = requests_mock.put(re.compile('/v2/tasks/0/fail'))

    event = RawTaskEvent(task_id='0', version=2).model_dump()

    with pytest.raises(Exception, match=r'^test_user_app_raises$'):
        task_app(event, context)

    assert put_mock.request_history[0].json() == {
        'fail_reason': 'test_user_app_raises'
    }


def test_other_app_can_read_task_cache(context, task_event):
    @task(cache=True)
    def task_app(event, api, cache):
        cache.set(key='result', value='42')

    task_app(task_event(TaskEvent(asset_id=3, company_id=int())), context)

    # e.g. a scheduled or stream app reading the task app cache
    reader = Cache(
        hash_name=get_task_cache_key(
            provider=SETTINGS.PROVIDER, asset_id=3, app_key=SETTINGS.APP_KEY
        ),
        redis_dsn=SETTINGS.CACHE_URL,
    )

    assert reader.get(key='result') == '42'


def test_app_runner_injects_fake_cache(app_runner):
    @task(cache=True)
    def task_app(event, api, cache):
        assert isinstance(cache.cache_repo.client, fakeredis.FakeRedis)

        cache.set(key='key', value='value')
        return cache.get(key='key')

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(task_app, event=event) == 'value'


def test_app_runner_reuses_provided_cache(app_runner):
    @task(cache=True)
    def task_app(event, api, cache):
        counter = int(cache.get(key='counter') or 0) + 1
        cache.set(key='counter', value=str(counter))
        return counter

    event = TaskEvent(asset_id=int(), company_id=int())

    cache = Cache(hash_name='hash_name', redis_dsn=SETTINGS.CACHE_URL, use_fakes=True)

    assert app_runner(task_app, event=event, cache=cache) == 1
    assert app_runner(task_app, event=event, cache=cache) == 2


def test_app_runner_detects_cache_through_extra_decorator(app_runner):
    """Decorators applied on top of the handler must not hide the cache marker."""

    def passthrough(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    @passthrough
    @task(cache=True)
    def task_app(event, api, cache):
        cache.set(key='key', value='value')
        return cache.get(key='key')

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(task_app, event=event) == 'value'


def test_app_runner_task_app_without_cache_unchanged(app_runner):
    @task
    def task_app(event, api):
        return 'Hello, World!'

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(task_app, event=event) == 'Hello, World!'

    # cache kwarg is ignored for task apps that don't use cache=True
    cache = Cache(hash_name='hash_name', redis_dsn=SETTINGS.CACHE_URL, use_fakes=True)

    assert app_runner(task_app, event=event, cache=cache) == 'Hello, World!'
