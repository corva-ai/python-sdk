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


@pytest.mark.parametrize('with_parentheses', (True, False))
def test_cache_hash_name_builder_requires_cache(with_parentheses):
    def task_app(event, api, cache):
        pass

    def builder(event):
        return 'custom/key'

    with pytest.raises(ValueError, match='requires cache=True'):
        if with_parentheses:
            task(cache_hash_name_builder=builder)(task_app)
        else:
            task(task_app, cache_hash_name_builder=builder)


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


def test_app_runner_injects_fake_cache(app_runner):
    @task(cache=True)
    def task_app(event, api, cache):
        assert isinstance(cache.cache_repo.client, fakeredis.FakeRedis)

        cache.set(key='key', value='value')
        return cache.get(key='key')

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(task_app, event=event) == 'value'


def test_app_runner_uses_default_hash_name(app_runner):
    @task(cache=True)
    def task_app(event, api, cache):
        return cache.cache_repo.hash_name

    event = TaskEvent(asset_id=3, company_id=int())

    expected_hash_name = get_task_cache_key(
        provider=SETTINGS.PROVIDER, asset_id=3, app_key=SETTINGS.APP_KEY
    )
    assert (
        app_runner(task_app, event=event)
        == HashMigrator.NEW_HASH_PREFIX + expected_hash_name
    )


def test_app_runner_uses_cache_hash_name_builder(app_runner):
    @task(cache=True, cache_hash_name_builder=lambda e: e.properties['cache_key'])
    def task_app(event, api, cache):
        return cache.cache_repo.hash_name

    event = TaskEvent(
        asset_id=int(), company_id=int(), properties={'cache_key': 'custom/key'}
    )

    assert (
        app_runner(task_app, event=event) == HashMigrator.NEW_HASH_PREFIX + 'custom/key'
    )


@pytest.mark.parametrize('provide_cache', (True, False))
def test_app_runner_fails_if_cache_hash_name_builder_fails(app_runner, provide_cache):
    """A builder that fails in production must fail under app_runner as well."""

    @task(cache=True, cache_hash_name_builder=lambda e: e.properties['cache_key'])
    def task_app(event, api, cache):
        pass

    event = TaskEvent(asset_id=int(), company_id=int())

    cache = (
        Cache(hash_name='hash_name', redis_dsn=SETTINGS.CACHE_URL, use_fakes=True)
        if provide_cache
        else None
    )

    with pytest.raises(KeyError, match='cache_key'):
        app_runner(task_app, event=event, cache=cache)


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
    """Decorators applied on top of the handler must not hide the task cache."""

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
