import pytest
from pytest_mock import MockerFixture

from corva.app.task import TaskApp
from corva.event import Event
from corva.models.task import TaskStatus, TaskData, TaskEventData, TaskContext, UpdateTaskData
from tests.conftest import ComparableException, APP_KEY, CACHE_URL

TASK_ID = '1'


@pytest.fixture(scope='function')
def task_app(api):
    return TaskApp(api=api, app_key=APP_KEY, cache_url=CACHE_URL)


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
def task_context_factory(task_event_data_factory, task_data_factory):
    def _task_context_factory(**kwargs):
        for key, val in dict(
             event=Event([task_event_data_factory()]),
             task=task_data_factory(),
        ).items():
            kwargs.setdefault(key, val)

        return TaskContext(**kwargs)

    return _task_context_factory


def test_group_by_field():
    assert TaskApp.group_by_field == 'task_id'


def test_get_task_data(mocker: MockerFixture, task_app, task_data_factory):
    task_data = task_data_factory()

    mocker.patch.object(
        task_app.api.session,
        'request',
        return_value=mocker.Mock(**{'json.return_value': task_data.dict()})
    )
    get_spy = mocker.spy(task_app.api, 'get')

    result = task_app.get_task_data(task_id=TASK_ID)

    assert task_data == result
    get_spy.assert_called_once_with(path=f'v2/tasks/{TASK_ID}')


def test_update_task_data(mocker: MockerFixture, task_app):
    status = TaskStatus.fail.value
    data = UpdateTaskData()

    mocker.patch.object(task_app.api.session, 'request')
    put_spy = mocker.spy(task_app.api, 'put')

    task_app.update_task_data(task_id=TASK_ID, status=status, data=data)

    put_spy.assert_called_once_with(path=f'v2/tasks/{TASK_ID}/{status}', data=data.dict())


def test_post_process_calls_update_task_data(mocker: MockerFixture, task_app, task_context_factory):
    save_data = {'key1': 'val1'}
    context = task_context_factory(task_result=save_data)

    mocker.patch.object(task_app.api, 'put')
    spy = mocker.spy(task_app, 'update_task_data')

    task_app.post_process(context=context)

    spy.assert_called_once_with(
        task_id=context.task.id,
        status=TaskStatus.success.value,
        data=UpdateTaskData(payload=save_data)
    )


def test_on_fail_calls_update_task_data(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()
    exc = ComparableException('123')

    mocker.patch.object(task_app.api, 'put')
    spy = mocker.spy(task_app, 'update_task_data')

    task_app.on_fail(context=context, exception=exc)

    spy.assert_called_once_with(
        task_id=context.task.id,
        status=TaskStatus.fail.value,
        data=UpdateTaskData(fail_reason=str(exc))
    )
