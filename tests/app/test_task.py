import pytest
from pytest_mock import MockerFixture

from corva.app.task import TaskApp
from corva.event import Event
from corva.models.task import TaskStatus, TaskData, TaskEventData, TaskContext, UpdateTaskData
from corva.network.api import Api
from corva.settings import CORVA_SETTINGS

TASK_ID = '1'


@pytest.fixture(scope='function')
def task_app():
    return TaskApp(
        api=Api(
            api_url=CORVA_SETTINGS.API_ROOT_URL,
            data_api_url=CORVA_SETTINGS.DATA_API_ROOT_URL,
            api_key=CORVA_SETTINGS.API_KEY,
            app_name=CORVA_SETTINGS.APP_NAME
        ),
        app_key=CORVA_SETTINGS.APP_KEY, cache_url=CORVA_SETTINGS.CACHE_URL
    )


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
    type(task_app.api).get = mocker.PropertyMock(return_value=mocker.Mock(wraps=task_app.api.get))

    result = task_app.get_task_data(task_id=TASK_ID)

    assert task_data == result
    task_app.api.get.assert_called_once_with(path=f'v2/tasks/{TASK_ID}')


def test_update_task_data(mocker: MockerFixture, task_app):
    status = TaskStatus.fail.value
    data = UpdateTaskData()

    mocker.patch.object(task_app.api.session, 'request')
    type(task_app.api).put = mocker.PropertyMock(return_value=mocker.Mock(wraps=task_app.api.put))

    task_app.update_task_data(task_id=TASK_ID, status=status, data=data)

    task_app.api.put.assert_called_once_with(path=f'v2/tasks/{TASK_ID}/{status}', data=data.dict())
