import traceback
from types import SimpleNamespace

import pytest
from pytest_mock import MockerFixture

from corva.app.base import BaseApp
from corva.app.task import TaskApp
from corva.app.utils.context import TaskContext
from corva.app.utils.task_model import UpdateTaskInfoData
from corva.event.event import Event
from corva.event.loader.task import TaskLoader
from tests.conftest import CustomException


@pytest.mark.parametrize(
    'attr_name,expected', (('group_by_field', 'task_id'),)
)
def test_default_values(attr_name, expected):
    assert getattr(TaskApp, attr_name) == expected


def test_event_loader(task_app):
    event_loader = task_app.event_loader()

    assert isinstance(event_loader, TaskLoader)


def test_get_context(mocker: MockerFixture, task_app, task_data_factory, task_event_data_factory):
    mocker.patch.object(task_app, 'get_task_data', return_value=task_data_factory())

    context = task_app.get_context(event=Event(data=[task_event_data_factory()]))

    assert isinstance(context, TaskContext)


def test_pre_process_calls_base(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()

    super_pre_process_mock = mocker.patch.object(BaseApp, 'pre_process')

    task_app.pre_process(context=context)

    super_pre_process_mock.assert_called_once_with(context=context)


def test_process_calls_base(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()

    super_process_mock = mocker.patch.object(BaseApp, 'process')

    task_app.process(context=context)

    super_process_mock.assert_called_once_with(context=context)


def test_post_process_calls_base(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()

    super_post_process_mock = mocker.patch.object(BaseApp, 'post_process')
    mocker.patch.object(task_app, 'update_task_data')

    task_app.post_process(context=context)

    super_post_process_mock.assert_called_once_with(context=context)


def test_on_fail_calls_base(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()
    exc = CustomException('')

    super_on_fail_mock = mocker.patch.object(BaseApp, 'on_fail')
    mocker.patch.object(task_app, 'update_task_data')

    task_app.on_fail(context=context, exception=exc)

    super_on_fail_mock.assert_called_once_with(context=context, exception=exc)


def test_get_task_data(mocker: MockerFixture, task_app, task_data_factory):
    task_id = '1'
    task_data = task_data_factory()

    get_mock = mocker.patch.object(
        task_app.api,
        'get',
        return_value=SimpleNamespace(data=task_data.dict())
    )

    result = task_app.get_task_data(task_id=task_id)

    assert task_data == result
    get_mock.assert_called_once_with(path=f'v2/tasks/{task_id}')


def test_update_task_data(mocker: MockerFixture, task_app):
    task_id = '1'
    status = 'fail'
    data = UpdateTaskInfoData()

    put_mock = mocker.patch.object(task_app.api, 'put')

    task_app.update_task_data(task_id=task_id, status=status, data=data)

    put_mock.assert_called_once_with(path=f'v2/tasks/{task_id}/{status}', json=data.dict())


def test_post_process_calls_update_task_data(mocker: MockerFixture, task_app, task_context_factory):
    save_data = {'key1': 'val1'}
    context = task_context_factory(save_data=save_data)

    update_task_data_mock = mocker.patch.object(task_app, 'update_task_data')

    task_app.post_process(context=context)

    update_task_data_mock.assert_called_once_with(
        task_id=context.task.id,
        status='success',
        data=UpdateTaskInfoData(payload=save_data)
    )


def test_on_fail_calls_update_task_data(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()
    exc = CustomException('123')
    update_task_data_info = UpdateTaskInfoData(
        fail_reason=str(exc),
        payload={'error': ''.join(traceback.TracebackException.from_exception(exc).format())}
    )

    update_task_data_mock = mocker.patch.object(task_app, 'update_task_data')

    task_app.on_fail(context=context, exception=exc)

    update_task_data_mock.assert_called_once_with(
        task_id=context.task.id,
        status='fail',
        data=update_task_data_info
    )
