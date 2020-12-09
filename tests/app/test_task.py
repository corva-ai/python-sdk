from types import SimpleNamespace

from pytest_mock import MockerFixture

from corva.app.task import TaskApp
from corva.models.task import UpdateTaskData
from tests.conftest import ComparableException


def test_group_by_field():
    assert TaskApp.group_by_field == 'task_id'


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
    data = UpdateTaskData()

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
        data=UpdateTaskData(payload=save_data)
    )


def test_on_fail_calls_update_task_data(mocker: MockerFixture, task_app, task_context_factory):
    context = task_context_factory()
    exc = ComparableException('123')
    update_task_data_info = UpdateTaskData(fail_reason=str(exc))

    update_task_data_mock = mocker.patch.object(task_app, 'update_task_data')

    task_app.on_fail(context=context, exception=exc)

    update_task_data_mock.assert_called_once_with(
        task_id=context.task.id,
        status='fail',
        data=update_task_data_info
    )
