import re

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva.handlers import task
from corva.models.task import RawTaskEvent, TaskEvent


@pytest.mark.parametrize(
    'status_code,json,status',
    (
        [400, None, 'fail'],
        [
            200,
            TaskEvent(asset_id=int(), company_id=int()).dict(),
            'success',
        ],
    ),
)
def test_lambda_succeeds_if_unable_to_get_task_event(
    status_code,
    json,
    status,
    context,
    requests_mock: RequestsMocker,
):
    @task
    def task_app(event, api):
        return True

    event = RawTaskEvent(task_id='0', version=2).dict()

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'), status_code=status_code, json=json
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/{status}'))

    result = task_app(event, context)[0]

    assert get_mock.called_once
    assert put_mock.called_once

    if status == 'fail':
        assert set(put_mock.request_history[0].json()) == {'fail_reason'}
        assert result is None

    if status == 'success':
        assert put_mock.request_history[0].json() == {'payload': True}
        assert result is True


def test_lambda_succeeds_if_unable_to_setup_logging(
    context, mocker: MockerFixture, requests_mock: RequestsMocker
):
    @task
    def task_app(event, api):
        pass

    event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=int(), company_id=int()),
    )
    mocker.patch(
        'corva.handlers.setup_logging',
        side_effect=Exception('test_setup_logging_raises'),
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/fail'))

    task_app(event, context)

    assert put_mock.called_once
    assert put_mock.request_history[0].json() == {
        'fail_reason': 'test_setup_logging_raises'
    }


@pytest.mark.parametrize(
    'status,side_effect',
    (['fail', Exception('test_user_app_raises')], ['success', None]),
)
def test_lambda_succeeds_if_user_app_fails(
    status,
    side_effect,
    context,
    mocker: MockerFixture,
    requests_mock: RequestsMocker,
):
    event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=int(), company_id=int()),
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/{status}'))

    result = task(mocker.Mock(side_effect=side_effect, return_value=True))(
        event, context
    )[0]

    assert put_mock.called_once

    if status == 'fail':
        assert put_mock.request_history[0].json() == {
            'fail_reason': 'test_user_app_raises'
        }
        assert result is None

    if status == 'success':
        assert put_mock.request_history[0].json() == {'payload': True}
        assert result is True


def test_lambda_succeeds_if_unable_to_update_task_data(context, mocker: MockerFixture):
    @task
    def task_app(event, api):
        return True

    event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=int(), company_id=int()),
    )
    update_task_data_patch = mocker.patch.object(
        RawTaskEvent,
        'update_task_data',
        side_effect=Exception,
    )

    task_app(event, context)

    update_task_data_patch.assert_called_once()


def test_task_app_succeeds(context, requests_mock: RequestsMocker):
    @task
    def task_app(event, api):
        return True

    event = RawTaskEvent(task_id='0', version=2).dict()

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'),
        json=TaskEvent(asset_id=int(), company_id=int()).dict(),
    )
    put_mock = requests_mock.put(re.compile('/v2/tasks/0/success'))

    result = task_app(event, context)[0]

    assert get_mock.called_once
    assert put_mock.called_once

    assert put_mock.request_history[0].json() == {'payload': True}
    assert result is True
