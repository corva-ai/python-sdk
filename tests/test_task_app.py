import logging
import re

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva import Logger
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
        assert put_mock.request_history[0].json() == {'payload': {}}
        assert result is True


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
        assert put_mock.request_history[0].json() == {'payload': {}}
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


@pytest.mark.parametrize(
    'app_result, expected_payload',
    [
        pytest.param(
            True,
            {},
            id='Task handler doesnt store non-dict data in task payload.',
        ),
        pytest.param(
            {'field': 'value'},
            {'field': 'value'},
            id='Task handler stores dict data in task payload.',
        ),
    ],
)
def test_task_app_succeeds(
    app_result, expected_payload, context, requests_mock: RequestsMocker
):
    @task
    def task_app(event, api):
        return app_result

    event = RawTaskEvent(task_id='0', version=2).dict()

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'),
        json=TaskEvent(asset_id=int(), company_id=int()).dict(),
    )
    put_mock = requests_mock.put(re.compile('/v2/tasks/0/success'))

    result = task_app(event, context)[0]

    assert get_mock.called_once
    assert put_mock.called_once

    assert put_mock.request_history[0].json()['payload'] == expected_payload
    assert result == app_result


def test_log_if_unable_to_update_task_data(context, mocker: MockerFixture, capsys):
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

    captured = capsys.readouterr()

    assert 'ASSET=0' in captured.out
    assert 'Could not update task data.' in captured.out
    update_task_data_patch.assert_called_once()


def test_log_if_user_app_fails(
    context,
    mocker: MockerFixture,
    requests_mock: RequestsMocker,
    capsys,
):
    event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=int(), company_id=int()),
    )
    put_mock = requests_mock.put(re.compile('/v2/tasks/0/fail'))

    task(mocker.Mock(side_effect=Exception))(event, context)

    captured = capsys.readouterr()

    assert put_mock.called_once
    assert 'ASSET=0' in captured.out
    assert 'Task app failed to execute.' in captured.out


def test_custom_log_handler(context, mocker: MockerFixture, capsys):
    @task(handler=logging.StreamHandler())
    def app(event, api):
        Logger.info('Info message!')

    raw_event = RawTaskEvent(task_id='0', version=2).dict()
    event = TaskEvent(asset_id=0, company_id=int())

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=event,
    )
    mocker.patch.object(RawTaskEvent, 'update_task_data')

    app(raw_event, context)

    captured = capsys.readouterr()

    assert captured.out.endswith('Info message!\n')
    assert captured.err == 'Info message!\n'
