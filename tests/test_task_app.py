import re

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva.application import Corva


@pytest.mark.parametrize(
    'status_code,json,status',
    (
        [400, None, 'fail'],
        [
            200,
            {
                'id': '0',
                'state': 'running',
                'asset_id': int(),
                'company_id': int(),
                'app_id': int(),
                'document_bucket': str(),
            },
            'success',
        ],
    ),
)
def test_get_task_data_raises(
    status_code,
    json,
    status,
    corva_context,
    requests_mock: RequestsMocker,
):
    def task_app(event, api):
        return True

    event = {'task_id': '0', 'version': 2}

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'), status_code=status_code, json=json
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/{status}'))

    result = Corva(corva_context).task(task_app, event)

    assert get_mock.called_once
    assert put_mock.called_once

    if status == 'fail':
        assert set(put_mock.request_history[0].json()) == {'fail_reason'}
        assert result is None

    if status == 'success':
        assert put_mock.request_history[0].json() == {'payload': True}
        assert result is True


@pytest.mark.parametrize('status,side_effect', (['fail', Exception], ['success', None]))
def test_user_app_raises(
    status,
    side_effect,
    corva_context,
    mocker: MockerFixture,
    requests_mock: RequestsMocker,
):
    event = {'task_id': '0', 'version': 2}

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'),
        json={
            'id': '0',
            'state': 'running',
            'asset_id': int(),
            'company_id': int(),
            'app_id': int(),
            'document_bucket': str(),
        },
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/{status}'))

    result = Corva(corva_context).task(
        mocker.Mock(side_effect=side_effect, return_value=True), event
    )

    assert get_mock.called_once
    assert put_mock.called_once

    if status == 'fail':
        assert set(put_mock.request_history[0].json()) == {'fail_reason'}
        assert result is None

    if status == 'success':
        assert put_mock.request_history[0].json() == {'payload': True}
        assert result is True


def test_task_runner(
    corva_context,
    mocker: MockerFixture,
    requests_mock: RequestsMocker,
):
    def task_app(event, api):
        return True

    event = {'task_id': '0', 'version': 2}

    get_mock = requests_mock.get(
        re.compile('/v2/tasks/0'),
        json={
            'id': '0',
            'state': 'running',
            'asset_id': int(),
            'company_id': int(),
            'app_id': int(),
            'document_bucket': str(),
        },
    )
    put_mock = requests_mock.put(re.compile(f'/v2/tasks/0/success'))

    result = Corva(corva_context).task(task_app, event)

    assert get_mock.called_once
    assert put_mock.called_once

    assert put_mock.request_history[0].json() == {'payload': True}
    assert result is True
