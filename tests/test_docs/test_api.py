import contextlib
from json import JSONDecodeError

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva import Api, TaskEvent
from corva.testing import TestClient
from docs.src.api import tutorial001, tutorial002, tutorial003, tutorial004, tutorial005


def test_tutorial001(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())

    mock1 = requests_mock.get('/v2/pads')
    mock2 = requests_mock.get('/api/v1/data/provider/dataset/')
    mock3 = requests_mock.get('https://api.corva.ai/v2/pads')

    app_runner(tutorial001.task_app, event)

    assert mock1.called_once
    assert mock2.called_once
    assert mock3.called_once


@pytest.mark.parametrize(
    'json,ctx', ([{}, contextlib.nullcontext()], [None, pytest.raises(JSONDecodeError)])
)
def test_tutorial002(json, ctx, app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())

    mock1 = requests_mock.get('/v2/pads', json=json)
    mock2 = requests_mock.get('/v2/pads?company=1', complete_qs=True)

    with ctx:
        app_runner(tutorial002.task_app, event)

    assert mock1.called_once
    assert mock2.called_once


def test_tutorial003(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())

    post_mock = requests_mock.post('/v2/pads')
    delete_mock = requests_mock.delete('/v2/pads/123')
    put_mock = requests_mock.put('/api/v1/data/provider/dataset/')
    patch_mock = requests_mock.patch('/v2/pads/123')

    app_runner(tutorial003.task_app, event)

    assert post_mock.last_request._request.body.decode() == '{"key": "val"}'
    assert delete_mock.called_once
    assert put_mock.last_request._request.body.decode() == '{"key": "val"}'
    assert patch_mock.last_request._request.body.decode() == '{"key": "val"}'


def test_tutorial004(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())

    expected_headers = {
        'header': 'header-value',
        **TestClient._api.default_headers,
    }

    mock = requests_mock.get('/v2/pads')

    app_runner(tutorial004.task_app, event)

    assert mock.call_count == 2
    for header, header_value in expected_headers.items():
        assert mock.request_history[0]._request.headers[header] == header_value
    assert mock.request_history[1].timeout == 5


def test_tutorial005(app_runner, mocker: MockerFixture):
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())

    mock = mocker.patch.object(Api, 'get_dataset')

    app_runner(tutorial005.task_app, event)

    mock.assert_called_once()
