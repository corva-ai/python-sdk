from typing import Literal

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.configuration import SETTINGS


@pytest.fixture(scope='function')
def event(patch_settings):
    return [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]


def app(event, api, cache):
    return api


def test_request_default_headers(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.get('')  # do some api call

    request_patch.assert_called_once()
    assert not (
        set(request_patch.call_args.kwargs['headers'])
        - {'Authorization', 'X-Corva-App'}
    )


@pytest.mark.parametrize(
    'path,url,type_',
    [
        ['http://localhost', 'http://localhost', ''],
        ['api/v10/path', '%s/api/v10/path', 'data'],
        ['/api/v10/path', '%s/api/v10/path', 'data'],
        ['v10/path', '%s/v10/path', 'corva'],
        ['/v10/path', '%s/v10/path', 'corva'],
    ],
)
def test_request_url(
    event, mocker: MockerFixture, path, url, type_: Literal['data', 'corva', '']
):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.get(path)

    expected = url

    if type_ == 'data':
        expected = url % SETTINGS.DATA_API_ROOT_URL

    if type_ == 'corva':
        expected = url % SETTINGS.API_ROOT_URL

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['url'] == expected


def test_request_data_param_passed_as_json(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.post('', data={})

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['json'] == {}


def test_request_additional_headers(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.post('', headers={'custom': 'value'})

    request_patch.assert_called_once()
    assert len(request_patch.call_args.kwargs['headers']) == 3
    assert request_patch.call_args.kwargs['headers']['custom'] == 'value'


@pytest.mark.parametrize(
    'timeout, raises',
    [(3, False), (30, False), (2, True), (31, True)],
)
def test_request_timeout_limits(event, mocker: MockerFixture, timeout, raises):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    if raises:
        pytest.raises(ValueError, api.post, '', timeout=timeout)
        return

    api.post('', timeout=timeout)

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['timeout'] == timeout
