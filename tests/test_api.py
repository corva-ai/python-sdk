from typing import Literal

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.configuration import SETTINGS


@pytest.fixture(scope='function')
def event():
    return (
        '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
        '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}}]'
    ) % SETTINGS.APP_KEY


def app(event, api, cache):
    return api


def test_default_headers(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.get('')  # do some api call

    request_patch.assert_called_once()
    assert 'Authorization' in request_patch.call_args.kwargs['headers']
    assert 'X-Corva-App' in request_patch.call_args.kwargs['headers']


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
def test_get_url(
    event, mocker: MockerFixture, path, url, type_: Literal['data', 'corva']
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


def test_data_passed_as_json(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.post('', data={})

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['json'] == {}


@pytest.mark.parametrize('timeout', [None, 12345])
def test_timeout(event, mocker: MockerFixture, timeout):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.post('', timeout=timeout)
    request_patch.assert_called_once()

    assert timeout != api.timeout

    expected = timeout or api.timeout

    assert request_patch.call_args.kwargs['timeout'] == expected


def test_custom_headers(event, mocker: MockerFixture):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=None).stream(app, event)[0]

    api.post('', headers={'custom': 'value'})

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['headers']['custom'] == 'value'