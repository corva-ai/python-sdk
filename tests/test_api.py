import json
from typing import Literal
from unittest import mock

import pytest
import requests
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva.application import Corva
from corva.configuration import SETTINGS


@pytest.fixture(scope='function')
def event():
    return {"records": [{"asset_id": 0, "timestamp": 0}]}


def app(event, api, cache):
    return api


def test_request_default_headers(event, mocker: MockerFixture, corva_context):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=corva_context).stream(app, event)[0]

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
    event,
    mocker: MockerFixture,
    path,
    url,
    type_: Literal['data', 'corva', ''],
    corva_context,
):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=corva_context).stream(app, event)[0]

    api.get(path)

    expected = url

    if type_ == 'data':
        expected = url % SETTINGS.DATA_API_ROOT_URL

    if type_ == 'corva':
        expected = url % SETTINGS.API_ROOT_URL

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['url'] == expected


def test_request_data_param_passed_as_json(event, mocker: MockerFixture, corva_context):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=corva_context).stream(app, event)[0]

    api.post('', data={})

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['json'] == {}


def test_request_additional_headers(event, mocker: MockerFixture, corva_context):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=corva_context).stream(app, event)[0]

    api.post('', headers={'custom': 'value'})

    request_patch.assert_called_once()
    assert len(request_patch.call_args.kwargs['headers']) == 3
    assert request_patch.call_args.kwargs['headers']['custom'] == 'value'


@pytest.mark.parametrize(
    'timeout, raises',
    [(3, False), (30, False), (2, True), (31, True)],
)
def test_request_timeout_limits(
    event, mocker: MockerFixture, timeout, raises, corva_context
):
    request_patch = mocker.patch('requests.request')

    api = Corva(context=corva_context).stream(app, event)[0]

    if raises:
        pytest.raises(ValueError, api.post, '', timeout=timeout)
        return

    api.post('', timeout=timeout)

    request_patch.assert_called_once()
    assert request_patch.call_args.kwargs['timeout'] == timeout


@pytest.mark.parametrize(
    'fields,skip,limit,query,sort',
    ([None, 0, 1, {}, {}], ['_id', 1, 2, {'k1': 'v1'}, {'k2': 'v2'}]),
)
def test_get_dataset(
    fields,
    skip,
    limit,
    query,
    sort,
    event,
    corva_context,
    requests_mock: RequestsMocker,
    mocker: MockerFixture,
):
    api = Corva(context=corva_context).stream(app, event)[0]

    provider = SETTINGS.PROVIDER
    dataset = 'dataset'

    get_spy = mocker.spy(api, 'get')
    get_mock = requests_mock.get(f'/api/v1/data/{provider}/{dataset}/', text='[{}]')

    result = api.get_dataset(
        provider=provider,
        dataset=dataset,
        query=query,
        sort=sort,
        limit=limit,
        skip=skip,
        fields=fields,
    )

    expected = mock.call(
        f'/api/v1/data/{provider}/{dataset}/',
        params={
            'query': json.dumps(query),
            'sort': json.dumps(sort),
            'fields': fields,
            'limit': limit,
            'skip': skip,
        },
    )

    assert get_spy.call_args == expected
    assert get_mock.called_once
    assert result == [{}]


def test_get_dataset_raises(event, corva_context, requests_mock: RequestsMocker):
    api = Corva(context=corva_context).stream(app, event)[0]

    provider = SETTINGS.PROVIDER
    dataset = 'dataset'

    get_mock = requests_mock.get(
        f'/api/v1/data/{provider}/{dataset}/',
        status_code=400,
    )

    pytest.raises(
        requests.HTTPError,
        api.get_dataset,
        provider=provider,
        dataset=dataset,
        query={},
        sort={},
        limit=1,
    )

    assert get_mock.called_once
