import contextlib
import datetime
import json
import logging
import posixpath
import uuid
from typing import Iterable

import httpx
import pytest
import pytest_httpx
import yaml

import corva.api_adapter
import corva.configuration


class TestLogsFailedRequests:
    def test_no_response(self, caplog: pytest.LogCaptureFixture):
        caplog.handler.setFormatter(logging.Formatter('%(message)s'))

        sdk = corva.api_adapter.UserApiSdk(
            platform_v1_url='',
            platform_v2_url='',
            data_api_url='',
            api_key='',
            app_key='',
            logger=logging.getLogger(),
        )

        with sdk as s, pytest.raises(httpx.HTTPError):
            s.data.v1.http.get(url='whatever')

        actual = yaml.safe_load(caplog.text)

        expected = {
            'message': "Request failed - Request URL is missing an 'http://' or "
            "'https://' protocol.",
            'request': {
                'method': 'GET',
                'url': '/whatever',
                'headers': {
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate',
                    'connection': 'keep-alive',
                    'user-agent': 'python-httpx/0.22.0',
                    'authorization': '[secure]',
                    'x-corva-app': '',
                },
                'content': "b''",
            },
        }

        assert expected == actual

    def test_unsuccessful_response(
        self, caplog: pytest.LogCaptureFixture, httpx_mock: pytest_httpx.HTTPXMock
    ):
        caplog.handler.setFormatter(logging.Formatter('%(message)s'))
        httpx_mock.add_response(status_code=400)

        sdk = corva.api_adapter.UserApiSdk(
            platform_v1_url='',
            platform_v2_url='',
            data_api_url="https://test_url",
            api_key='',
            app_key='',
            logger=logging.getLogger(),
        )

        with sdk as s:
            s.data.v1.http.get(url='')

        actual = yaml.safe_load(caplog.text)

        expected = {
            'message': "Request failed - Client error '400 Bad Request' for url "
            "'https://test_url/'\nFor more information check: "
            "https://httpstatuses.com/400",
            'response': {
                'code': 400,
                'reason': 'Bad Request',
                'headers': {},
                'content': "b''",
            },
            'request': {
                'method': 'GET',
                'url': 'https://test_url/',
                'headers': {
                    'host': 'test_url',
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate',
                    'connection': 'keep-alive',
                    'user-agent': 'python-httpx/0.22.0',
                    'authorization': '[secure]',
                    'x-corva-app': '',
                },
                'content': "b''",
            },
        }

        assert actual == expected


def vcr_before_record_request(request):
    request.uri = None

    return request


@pytest.fixture(scope="module")
def vcr_config():
    return {
        # Replace the Authorization request header
        "filter_headers": ["Authorization", "host"],
        "before_record_request": vcr_before_record_request,
    }


@pytest.fixture(scope='module')
def platform_v1_url() -> str:
    return posixpath.join(corva.configuration.SETTINGS.API_ROOT_URL, 'v1')


@pytest.fixture(scope='module')
def platform_v2_url() -> str:
    return posixpath.join(corva.configuration.SETTINGS.API_ROOT_URL, 'v2')


@pytest.fixture(scope='module')
def data_url() -> str:
    return posixpath.join(corva.configuration.SETTINGS.DATA_API_ROOT_URL, 'api/v1')


@pytest.fixture(scope='module')
def headers() -> dict:
    return {'Authorization': f'Bearer {corva.configuration.get_test_bearer()}'}


@pytest.fixture(scope='module')
def data(data_url: str, headers: dict) -> Iterable[httpx.Client]:
    with httpx.Client(base_url=data_url, headers=headers) as data:
        yield data


@pytest.fixture(scope='module')
def provider() -> str:
    return corva.configuration.SETTINGS.PROVIDER


@pytest.fixture(scope='module')
def dataset() -> str:
    return corva.configuration.get_test_dataset()


@pytest.fixture(scope='module')
def app_key() -> str:
    now = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0)
    return f'python-sdk-autotest-{now}'


@contextlib.contextmanager
def _setup(
    platform: httpx.Client, data: httpx.Client, provider: str, dataset: str
) -> Iterable[int]:
    response = platform.post(
        url='wells',
        json={
            'well': {'name': f'deleteme-python-sdk-autotest-{str(uuid.uuid4())[:8]}'}
        },
    )
    response.raise_for_status()
    well_id = int(response.json()['data']['id'])

    response = platform.get(url=f'wells/{well_id}?fields[]=well.asset_id')
    response.raise_for_status()
    asset_id: int = response.json()['data']['attributes']['asset_id']

    data.delete(
        f'data/{provider}/{dataset}/',
        params={'query': json.dumps({'asset_id': asset_id})},
    ).raise_for_status()

    try:
        yield asset_id
    finally:
        platform.delete(f'wells/{well_id}').raise_for_status()

        data.delete(
            f'data/{provider}/{dataset}/',
            params={'query': json.dumps({'asset_id': asset_id})},
        ).raise_for_status()


@pytest.fixture(scope='function')
def setup_(
    platform_v2_url: str, data: httpx.Client, headers: dict, provider: str, dataset: str
) -> Iterable[int]:
    with httpx.Client(base_url=platform_v2_url, headers=headers) as platform:
        yield _setup(platform=platform, data=data, provider=provider, dataset=dataset)


@pytest.fixture(scope='function')
def sdk(
    platform_v1_url: str, platform_v2_url: str, data_url: str, app_key: str
) -> Iterable[corva.api_adapter.UserApiSdk]:
    sdk = corva.api_adapter.UserApiSdk(
        platform_v1_url=platform_v1_url,
        platform_v2_url=platform_v2_url,
        data_api_url=data_url,
        api_key=corva.configuration.get_test_api_key(),
        app_key=app_key,
        logger=logging.getLogger(),
    )

    yield sdk


class TestUserApiSdk:
    @pytest.mark.vcr
    def test_get(
        self,
        setup_: Iterable[int],
        sdk: corva.api_adapter.UserApiSdk,
        dataset: str,
        provider: str,
        data: httpx.Client,
    ):
        with setup_ as asset_id:
            data.post(
                f'data/{provider}/{dataset}/',
                json=[
                    {
                        "asset_id": asset_id,
                        "version": 1,
                        "data": {"k": "v"},
                        "timestamp": 10,
                    },
                    {
                        "asset_id": asset_id,
                        "version": 1,
                        "data": {"k": "v"},
                        "timestamp": 12,
                    },
                    {
                        "asset_id": asset_id,
                        "version": 1,
                        "data": {"k": "v"},
                        "timestamp": 11,
                    },
                    {
                        "asset_id": asset_id,
                        "version": 1,
                        "data": {"k": "v"},
                        "timestamp": 13,
                    },
                ],
            ).raise_for_status()

            with sdk as s:
                collection = s.data.v1.get(
                    provider=provider,
                    dataset=dataset,
                    query={'asset_id': asset_id},
                    sort={'timestamp': 1},
                    limit=2,
                    skip=1,
                    fields='timestamp',
                )

        assert len(collection) == 2
        assert set(doc['timestamp'] for doc in collection) == {11, 12}

    @pytest.mark.vcr
    def test_insert(
        self,
        setup_: Iterable[int],
        sdk: corva.api_adapter.UserApiSdk,
        dataset: str,
        provider: str,
        data: httpx.Client,
    ):
        with setup_ as asset_id:
            response = data.get(
                url=f"data/{provider}/{dataset}/",
                params={
                    "query": json.dumps({'asset_id': asset_id}),
                    "sort": json.dumps({'timestamp': 1}),
                    "limit": 1,
                },
            )
            response.raise_for_status()
            collection = response.json()

            assert not collection

            with sdk as s:
                s.data.v1.insert(
                    provider=provider,
                    dataset=dataset,
                    documents=[
                        {
                            "asset_id": asset_id,
                            "version": 1,
                            "data": {"k": "v"},
                            "timestamp": 10,
                        },
                        {
                            "asset_id": asset_id,
                            "version": 1,
                            "data": {"k": "v"},
                            "timestamp": 11,
                        },
                    ],
                )

            response = data.get(
                url=f"data/{provider}/{dataset}/",
                params={
                    "query": json.dumps({'asset_id': asset_id}),
                    "sort": json.dumps({'timestamp': 1}),
                    "limit": 3,
                },
            )
            response.raise_for_status()
            collection = response.json()

        assert len(collection) == 2
