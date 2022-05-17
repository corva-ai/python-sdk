import logging

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
            platform_api_url='',
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
                'url': 'api/v1/whatever',
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
            platform_api_url='',
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
            "'https://test_url/api/v1/'\nFor more information check: "
            "https://httpstatuses.com/400",
            'response': {
                'code': 400,
                'reason': 'Bad Request',
                'headers': {},
                'content': "b''",
            },
            'request': {
                'method': 'GET',
                'url': 'https://test_url/api/v1/',
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