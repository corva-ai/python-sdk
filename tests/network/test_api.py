from unittest.mock import patch

from pytest import fixture
from requests import HTTPError
from requests import Response

from worker.network.api import Api


@fixture
def api():
    return Api(api_url='https://api', data_api_url='https://data')


def test_default_headers(api):
    assert not {'Authorization', 'X-Corva-App'} - set(api.session.headers)


def test_custom_request_exceptions(api):
    response = Response()
    asset_id = 123
    for status_code, error_msg in [
        (401, '401 Unable to reach Corva API.'),
        (403, f'403 No access to asset {asset_id}.')
    ]:
        response.status_code = status_code
        with patch.object(api.session, 'request', return_value=response):
            try:
                api.get('', n_retries=1, asset_id=asset_id)
            except HTTPError as e:
                assert str(e) == error_msg


def test_get_url(api):
    path = f'{api.api_url}/1'
    assert api._get_url(path=path) == path

    path = f'{api.data_api_url}/1'
    assert api._get_url(path=path) == path

    path = '/api/v10/data'
    assert api._get_url(path=path) == f'{api.data_api_url}{path}'

    path = 'api/v10/data'
    assert api._get_url(path=path) == f'{api.data_api_url}/{path}'

    path = 'api/v10/message_producer'
    assert api._get_url(path=path) == f'{api.data_api_url}/{path}'

    path = 'api/v10/path'
    assert api._get_url(path=path) == f'{api.api_url}/{path}'
