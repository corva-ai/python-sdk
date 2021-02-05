import pytest


def test_default_headers(api):
    assert not {'Authorization', 'X-Corva-App'} - set(api.session.headers)


def test_get_url(api):
    path = '/api/v10/data'
    assert api._get_url(path=path) == f'{api.data_api_url}{path}'

    path = 'api/v10/data'
    assert api._get_url(path=path) == f'{api.data_api_url}/{path}'

    path = 'api/v10/message_producer'
    assert api._get_url(path=path) == f'{api.data_api_url}/{path}'

    path = 'api/v10/path'
    assert api._get_url(path=path) == f'{api.api_url}/{path}'


def test_request_invalid_method(api):
    method = 'random'
    with pytest.raises(ValueError) as exc:
        api._request(method=method, path='random')
    assert str(exc.value) == f'Invalid HTTP method {method}.'
