import posixpath
import re
from typing import Optional

import requests


class Api:
    """Provides a convenient way to access Corva API and Corva Data API.

    Api is a thin wrapper around `requests` library that adds
    authorization, convenient url usage  and timeouts to requests.
    """

    TIMEOUT = 30  # seconds

    def __init__(
        self,
        *,
        api_url: str,
        data_api_url: str,
        api_key: str,
        app_name: str,
        timeout: Optional[int] = None,
    ):
        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.app_name = app_name
        self.timeout = timeout or self.TIMEOUT

    @property
    def auth_headers(self):
        return {
            'Authorization': f'API {self.api_key}',
            'X-Corva-App': self.app_name,
        }

    def get(self, path: str, **kwargs):
        return self._request('GET', path, **kwargs)

    def post(self, path: str, **kwargs):
        return self._request('POST', path, **kwargs)

    def patch(self, path: str, **kwargs):
        return self._request('PATCH', path, **kwargs)

    def put(self, path: str, **kwargs):
        return self._request('PUT', path, **kwargs)

    def delete(self, path: str, **kwargs):
        return self._request('DELETE', path, **kwargs)

    def _get_url(self, suffix: str):
        """Builds complete url.

        Args:
          path: either complete or only path part of the HTTP URL.

        Returns:
          1 path param, if path is a complete url.
          2 data api url, if path contains data api url pattern.
          3 corva api url, if above points are False.
        """

        if suffix.startswith('http'):
            return suffix

        suffix = suffix.lstrip(
            '/'
        )  # delete leading forward slash for posixpath.join to work correctly

        # search text like api/v1 or api/v10 in path
        if bool(re.search(r'api/v\d+', suffix)):
            return posixpath.join(self.data_api_url, suffix)

        return posixpath.join(self.api_url, suffix)

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        """Executes the request.

        Args:
          method: HTTP method.
          path: url to call.
          data: request body, that will be casted to json.
          params: url query string params.
          headers: additional headers to include in request.
          timeout: custom request timeout in seconds.

        Returns:
          requests.Response instance.
        """

        timeout = timeout or self.timeout

        headers = {
            **self.auth_headers,
            **(headers or {}),
        }

        response = requests.request(
            method=method,
            url=self._get_url(path),
            params=params,
            json=data,
            headers=headers,
            timeout=timeout,
        )

        return response
