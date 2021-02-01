import os
import re
from typing import List, Optional

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class Api:
    """Provides a convenient way to access Corva API and Corva Data API

    Api is a thin wrapper around `requests` library that handles
     authorization, adds timeouts and retries to request.
    """

    ALLOWED_METHODS = {'GET', 'POST', 'PATCH', 'PUT', 'DELETE'}
    DEFAULT_TIMEOUT = 600
    DEFAULT_MAX_RETRIES = 3

    def __init__(
         self,
         api_url: str,
         data_api_url: str,
         api_key: str,
         app_name: str,
         timeout: Optional[int] = None,
         max_retries: Optional[int] = None
    ):
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.app_name = app_name
        self.session = self._init_session(
            api_key=api_key,
            app_name=app_name,
            max_retries=self.max_retries,
            allowed_methods=list(self.ALLOWED_METHODS)
        )

    @staticmethod
    def _init_session(api_key: str, app_name: str, max_retries: int, allowed_methods: List[str]):
        session = Session()

        session.headers.update({
            'Authorization': f'API {api_key}',
            'X-Corva-App': app_name
        })
        session.mount(
            'https://',
            HTTPAdapter(
                max_retries=Retry(
                    total=max_retries,
                    status_forcelist=[408, 429, 500, 502, 503, 504],
                    allowed_methods=allowed_methods,
                    backoff_factor=0.3,
                    raise_on_status=False
                )
            )
        )

        return session

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

    def _get_url(self, path: str):
        # search text like api/v1/data or api/v1/message_producer in path
        if bool(re.search(r'api/v\d+/(data|message_producer)', path)):
            base_url = self.data_api_url
        else:
            base_url = self.api_url

        return os.path.join(base_url.strip('/'), path.strip('/'))

    def _request(
         self,
         method: str,
         path: str,
         data: Optional[dict] = None,
         params: Optional[dict] = None,
         headers: Optional[dict] = None,
         max_retries: Optional[int] = None,
         timeout: Optional[int] = None
    ) -> Response:
        """Executes the request

        params:
         method: HTTP method
         path: url to call
         data: request body
         params: url query string params
         headers: additional headers to include in request
         max_retries: custom value for max number of retries
         timeout: request timeout in seconds
        returns: response
        raises: HTTPError for unsuccessful requests
        """

        if method not in self.ALLOWED_METHODS:
            raise ValueError(f'Invalid HTTP method {method}.')

        max_retries = max_retries or self.max_retries
        timeout = timeout or self.timeout

        # not thread safe
        self.session.adapters['https://'].max_retries.total = max_retries

        response = self.session.request(
            method=method,
            url=self._get_url(path=path),
            params=params,
            json=data,
            headers=headers,
            timeout=timeout
        )

        response.raise_for_status()

        return response
