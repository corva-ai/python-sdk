import os
import re
from typing import List, Optional

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from corva import settings


class Api:
    ALLOWED_METHODS = {'GET', 'POST', 'PATCH', 'PUT', 'DELETE'}

    def __init__(
         self,
         api_url: str = settings.API_ROOT_URL,
         data_api_url: str = settings.DATA_API_ROOT_URL,
         api_key: str = settings.API_KEY,
         app_name: str = settings.APP_NAME,
         timeout: int = 600,
         max_retries: int = 3
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.app_name = app_name
        self.session = self._init_session(
            api_key=api_key, app_name=app_name, max_retries=max_retries, allowed_methods=list(self.ALLOWED_METHODS)
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
         data: Optional[dict] = None,  # request body
         params: Optional[dict] = None,  # url query string params
         headers: Optional[dict] = None,  # additional headers to include in request
         max_retries: Optional[int] = None,  # custom value for max number of retries
         timeout: Optional[int] = None,  # request timeout in seconds
    ) -> Response:

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
