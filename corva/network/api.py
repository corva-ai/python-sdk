import os
import re
from dataclasses import dataclass, field
from typing import Optional

from requests import HTTPError, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from corva import settings


@dataclass(eq=False)
class Api:
    HTTP_METHODS = {'GET', 'POST', 'PATCH', 'PUT', 'DELETE'}

    timeout: int = 600  # seconds
    max_retries: int = 3
    api_url: str = settings.API_ROOT_URL
    data_api_url: str = settings.DATA_API_ROOT_URL
    api_key: str = settings.API_KEY
    app_name: str = settings.APP_NAME
    session: Session = field(default_factory=Session)

    def __post_init__(self):
        self.session.headers.update({
            'Authorization': f'API {self.api_key}',
            'X-Corva-App': self.app_name
        })
        self.session.mount(
            'https://',
            HTTPAdapter(
                max_retries=Retry(
                    total=self.max_retries,
                    status_forcelist=[408, 429, 500, 502, 503, 504],
                    allowed_methods=list(self.HTTP_METHODS),
                    backoff_factor=0.3,
                    raise_on_status=False
                )
            )
        )

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
        if path.startswith(self.api_url) or path.startswith(self.data_api_url):
            return path

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
         data: Optional[dict] = None,  # form-encoded data
         json: Optional[dict] = None,  # auto encodes dict to json and adds Content-Type=application/json header
         params: Optional[dict] = None,  # url query string params
         headers: Optional[dict] = None,  # additional headers to include in request
         max_retries: Optional[int] = None,  # custom value for max number of retries
         timeout: Optional[int] = None,  # request timeout in seconds
         asset_id: Optional[int] = None,  # asset_id to use in error message
    ):
        if method not in self.HTTP_METHODS:
            raise ValueError(f'Invalid HTTP method {method}.')

        max_retries = max_retries or self.max_retries
        timeout = timeout or self.timeout

        # not thread safe
        self.session.adapters['https://'].max_retries.total = max_retries

        response = self.session.request(
            method=method,
            url=self._get_url(path=path),
            data=data,
            params=params,
            json=json,
            headers=headers,
            timeout=timeout
        )

        code_to_error = {
            401: '401 Unable to reach Corva API.',
            403: f'403 No access to asset {asset_id}.'
        }

        try:
            response.raise_for_status()
        except HTTPError as e:
            if (custom_error := code_to_error.get(response.status_code)) is not None:
                raise HTTPError(custom_error, response=response) from e
            raise

        return Result(
            response=response,
            **dict(
                data=data,
                json=json,
                params=params,
                headers=headers,
                max_retries=max_retries,
                timeout=timeout,
                asset_id=asset_id
            )
        )


class Result:
    def __init__(self, response, **kwargs):
        self.response = response
        self.params = kwargs
        self.data = None

        try:
            self.data = response.json()
        except ValueError as exc:
            raise ValueError('Invalid API response') from exc

    def __repr__(self):
        return repr(self.data)

    def __iter__(self):
        return iter(self.data)

    @property
    def status(self):
        return self.response.status_code

    @property
    def count(self):
        if isinstance(self.data, (list, set, tuple, dict)):
            return len(self.data)

        return 0
