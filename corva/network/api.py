import posixpath
import re
from typing import Optional

import requests
import requests.adapters
import urllib3


class Api(requests.Session):
    """Provides a convenient way to access Corva API and Corva Data API

    Api is a thin wrapper around `requests.Session` that adds
     authorization, timeouts and retries to requests.
    """

    MAX_RETRIES = 3
    TIMEOUT = 30  # seconds

    def __init__(
        self,
        api_url: str,
        data_api_url: str,
        api_key: str,
        app_name: str,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
    ):
        super().__init__()

        self.timeout = timeout or self.TIMEOUT
        self.max_retries = max_retries or self.MAX_RETRIES
        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.app_name = app_name

        self.headers.update(
            {
                'Authorization': f'API {api_key}',
                'X-Corva-App': app_name,
            }
        )

        self.mount(
            'https://',
            requests.adapters.HTTPAdapter(
                max_retries=urllib3.Retry(
                    total=self.max_retries,
                    status_forcelist=[408, 429, 500, 502, 503, 504],
                    backoff_factor=0.3,
                    raise_on_redirect=False,
                    raise_on_status=False,
                )
            ),
        )

    def _get_url(self, suffix: str) -> str:
        """Builds complete url from base prefix and suffix

        returns:
         1 suffix param, if suffix is a complete url
         2 data api url, if suffix contains data api url pattern
         3 corva api url, if above points are not True
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

    def request(
        self,
        method: str,
        url: str,
        data: Optional[dict] = None,
        max_retries: Optional[int] = None,
        **kwargs,
    ) -> requests.Response:
        """Executes the request

        params:
         method: HTTP method
         url: url to call
         data: request body (will be automatically casted to json)
         max_retries: custom value for max number of retries
        raises:
         requests.HTTPError for unsuccessful request without retries
         urllib3.exceptions.MaxRetryError for unsuccessful request with retries
        """

        max_retries = max_retries or self.max_retries
        kwargs.setdefault('timeout', self.timeout)

        if data is not None:  # Corva endpoints work only with json data
            kwargs['json'] = data

        # not thread safe
        self.adapters['https://'].max_retries.total = max_retries

        response = super().request(method, self._get_url(url), **kwargs)

        return response
