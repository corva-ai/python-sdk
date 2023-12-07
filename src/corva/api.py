import json
import posixpath
import re
from http import HTTPStatus
from typing import List, Optional, Sequence, Union

import requests
from tenacity import (
    RetryError,
    retry,
    retry_if_result,
    stop_after_attempt,
    wait_random_exponential,
)


class Api:
    """Provides a convenient way to access the Corva Platform API and Corva Data API.

    Api wraps the Python `requests` library and adds automatic authorization,
    convenient URL usage and reasonable timeouts to API requests.
    """

    TIMEOUT_LIMITS = (3, 30)  # seconds
    DEFAULT_MAX_RETRIES = int(0)

    def __init__(
        self,
        *,
        api_url: str,
        data_api_url: str,
        api_key: str,
        app_key: str,
        timeout: Optional[int] = None,
        app_connection_id: Optional[int] = None,
    ):
        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.app_key = app_key
        self.app_connection_id = app_connection_id
        self.timeout = timeout or self.TIMEOUT_LIMITS[1]
        self._max_retries = self.DEFAULT_MAX_RETRIES

    @property
    def default_headers(self):
        return {
            "Authorization": f"API {self.api_key}",
            "X-Corva-App": self.app_key,
        }

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @max_retries.setter
    def max_retries(self, value: int):
        if not (0 <= value <= 10):
            raise ValueError("Values between 0 and 10 are allowed")
        self._max_retries = value

    def get(self, path: str, **kwargs):
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs):
        return self._request("POST", path, **kwargs)

    def patch(self, path: str, **kwargs):
        return self._request("PATCH", path, **kwargs)

    def put(self, path: str, **kwargs):
        return self._request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs):
        return self._request("DELETE", path, **kwargs)

    def _get_url(self, path: str):
        """Builds complete url.

        Args:
          path: either complete or only path part of the HTTP URL.

        Returns:
          1 path param, if path is a complete url.
          2 data api url, if path contains data api url pattern.
          3 corva api url, if above points are False.
        """

        if path.startswith("http"):
            return path

        path = path.lstrip(
            "/"
        )  # delete leading forward slash for posixpath.join to work correctly

        # search text like api/v1 or api/v10 in path
        if bool(re.search(r"api/v\d+", path)):
            return posixpath.join(self.data_api_url, path)

        return posixpath.join(self.api_url, path)

    @staticmethod
    def _execute_request(
        method: str,
        url: str,
        params: Optional[dict],
        data: Optional[dict],
        headers: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
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
        return requests.request(
            method=method,
            url=url,
            params=params,
            json=data,
            headers=headers,
            timeout=timeout,
        )

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
        """Prepares HTTP request.

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
        retryable_status_codes = [
            HTTPStatus.TOO_MANY_REQUESTS,  # 428
            HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
            HTTPStatus.BAD_GATEWAY,  # 502
            HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            HTTPStatus.GATEWAY_TIMEOUT,  # 504
        ]

        timeout = timeout or self.timeout
        self._validate_timeout(timeout)

        url = self._get_url(path)

        headers = {
            **self.default_headers,
            **(headers or {}),
        }

        if self.max_retries > 0:
            retry_decorator = retry(
                stop=stop_after_attempt(self.max_retries),
                wait=wait_random_exponential(multiplier=0.25, max=10),
                retry=retry_if_result(
                    lambda r: r.status_code in retryable_status_codes
                ),
            )
            retrying_request = retry_decorator(self._execute_request)
            try:
                response = retrying_request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=headers,
                    timeout=timeout,
                )
            except RetryError as e:
                if not e.last_attempt.failed:
                    response = e.last_attempt.result()
                else:
                    raise
        else:
            response = self._execute_request(
                method=method,
                url=url,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
            )

        return response

    def _validate_timeout(self, timeout: int) -> None:
        if self.TIMEOUT_LIMITS[0] > timeout or self.TIMEOUT_LIMITS[1] < timeout:
            raise ValueError(
                f"Timeout must be between {self.TIMEOUT_LIMITS[0]} and "
                f"{self.TIMEOUT_LIMITS[1]} seconds."
            )

    def get_dataset(
        self,
        provider: str,
        dataset: str,
        *,
        query: dict,
        sort: dict,
        limit: int,
        skip: int = 0,
        fields: Optional[str] = None,
    ) -> List[dict]:
        """Fetches data from the endpoint '/api/v1/data/{provider}/{dataset}/'.

        Args:
          provider: company name, that owns the dataset.
          dataset: dataset name.
          query: search conditions. Example: {"asset_id": 123} - will fetch data
            for asset with id 123.
          sort: sort conditions. Example: {"timestamp": 1} - will sort data
            in ascending order by timestamp.
          limit: number of data points to fecth.
            Recommendation for setting the limit:
              1. The bigger ↑ each data point is - the smaller ↓ the limit;
              2. The smaller ↓ each data point is - the bigger ↑ the limit.
          skip: exclude from a response the first N items of a dataset.
          fields: comma separated list of fields to return. Example: "_id,data".

        Raises:
          requests.HTTPError: if request was unsuccessful.

        Returns:
          Data from dataset.
        """

        response = self.get(
            f"/api/v1/data/{provider}/{dataset}/",
            params={
                "query": json.dumps(query),
                "sort": json.dumps(sort),
                "fields": fields,
                "limit": limit,
                "skip": skip,
            },
        )
        response.raise_for_status()

        data = list(response.json())

        return data

    def produce_messages(self, data: Sequence[dict]) -> None:
        """Posts data to the endpoint '/api/v1/message_producer/'.

        Args:
          data: messages to post.
            Message examples:
            - time message [{"timestamp": 1}];
            - depth message [{"measured_depth": 1.0}].

        Raises:
          requests.HTTPError: if request was unsuccessful.
        """

        response = self.post(
            "/api/v1/message_producer/",
            data={"app_connection_id": self.app_connection_id, "data": data},
        )
        response.raise_for_status()

    def insert_data(
        self,
        provider: str,
        dataset: str,
        data: Sequence[dict],
        *,
        produce: bool = False,
    ) -> dict:
        """Posts data to the endpoint '/api/v1/data/{provider}/{dataset}/'.

        Args:
          provider: company name, that owns the dataset.
          dataset: dataset name.
          data: documents to insert.
          produce: whether to send data to message producer.

        Raises:
          requests.HTTPError: if request was unsuccessful.

        Returns: response dict.
        """

        body: Union[dict, List[dict]]  # make mypy happy

        if produce:
            body = {
                "data": list(data),
                "producer": {"app_connection_id": self.app_connection_id},
            }
        else:
            body = list(data)

        response = self.post(f"/api/v1/data/{provider}/{dataset}/", data=body)
        response.raise_for_status()

        return response.json()
