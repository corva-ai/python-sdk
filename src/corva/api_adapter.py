# What's needed?
# 1. Api SDK Request retries
# 2. Api SDK Request timeouts
# 3. Use api as standalone object
# 4. Request/Response error logging
import dataclasses
import functools
import json
import logging
import posixpath
from typing import Callable, List, Optional

import httpx
import yaml


def _httpx_headers_to_dict(headers: httpx.Headers) -> dict:
    return json.loads(
        repr(headers)  # use built-in `repr` as it obfuscates sensitive headers
        .strip("Headers()")  # strip obsolete data
        .replace(
            "'", '"'
        )  # replace single quotes with double ones to get proper json string
    )


def _failed_request_msg(
    msg: str,
    request: httpx.Request,
    response: Optional[httpx.Response],
) -> str:
    data = {"message": f"Request failed - {msg}"}

    if response:
        # log response first, so there is less chance it gets truncated
        # and users are able to see server error message
        data["response"] = {
            "code": response.status_code,
            "reason": response.reason_phrase,
            "headers": _httpx_headers_to_dict(response.headers),
            "content": str(response.content),
        }

    data["request"] = {
        "method": request.method,
        "url": str(request.url),
        "headers": _httpx_headers_to_dict(request.headers),
        "content": str(request.content),
    }

    # use yaml because it is much more readable in logs
    return yaml.dump(data, sort_keys=False)


def logging_send(func: Callable, *, logger: logging.Logger) -> Callable:
    @functools.wraps(func)
    def wrapper(request: httpx.Request, *args, **kwargs):
        try:
            response = func(request, *args, **kwargs)
        except httpx.HTTPError as exc:
            # Response was not received at all
            logger.error(
                _failed_request_msg(msg=str(exc), request=request, response=None)
            )
            raise

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # Response has unsuccessful status
            logger.error(
                _failed_request_msg(msg=str(exc), request=request, response=response)
            )

        return response

    return wrapper


# ===============  User stuff  ===============

# ================================================


class DataApiV1Sdk:
    def __init__(self, client: httpx.Client):
        self.http = client

    def get(
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
        """Fetches data from the endpoint GET 'data/{provider}/{dataset}/'.

        Args:
            provider: company name owning the dataset.
            dataset: dataset name.
            query: search conditions. Example: {"asset_id": 123} - will fetch data
                for asset with id 123.
            sort: sort conditions. Example: {"timestamp": 1} - will sort data
                in ascending order by timestamp.
            limit: number of data points to fecth.
                Recommendation for setting the limit:
                    1. The bigger ↑ each data point is - the smaller ↓ the limit;
                    2. The smaller ↓ each data point is - the bigger ↑ the limit.
            skip: exclude from the response the first N items of the dataset.
            fields: comma separated list of fields to return. Example: "_id,data".

        Raises:
            requests.HTTPError: if request was unsuccessful.

        Returns:
            Data from dataset.
        """

        response = self.http.get(
            url=f"data/{provider}/{dataset}/",
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

    def get_one(self):
        ...

    def get_aggregate(self):
        ...

    def get_aggregate_pipeline(self):
        ...

    def set(self):
        ...

    def delete(self):
        ...

    def update(self):
        ...

    def update_partial(self):
        ...


class PlatformApiV1Sdk:
    def __init__(self, client: httpx.Client):
        self.http = client

    # .....
    # other high-level methods here
    # .....


class PlatformApiV2Sdk:
    def __init__(self, client: httpx.Client):
        self.http = client

    # .....
    # other high-level methods here
    # .....


# ================================================


@dataclasses.dataclass(frozen=True)
class PlatformApiVersions:
    v1: PlatformApiV1Sdk
    v2: PlatformApiV2Sdk


@dataclasses.dataclass(frozen=True)
class DataApiVersions:
    v1: DataApiV1Sdk


class UserApiSdk:
    def __init__(
        self,
        platform_api_url: str,
        data_api_url: str,
        api_key: str,
        app_key: str,
        logger: logging.Logger,
        timeout: int = 30,
    ):
        self._platform_api_url = platform_api_url
        self._data_api_url = data_api_url
        self._headers = {
            "Authorization": f"API {api_key}",
            "X-Corva-App": app_key,
        }
        self._logger = logger
        self._timeout = timeout

    def __enter__(self):
        data_cli = httpx.Client(
            base_url=posixpath.join(self._data_api_url, "api/v1"),
            headers=self._headers,
            timeout=self._timeout,
        )
        platform_v1_cli = httpx.Client(
            base_url=posixpath.join(self._platform_api_url, "v1"),
            headers=self._headers,
            timeout=self._timeout,
        )
        platform_v2_cli = httpx.Client(
            base_url=posixpath.join(self._platform_api_url, "v2"),
            headers=self._headers,
            timeout=self._timeout,
        )

        data_cli.send = logging_send(func=data_cli.send, logger=self._logger)
        platform_v1_cli.send = logging_send(
            func=platform_v1_cli.send, logger=self._logger
        )
        platform_v2_cli.send = logging_send(
            func=platform_v2_cli.send, logger=self._logger
        )

        self.data = DataApiVersions(v1=DataApiV1Sdk(client=data_cli))
        self.platform = PlatformApiVersions(
            v1=PlatformApiV1Sdk(client=platform_v1_cli),
            v2=PlatformApiV2Sdk(client=platform_v2_cli),
        )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data.v1.http.close()
        self.platform.v1.http.close()
        self.platform.v2.http.close()


# with UserApiSdk(..., ..., ..., ..., ...) as sdk:
#     sdk.data.v1.http.get()
#     ...


def app(api: UserApiSdk):
    # Objective: users should get clear, easy to use Api interface without low level details that
    # they currently get in api object.

    # Currently users get a low level api adapter:
    #   - adapter contains low-level functions of no use for user
    #   - adapter shuffles functions for all apis (not clear which one is called)
    #   - user needs to specify api and version through path param

    # New api sdk:
    #   - has "dot" acess to api type and version (e.g., api.data.v1..., api.platform.v1...)
    #   - has frequently used api method represented as functions (e.g., api.data.v1.get_dataset)
    #   - not frequently used api method can be called using http client (e.g., api.data.v1.http.get...)
    #   - has correct type hints for all methods (intead of *args and **kwargs currently used)

    # Platform api calls

    # before
    #   - not clear which api is called
    api.get("/v1/data/provider/dataset")
    # after
    api.platform.v1.http.get("data/provider/dataset")

    # before
    #   - not clear which api is called
    api.post("/v2/rigs")
    # after
    api.platform.v2.http.post("rigs")

    # Specialized functions

    # before
    #   - not clear which api is called
    api.get_dataset()
    # after
    api.data.v1.get_dataset()

    # Data api calls

    # before
    #   - not clear which api is called
    api.delete("/api/v1/data/provider/dataset/")
    # after
    api.data.v1.http.delete("data/provider/dataset/")
