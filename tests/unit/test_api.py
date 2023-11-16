import contextlib
import json
import time
import urllib.parse
from http import HTTPStatus

import pytest
import requests
from requests_mock import Mocker as RequestsMocker

from corva.api import Api
from corva.configuration import SETTINGS
from corva.handlers import task
from corva.models.task import TaskEvent


@task
def app(event, api):
    return api


@pytest.fixture(scope="function")
def api(app_runner) -> Api:
    """Returns Api instance from task app."""

    event = TaskEvent(asset_id=int(), company_id=int())

    return app_runner(app, event)


def test_request_default_headers(api, requests_mock: RequestsMocker):
    # do some api call
    requests_mock.get(
        "",
        request_headers={
            "Authorization": f"API {api.api_key}",
            "X-Corva-App": api.app_key,
        },
    )
    api.get("")


@pytest.mark.parametrize(
    "path,expected",
    [
        ["http://localhost", "http://localhost"],
        ["api/v10/path", f"{SETTINGS.DATA_API_ROOT_URL}/api/v10/path"],
        ["/api/v10/path", f"{SETTINGS.DATA_API_ROOT_URL}/api/v10/path"],
        ["v10/path", f"{SETTINGS.API_ROOT_URL}/v10/path"],
        ["/v10/path", f"{SETTINGS.API_ROOT_URL}/v10/path"],
    ],
)
def test_request_url(path, expected, api, requests_mock: RequestsMocker):
    requests_mock.get(expected)
    api.get(path)


def test_request_data_param_passed_as_json(api, requests_mock: RequestsMocker):
    post_mock = requests_mock.post("")
    api.post("", data={})
    assert post_mock.last_request._request.body.decode() == "{}"


def test_request_additional_headers(api, requests_mock: RequestsMocker):
    custom_headers = {"custom": "value"}

    requests_mock.post("", request_headers={**api.default_headers, **custom_headers})
    api.post("", headers={"custom": "value"})


@pytest.mark.parametrize(
    "timeout, exc_ctx",
    [
        (3, contextlib.nullcontext()),
        (30, contextlib.nullcontext()),
        (2, pytest.raises(ValueError)),
        (31, pytest.raises(ValueError)),
    ],
)
def test_request_timeout_limits(timeout, exc_ctx, api, requests_mock: RequestsMocker):
    requests_mock.post("")

    with exc_ctx:
        api.post("", timeout=timeout)


@pytest.mark.parametrize(
    "fields,skip,limit,query,sort",
    (
        [None, 0, 1, {}, {}],
        [
            "_id",
            1,
            2,
            {"k1": "v1"},
            {"k2": "v2"},
        ],
    ),
)
def test_get_dataset(
    fields,
    skip,
    limit,
    query,
    sort,
    api,
    requests_mock: RequestsMocker,
):
    provider = SETTINGS.PROVIDER
    dataset = "dataset"

    qs = urllib.parse.urlencode(
        {
            "query": json.dumps(query),
            "sort": json.dumps(sort),
            **({"fields": fields} if fields else {}),
            "limit": limit,
            "skip": skip,
        }
    )

    requests_mock.get(
        f"/api/v1/data/{provider}/{dataset}/?{qs}", complete_qs=True, text="[{}]"
    )

    result = api.get_dataset(
        provider=provider,
        dataset=dataset,
        query=query,
        sort=sort,
        limit=limit,
        skip=skip,
        fields=fields,
    )

    assert result == [{}]


def test_get_dataset_raises(api, requests_mock: RequestsMocker):
    provider = SETTINGS.PROVIDER
    dataset = "dataset"

    requests_mock.get(
        f"/api/v1/data/{provider}/{dataset}/",
        status_code=400,
    )

    pytest.raises(
        requests.HTTPError,
        api.get_dataset,
        provider=provider,
        dataset=dataset,
        query={},
        sort={},
        limit=1,
    )


def test_retrying_logic_works_as_expected(api, requests_mock: RequestsMocker):
    path = "/"
    url = f"{SETTINGS.API_ROOT_URL}{path}"

    bad_requests_statuses_codes = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
    ]
    good_requests_statuses_codes = [HTTPStatus.OK]

    requests_sequence_return_status_codes = (
        bad_requests_statuses_codes + good_requests_statuses_codes
    )

    requests_mock.register_uri(
        "GET",
        url,
        [
            {"status_code": int(status_code)}
            for status_code in requests_sequence_return_status_codes
        ],
    )

    start_time = time.time()
    response = api.get(path)
    end_time = time.time()

    assert response.status_code in good_requests_statuses_codes
    assert len(requests_mock.request_history) == len(
        requests_sequence_return_status_codes
    )
    assert end_time - start_time > 1, (
        f"At least 1 second retry delay should be applied for "
        f"{len(bad_requests_statuses_codes)} retries."
    )


def test_retrying_logic_for_custom_max_retries_makes_n_calls(
    api, requests_mock: RequestsMocker
):
    custom_max_retries = 2
    path = "/"
    url = f"{SETTINGS.API_ROOT_URL}{path}"

    bad_requests_statuses_codes = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
    ]

    requests_mock.register_uri(
        "GET",
        url,
        [
            {"status_code": int(status_code)}
            for status_code in bad_requests_statuses_codes
        ],
    )

    assert len(bad_requests_statuses_codes) >= custom_max_retries

    api.max_retries = custom_max_retries
    api.get(path)

    assert (
        len(requests_mock.request_history) == custom_max_retries
    ), f"When all requests fail only {custom_max_retries} requests should be made."
