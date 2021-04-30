import contextlib
from types import SimpleNamespace

import pydantic
import pytest
import requests_mock as requests_mock_lib
from requests_mock import Mocker as RequestsMocker

from corva.handlers import task
from corva.models.task import RawTaskEvent, TaskEvent


@task
def task_app(event, api):
    return api.api_key


@pytest.mark.parametrize(
    'aws_event,aws_context,exc_ctx,expected',
    (
        [
            RawTaskEvent(task_id='', version=2).dict(),
            SimpleNamespace(
                aws_request_id='', client_context=SimpleNamespace(env={'API_KEY': ''})
            ),
            contextlib.nullcontext(),
            '',
        ],
        [
            RawTaskEvent(
                task_id='', version=2, client_context={'env': {'API_KEY': ''}}
            ).dict(),
            SimpleNamespace(aws_request_id='', client_context=None),
            contextlib.nullcontext(),
            '',
        ],
        [
            RawTaskEvent(
                task_id='', version=2, client_context={'env': {'API_KEY': '0'}}
            ).dict(),
            SimpleNamespace(
                aws_request_id='', client_context=SimpleNamespace(env={'API_KEY': '1'})
            ),
            contextlib.nullcontext(),
            '1',
        ],
        [
            RawTaskEvent(task_id='', version=2).dict(),
            SimpleNamespace(aws_request_id='', client_context=None),
            pytest.raises(pydantic.ValidationError),
            '',
        ],
    ),
    ids=(
        'Lambda context has not None `client_context`. Lambda event has no `client_context`.',
        'Lambda context has None `client_context`. Lambda event has `client_context`.',
        'Lambda context has not None `client_context`. Lambda event has `client_context`.',
        'No `client_context` in Lambda context and event.',
    ),
)
def test_context(
    aws_event, aws_context, exc_ctx, expected, requests_mock: RequestsMocker
):
    requests_mock.request(
        requests_mock_lib.ANY,
        requests_mock_lib.ANY,
        json=TaskEvent(asset_id=0, company_id=0).dict(),
    )

    with exc_ctx:
        assert task_app(aws_event, aws_context)[0] == expected
