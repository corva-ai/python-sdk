import pytest
import requests_mock as requests_mock_lib
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva.application import Corva
from corva.configuration import SETTINGS
from corva.models.scheduled import RawScheduledEvent, ScheduledEvent
from corva.models.stream import StreamEvent
from corva.models.task import TaskEvent


def app(event, api, cache):
    return event


@pytest.mark.parametrize(
    'event,expected,raises',
    (
        [
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
            },
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
                "metadata": {
                    'app_stream_id': int(),
                    'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                },
            },
            False,
        ],
        [
            [
                {
                    "records": [{"asset_id": 0, "timestamp": 0}],
                }
            ],
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
                "metadata": {
                    'app_stream_id': int(),
                    'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                },
            },
            False,
        ],
        [
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
                "metadata": {
                    'app_stream_id': 1,
                    'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                },
            },
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
                "metadata": {
                    'app_stream_id': 1,
                    'apps': {SETTINGS.APP_KEY: {'app_connection_id': int()}},
                },
            },
            False,
        ],
        [
            {
                "records": [{"asset_id": 0, "timestamp": 0}],
                "metadata": {},
            },
            {},
            True,
        ],
    ),
    ids=[
        'input event type: dict',
        'input event type: List[dict]',
        'metadata can be overwritten by user',
        'no extra fields added to user metadata',
    ],
)
def test_patch_stream_changes_event(event, expected, raises, corva_context):
    corva = Corva(corva_context)

    if raises:
        pytest.raises(Exception, corva.stream, app, event)
        return

    actual_event = corva.stream(app, event)[0]

    assert actual_event == StreamEvent(**expected)


@pytest.mark.parametrize(
    'event,expected',
    (
        [
            {
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
            {
                "app_connection": 0,
                "app_stream": 0,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
        ],
        [
            [
                {
                    "schedule": 0,
                    "interval": 0,
                    "schedule_start": 0,
                    "schedule_end": 0,
                    "asset_id": 0,
                }
            ],
            {
                "app_connection": 0,
                "app_stream": 0,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
        ],
        [
            [
                [
                    {
                        "schedule": 0,
                        "interval": 0,
                        "schedule_start": 0,
                        "schedule_end": 0,
                        "asset_id": 0,
                    }
                ]
            ],
            {
                "app_connection": 0,
                "app_stream": 0,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
        ],
        [
            {
                "app_connection": 1,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
            {
                "app_connection": 1,
                "app_stream": 0,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
        ],
        [
            {
                "app_stream": 1,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
            {
                "app_connection": 0,
                "app_stream": 1,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            },
        ],
    ),
    ids=[
        'input event type: dict',
        'input event type: List[dict]',
        'input event type: List[List[dict]]',
        'app_connection can be overwritten by user',
        'app_stream can be overwritten by user',
    ],
)
def test_patch_scheduled_changes_event(event, expected, corva_context):
    corva = Corva(corva_context)

    actual_event = corva.scheduled(app, event)[0]

    assert actual_event == RawScheduledEvent(**expected)


@pytest.mark.parametrize(
    '_patch_scheduled,is_patched',
    ([True, True], [False, False]),
    indirect=['_patch_scheduled'],
)
def test_patch_scheduled_runner_param(
    _patch_scheduled, is_patched, requests_mock: RequestsMocker, corva_context
):
    event = {
        "schedule": 0,
        "interval": 0,
        "schedule_start": 0,
        "asset_id": 0,
    }

    corva = Corva(corva_context)

    post_mock = requests_mock.post(requests_mock_lib.ANY)

    corva.scheduled(app, event)

    if is_patched:
        assert not post_mock.called
    else:
        assert post_mock.called_once


def test_task_app_runner(app_runner):
    """Should not raise."""

    def lambda_handler(event, context):
        return Corva(context).task(fn=lambda event, api: 'Task app result', event=event)

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(lambda_handler, event) == 'Task app result'


@pytest.mark.parametrize(
    'asset_id',
    (
        1,
        2,
    ),
)
def test_scheduled_app_runner_sets_correct_asset_id(
    asset_id, app_runner, mocker: MockerFixture
):
    def lambda_handler(event, context):
        return Corva(context).scheduled(fn=lambda event, api, cache: None, event=event)

    # override scheduled_runner to return event from context
    mocker.patch(
        'corva.application.scheduled_runner', lambda fn, context: context.event
    )

    event = ScheduledEvent(asset_id=asset_id, time_from=int(), time_to=int())

    raw_event = app_runner(lambda_handler, event)

    assert raw_event.asset_id == event.asset_id


def test_scheduled_app_runner(app_runner):
    """Should not raise."""

    def lambda_handler(event, context):
        return Corva(context).scheduled(
            fn=lambda event, api, cache: 'Scheduled app result', event=event
        )

    event = ScheduledEvent(asset_id=int(), time_from=int(), time_to=int())

    assert app_runner(lambda_handler, event) == 'Scheduled app result'
