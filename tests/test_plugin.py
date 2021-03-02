import pytest

from corva import Corva
from corva.configuration import SETTINGS
from corva.models.scheduled import ScheduledEvent
from corva.models.stream import StreamEvent


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
def test_patch_stream(event, expected, raises, corva_context):
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
def test_patch_scheduled(event, expected, corva_context):
    corva = Corva(corva_context)

    actual_event = corva.scheduled(app, event)[0]

    assert actual_event == ScheduledEvent(**expected)
