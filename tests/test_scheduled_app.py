import re
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.models.scheduled import RawScheduledEvent


@pytest.mark.parametrize(
    'status_code',
    (200, 400),
    ids=('request successful', 'request failed - should not raise'),
)
def test_set_completed_status(status_code, context, requests_mock):
    def scheduled_app(event, api, state):
        # patch post request, that sets scheduled task as completed
        # looks for url path like /scheduler/123/completed
        requests_mock.post(
            re.compile(r'/scheduler/\d+/completed'), status_code=status_code
        )

        api.post = Mock(wraps=api.post)  # spy on api.post

        return api

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    api = Corva(context).scheduled(scheduled_app, event)[0]

    api.post.assert_called_once_with(path='scheduler/0/completed')


@pytest.mark.parametrize(
    'value,expected',
    (
        # 31 December 9999 23:59:59 in sec
        [253402300799, 253402300799],
        # 1 January 10000 00:00:00 in sec
        [253402300800, 253402300],
        # 1 January 2021 00:00:00 in ms
        [1609459200000, 1609459200],
        # 1 January 2021 00:00:00 in sec
        [1609459200, 1609459200],
    ),
    ids=(
        'no cast performed',
        'casted from ms to sec',
        'casted from ms to sec',
        'no cast performed',
    ),
)
def test_set_schedule_start(value, expected, context, mocker: MockerFixture):
    def app(event, api, state):
        return event

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=value,
                app_connection=int(),
                app_stream=int(),
            ).dict(by_alias=True, exclude_unset=True, exclude_defaults=True)
        ]
    ]

    # override scheduled_runner to return event from context
    mocker.patch(
        'corva.application.scheduled_runner', lambda fn, context: context.event
    )

    result_event: RawScheduledEvent = Corva(context).scheduled(app, event)[0]

    assert result_event.schedule_start == expected


@pytest.mark.parametrize(
    'schedule_start,interval,expected',
    (
        [2, 1, 2],
        [2, 2, 1],
    ),
)
def test_set_time_from(
    schedule_start, interval, expected, context, mocker: MockerFixture
):
    def app(event, api, state):
        return event

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=interval,
                schedule=int(),
                schedule_start=schedule_start,
                app_connection=int(),
                app_stream=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    # override scheduled_runner to return event from context
    mocker.patch(
        'corva.application.scheduled_runner', lambda fn, context: context.event
    )

    event = Corva(context).scheduled(app, event)[0]

    assert event.time_from == expected
