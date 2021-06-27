import re

import pytest
from pytest_mock import MockerFixture

from corva.handlers import scheduled
from corva.models.scheduled import RawScheduledEvent, ScheduledEvent


def test_set_completed_status(context, requests_mock):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    # patch post request, that sets scheduled task as completed
    # looks for url path like /scheduler/123/completed
    post_mock = requests_mock.post(re.compile(r'/scheduler/\d+/completed'))

    scheduled_app(event, context)

    assert post_mock.called_once
    assert post_mock.last_request.path == '/scheduler/0/completed'


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
    @scheduled
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
                company=int(),
            ).dict(by_alias=True, exclude_unset=True, exclude_defaults=True)
        ]
    ]

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledEvent = app(event, context)[0]

    assert result_event.end_time == expected


@pytest.mark.parametrize(
    'schedule_start,interval,expected',
    (
        [2, 1, 2],
        [2, 2, 1],
    ),
)
def test_set_start_time(
    schedule_start, interval, expected, context, mocker: MockerFixture
):
    @scheduled
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
                company=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledEvent = app(event, context)[0]

    assert result_event.start_time == expected


def test_set_completed_status_should_not_fail_lambda(context, mocker: MockerFixture):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    patch = mocker.patch.object(
        RawScheduledEvent, 'set_schedule_as_completed', side_effect=Exception
    )

    scheduled_app(event, context)

    patch.assert_called_once()


def test_log_if_unable_to_set_completed_status(context, mocker: MockerFixture, capsys):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    event = [
        [
            RawScheduledEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
            ).dict(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    patch = mocker.patch.object(
        RawScheduledEvent, 'set_schedule_as_completed', side_effect=Exception
    )

    scheduled_app(event, context)

    captured = capsys.readouterr()

    assert 'ASSET=0 AC=0' in captured.out
    assert 'An exception occured while setting schedule as completed.' in captured.out
    patch.assert_called_once()
