import logging
import re
from copy import deepcopy

import pytest
import redis
import requests_mock as requests_mock_lib
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from corva import Logger
from corva.handlers import scheduled
from corva.models.rerun import RerunTime, RerunTimeRange
from corva.models.scheduled.raw import (
    RawScheduledDataTimeEvent,
    RawScheduledDepthEvent,
    RawScheduledEvent,
    RawScheduledNaturalTimeEvent,
)
from corva.models.scheduled.scheduled import (
    ScheduledDataTimeEvent,
    ScheduledEvent,
    ScheduledNaturalTimeEvent,
)
from corva.models.scheduled.scheduler_type import SchedulerType


def test_set_completed_status(context, requests_mock):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    event = [
        [
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).model_dump(
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
    'event, post_called',
    [
        pytest.param(
            RawScheduledNaturalTimeEvent(
                asset_id=int(),
                company=int(),
                schedule=int(),
                app_connection=int(),
                app_stream=int(),
                scheduler_type=SchedulerType.natural_time,
                schedule_start=int(),
                interval=int(),
            ),
            True,
            id='Set status as completed for failed natural time app.',
        ),
        pytest.param(
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ),
            False,
            id='Does not set status as completed for failed data time app.',
        ),
        pytest.param(
            RawScheduledDepthEvent(
                asset_id=int(),
                depth_milestone=float(),
                schedule=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_depth_milestone,
                top_depth=0.0,
                bottom_depth=1.0,
                log_identifier='',
            ),
            False,
            id='Does not set status as completed for failed depth app.',
        ),
    ],
)
def test_set_completed_status_for_failed_apps(
    event: RawScheduledEvent, post_called: bool, context, requests_mock
):
    @scheduled
    def scheduled_app(event, api, state):
        raise Exception

    app_event = [
        [
            event.model_dump(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    # patch post request, that sets scheduled task as completed
    # looks for url path like /scheduler/123/completed
    post_mock = requests_mock.post(re.compile(r'/scheduler/\d+/completed'))

    with pytest.raises(Exception):
        scheduled_app(app_event, context)

    if post_called:
        assert post_mock.called_once
        assert post_mock.last_request.path == '/scheduler/0/completed'
    else:
        assert not post_mock.called_once


@pytest.mark.parametrize(
    'event',
    (
        RawScheduledDataTimeEvent(
            asset_id=int(),
            interval=int(),
            schedule=int(),
            schedule_start=int(),
            app_connection=int(),
            app_stream=int(),
            company=int(),
            scheduler_type=SchedulerType.data_time,
        ).model_dump(
            by_alias=True,
            exclude_unset=True,
        ),
        RawScheduledDepthEvent(
            asset_id=int(),
            depth_milestone=float(),
            schedule=int(),
            app_connection=int(),
            app_stream=int(),
            company=int(),
            scheduler_type=SchedulerType.data_depth_milestone,
            top_depth=0.0,
            bottom_depth=1.0,
            log_identifier='',
        ).model_dump(
            by_alias=True,
            exclude_unset=True,
        ),
        RawScheduledNaturalTimeEvent(
            asset_id=int(),
            interval=int(),
            schedule=int(),
            app_connection=int(),
            app_stream=int(),
            company=int(),
            scheduler_type=SchedulerType.natural_time,
            schedule_start=int(),
        ).model_dump(
            by_alias=True,
            exclude_unset=True,
        ),
    ),
)
@pytest.mark.parametrize('is_dict', (True, False))
def test_event_parsing(event, is_dict, requests_mock: RequestsMocker, context):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    if not is_dict:
        event = [[event]]

    requests_mock.post(requests_mock_lib.ANY)

    scheduled_app(event, context)


@pytest.mark.parametrize(
    'event,attr',
    (
        [
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ),
            'end_time',
        ],
        [
            RawScheduledNaturalTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.natural_time,
            ),
            'schedule_start',
        ],
    ),
)
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
def test_set_schedule_start(
    event: RawScheduledEvent, attr: str, value, expected, context, mocker: MockerFixture
):
    @scheduled
    def app(e, api, state):
        return e

    event = event.model_copy(update={'schedule_start': value})
    app_event = (
        type(event)
        .model_validate(
            event.model_dump(
                by_alias=True,
                exclude_unset=True,
            )
        )
        .model_dump(
            by_alias=True,
            exclude_unset=True,
        )
    )

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledEvent = app(app_event, context)[0]

    assert getattr(result_event, attr) == expected


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
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=interval,
                schedule=int(),
                schedule_start=schedule_start,
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).model_dump(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledDataTimeEvent = app(event, context)[0]

    assert result_event.start_time == expected


def test_set_completed_status_should_not_fail_lambda(context, mocker: MockerFixture):
    @scheduled
    def scheduled_app(event, api, state):
        pass

    event = [
        [
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).model_dump(
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
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).model_dump(
                by_alias=True,
                exclude_unset=True,
            )
        ]
    ]

    patch = mocker.patch.object(
        RawScheduledEvent, 'set_schedule_as_completed', side_effect=Exception('Oops!')
    )

    scheduled_app(event, context)

    captured = capsys.readouterr()

    assert 'ASSET=0 AC=0' in captured.out
    assert 'Could not set schedule as completed. Details: Oops!.' in captured.out
    patch.assert_called_once()


def test_custom_log_handler(context, capsys, mocker: MockerFixture):
    @scheduled(handler=logging.StreamHandler())
    def app(event, api, cache):
        Logger.info('Info message!')

    event = RawScheduledDataTimeEvent(
        asset_id=0,
        interval=int(),
        schedule=int(),
        schedule_start=int(),
        app_connection=1,
        app_stream=int(),
        company=int(),
        scheduler_type=SchedulerType.data_time,
    )

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    app(
        [
            [
                event.model_dump(
                    by_alias=True,
                    exclude_unset=True,
                )
            ]
        ],
        context,
    )

    captured = capsys.readouterr()

    assert captured.out.endswith('Info message!\n')
    assert captured.err == 'Info message!\n'


@pytest.mark.parametrize(
    'time, expected',
    (
        # 1 January 2021 00:00:00 in ms
        pytest.param(1609459200000, 1609459200, id='casted from ms to sec'),
        # 1 January 2021 00:00:00 in sec
        pytest.param(1609459200, 1609459200, id='no cast performed'),
    ),
)
def test_rerun_data_time_cast_from_ms_to_s(
    time: int, expected: int, context, mocker: MockerFixture
):
    @scheduled
    def app(event, api, state):
        return event

    event = RawScheduledDataTimeEvent(
        asset_id=0,
        company=10,
        schedule=20,
        app_connection=30,
        app_stream=40,
        scheduler_type=SchedulerType.data_time,
        schedule_start=50,
        interval=60,
        rerun=RerunTime(
            range=RerunTimeRange(start=time, end=time), invoke=70, total=80
        ),
    ).model_dump(by_alias=True, exclude_unset=True)

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledDataTimeEvent = app(event, context)[0]

    assert result_event.rerun is not None  # for mypy to not complain.
    assert result_event.rerun.range.start == expected
    assert result_event.rerun.range.end == expected


@pytest.mark.parametrize(
    'time, expected',
    (
        # 1 January 2021 00:00:00 in ms
        pytest.param(1609459200000, 1609459200, id='casted from ms to sec'),
        # 1 January 2021 00:00:00 in sec
        pytest.param(1609459200, 1609459200, id='no cast performed'),
    ),
)
def test_rerun_natural_time_cast_from_ms_to_s(
    time: int, expected: int, context, mocker: MockerFixture
):
    @scheduled
    def app(event, api, state):
        return event

    event = RawScheduledNaturalTimeEvent(
        asset_id=0,
        company=10,
        schedule=20,
        app_connection=30,
        app_stream=40,
        scheduler_type=SchedulerType.data_time,
        schedule_start=50,
        interval=60,
        rerun=RerunTime(
            range=RerunTimeRange(start=time, end=time), invoke=70, total=80
        ),
    ).model_dump(by_alias=True, exclude_unset=True)

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledNaturalTimeEvent = app(event, context)[0]

    assert result_event.rerun is not None  # for mypy to not complain.
    assert result_event.rerun.range.start == expected
    assert result_event.rerun.range.end == expected


def test_cache_connection_limit(requests_mock: RequestsMocker, context):
    """
    provided Cache object can't init more than one connection
    """

    event = RawScheduledDataTimeEvent(
        asset_id=int(),
        interval=int(),
        schedule=int(),
        schedule_start=int(),
        app_connection=int(),
        app_stream=int(),
        company=int(),
        scheduler_type=SchedulerType.data_time,
    ).model_dump(
        by_alias=True,
        exclude_unset=True,
    )

    @scheduled
    def scheduled_app(event, api, cache):
        pool = cache.cache_repo.client.connection_pool

        # unique single conn
        pool.get_connection()

        # Should be an error here since `max_connections=1` is passed to a Redis client
        pool.get_connection()

    requests_mock.post(requests_mock_lib.ANY)

    with pytest.raises(redis.exceptions.ConnectionError):
        scheduled_app(event, context)


@pytest.mark.parametrize("merge_events", [True, False])
def test_merge_events_parameter(merge_events, context, mocker):
    @scheduled(merge_events=merge_events)
    def scheduled_app(event, api, state):
        # For this test, just return the processed event list for inspection.
        return event

    # Create two separate events with different schedule_start values.
    # Note: schedule_start is provided in milliseconds.
    event1 = RawScheduledDataTimeEvent(
        asset_id=1,
        interval=60,  # interval value in seconds
        schedule=123,
        schedule_start=1744718400000,  # 2025-04-15T12:00:00 in milliseconds
        schedule_end=1744718460000,  # 2025-04-15T12:01:00 in milliseconds
        app_connection=1,
        app_stream=2,
        company=1,
        scheduler_type=SchedulerType.data_time,
    ).model_dump(by_alias=True, exclude_unset=True)

    event2 = RawScheduledDataTimeEvent(
        asset_id=1,
        interval=60,
        schedule=124,
        schedule_start=1744718460000,  # 2025-04-15T12:01:00 in milliseconds
        schedule_end=1744718520000,  # 2025-04-15T12:02:00 in milliseconds
        app_connection=1,
        app_stream=2,
        company=1,
        scheduler_type=SchedulerType.data_time,
    ).model_dump(by_alias=True, exclude_unset=True)
    original_event1 = deepcopy(event1)

    # Combine the events in the input structure
    events = [[event1, event2]]

    # Call the scheduled app which should process the events.
    result = scheduled_app(events, context)

    if merge_events:
        # When merging is enabled, the app should merge the input events
        # into a single event.
        assert (
            len(result) == 1
        ), "Expected a single merged event when merge_events is true."
        merged_event = result[0]

        # Calculate the expected start_time and end_time.
        expected_start_time = (
            original_event1["schedule_start"] - original_event1["interval"] + 1
        )
        expected_end_time = event2["schedule_start"]

        assert (
            merged_event.start_time == expected_start_time
        ), f"Expected time {expected_start_time}, got {merged_event.start_time}."
        # The merged event is expected to have an 'end_time' attribute set.
        actual_end_time = getattr(merged_event, "end_time", None)
        assert (
            actual_end_time == expected_end_time
        ), f"Expected merged end_time {expected_end_time}, got {actual_end_time}."
    else:
        # When merging is disabled, the app should return the events as-is
        # (i.e. two separate events).
        assert (
            len(result) == 2
        ), "Expected two separate events when merge_events is false."
