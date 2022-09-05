import logging
import re

import pytest
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
            event.dict(
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
        ).dict(
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
        ).dict(
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
        ).dict(
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

    event = event.copy(update={'schedule_start': value})
    app_event = (
        type(event)
        .parse_obj(
            event.dict(
                by_alias=True,
                exclude_unset=True,
            )
        )
        .dict(
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
            ).dict(
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
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=int(),
                schedule=int(),
                schedule_start=int(),
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).dict(
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
                event.dict(
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
    ).dict(by_alias=True, exclude_unset=True)

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
    ).dict(by_alias=True, exclude_unset=True)

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    result_event: ScheduledNaturalTimeEvent = app(event, context)[0]

    assert result_event.rerun is not None  # for mypy to not complain.
    assert result_event.rerun.range.start == expected
    assert result_event.rerun.range.end == expected
