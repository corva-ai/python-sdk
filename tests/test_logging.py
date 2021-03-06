import datetime

import freezegun
import pytest
from pytest_mock import MockerFixture

from corva import Logger
from corva.configuration import SETTINGS
from corva.handlers import scheduled, stream, task
from corva.models.scheduled import RawScheduledEvent
from corva.models.stream.log_type import LogType
from corva.models.stream.raw import (
    RawAppMetadata,
    RawMetadata,
    RawStreamEvent,
    RawStreamTimeEvent,
    RawTimeRecord,
)
from corva.models.task import RawTaskEvent, TaskEvent


def test_scheduled_logging(context, capsys, mocker: MockerFixture):
    @scheduled
    def app(event, api, cache):
        Logger.warning('Hello, World!')

    event = RawScheduledEvent(
        asset_id=0,
        interval=int(),
        schedule=int(),
        schedule_start=int(),
        app_connection=1,
        app_stream=int(),
        company=int(),
    )

    mocker.patch.object(RawScheduledEvent, 'set_schedule_as_completed')

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
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

    assert (
        capsys.readouterr().out
        == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} AC={event.app_connection_id} | Hello, World!\n'
    )


def test_stream_logging(context, capsys):
    @stream
    def app(event, api, cache):
        Logger.warning('Hello, World!')

    event = RawStreamTimeEvent(
        records=[
            RawTimeRecord(
                asset_id=0,
                company_id=int(),
                collection=str(),
                timestamp=int(),
            ),
        ],
        metadata=RawMetadata(
            app_stream_id=int(),
            apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
            log_type=LogType.time,
        ),
    )

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()], context)

    assert (
        capsys.readouterr().out
        == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} AC={event.app_connection_id} | Hello, World!\n'
    )


def test_task_logging(context, capsys, mocker: MockerFixture):
    @task
    def app(event, api):
        Logger.warning('Hello, World!')

    raw_event = RawTaskEvent(task_id='0', version=2).dict()
    event = TaskEvent(asset_id=0, company_id=int())

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=TaskEvent(asset_id=0, company_id=int()),
    )
    mocker.patch.object(RawTaskEvent, 'update_task_data')

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app(raw_event, context)

    captured = capsys.readouterr()

    assert (
        captured.out == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} | Hello, World!\n'
    )


@pytest.mark.parametrize(
    'log_level,expected',
    (
        ['WARN', ''],
        ['INFO', '2021-01-02T03:04:05.678Z qwerty INFO ASSET=0 AC=1 | Info message.\n'],
    ),
)
def test_custom_log_level(log_level, expected, context, capsys, mocker: MockerFixture):
    @stream
    def app(event, api, cache):
        Logger.debug('Debug message.')
        Logger.info('Info message.')

    event = RawStreamTimeEvent(
        records=[
            RawTimeRecord(
                asset_id=0,
                company_id=int(),
                collection=str(),
                timestamp=int(),
            ),
        ],
        metadata=RawMetadata(
            app_stream_id=int(),
            apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
            log_type=LogType.time,
        ),
    )

    mocker.patch.object(SETTINGS, 'LOG_LEVEL', log_level)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()], context)

    assert capsys.readouterr().out == expected


def test_each_app_invoke_has_separate_logger(context, capsys, mocker: MockerFixture):
    @stream
    def app(event, api, cache):
        Logger.warning('Hello, World!')
        Logger.warning('This should not be printed as logging is disabled.')

    event = RawStreamTimeEvent(
        records=[
            RawTimeRecord(
                asset_id=0,
                company_id=int(),
                collection=str(),
                timestamp=int(),
            ),
        ],
        metadata=RawMetadata(
            app_stream_id=int(),
            apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
            log_type=LogType.time,
        ),
    )

    # disable filtering, as app won't be invoked the second time
    mocker.patch.object(RawStreamEvent, 'filter_records', return_value=event.records)
    # first app invoke will reach the limit.
    # so, if logger is shared, second invoke will log nothing.
    mocker.patch.object(SETTINGS, 'LOG_THRESHOLD_MESSAGE_COUNT', 1)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()] * 2, context)

    expected = (
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, World!\n'
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Disabling the '
        'logging as maximum number of logged messages was reached: 1.\n'
    ) * 2

    assert capsys.readouterr().out == expected


def test_long_message_gets_truncated(mocker: MockerFixture, context, capsys):
    @stream
    def app(event, api, cache):
        Logger.warning('Hello, World!')

    event = RawStreamTimeEvent(
        records=[
            RawTimeRecord(
                asset_id=0,
                company_id=int(),
                collection=str(),
                timestamp=int(),
            ),
        ],
        metadata=RawMetadata(
            app_stream_id=int(),
            apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
            log_type=LogType.time,
        ),
    )

    mocker.patch.object(SETTINGS, 'LOG_THRESHOLD_MESSAGE_SIZE', 68)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()], context)

    expected = '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, W ...\n'

    assert capsys.readouterr().out == expected


@pytest.mark.parametrize(
    'max_message_count, expected',
    [
        (
            0,
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | '
            'Disabling the logging as maximum number of logged messages was reached: 0.\n',
        ),
        (
            1,
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, World!\n'
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | '
            'Disabling the logging as maximum number of logged messages was reached: 1.\n',
        ),
    ],
)
def test_max_message_count_reached(
    max_message_count, expected, mocker: MockerFixture, context, capsys
):
    @stream
    def app(event, api, cache):
        Logger.warning('Hello, World!')

    event = RawStreamTimeEvent(
        records=[
            RawTimeRecord(
                asset_id=0,
                company_id=int(),
                collection=str(),
                timestamp=int(),
            ),
        ],
        metadata=RawMetadata(
            app_stream_id=int(),
            apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
            log_type=LogType.time,
        ),
    )

    mocker.patch.object(SETTINGS, 'LOG_THRESHOLD_MESSAGE_COUNT', max_message_count)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()], context)

    assert capsys.readouterr().out == expected
