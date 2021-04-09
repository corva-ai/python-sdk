import datetime

import freezegun
import pytest
from pytest_mock import MockerFixture

from corva import Corva, Logger
from corva.configuration import SETTINGS
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
    def app(event, api, cache):
        Logger.warning('Hello, World!')

    event = RawScheduledEvent(
        asset_id=0,
        interval=int(),
        schedule=int(),
        schedule_start=int(),
        app_connection=1,
        app_stream=int(),
    )

    mocker.patch('corva.runners.scheduled.set_schedule_as_completed')

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        Corva(context).scheduled(
            app,
            [
                [
                    event.dict(
                        by_alias=True,
                        exclude_unset=True,
                    )
                ]
            ],
        )

    assert (
        capsys.readouterr().out
        == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} AC={event.app_connection_id} | Hello, World!\n'
    )


def test_stream_logging(context, capsys):
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
        Corva(context).stream(
            app,
            [event.dict()],
        )

    assert (
        capsys.readouterr().out
        == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} AC={event.app_connection_id} | Hello, World!\n'
    )


def test_task_logging(context, capsys, mocker: MockerFixture):
    def app(event, api):
        Logger.warning('Hello, World!')

    raw_event = RawTaskEvent(task_id='0', version=2).dict()
    event = TaskEvent(asset_id=0, company_id=int())

    mocker.patch(
        'corva.runners.task.get_task_event',
        return_value=TaskEvent(asset_id=0, company_id=int()),
    )
    mocker.patch('corva.runners.task.update_task_data')

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        Corva(context).task(fn=app, event=raw_event)

    captured = capsys.readouterr()

    assert (
        captured.out == f'2021-01-02T03:04:05.678Z {context.aws_request_id} WARNING '
        f'ASSET={event.asset_id} | Hello, World!\n'
    )


def test_max_chars_reached(context, capsys, mocker: MockerFixture):
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

    mocker.patch.object(SETTINGS, 'LOG_MAX_CHARS', 68)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        Corva(context).stream(
            app,
            [event.dict()],
        )

    # exclamation point was cut from the message
    expected = (
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, World\n'
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Disabling the '
        'logging as maximum number of logged characters was reached: 68.\n'
    )

    assert capsys.readouterr().out == expected


@pytest.mark.parametrize(
    'log_level,expected',
    (
        ['WARN', ''],
        ['INFO', '2021-01-02T03:04:05.678Z qwerty INFO ASSET=0 AC=1 | Info message.\n'],
    ),
)
def test_custom_log_level(log_level, expected, context, capsys, mocker: MockerFixture):
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
        Corva(context).stream(
            app,
            [event.dict()],
        )

    assert capsys.readouterr().out == expected


def test_each_app_invoke_has_separate_logger(context, capsys, mocker: MockerFixture):
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
    mocker.patch.object(SETTINGS, 'LOG_MAX_CHARS', 68)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        Corva(context).stream(
            app,
            [event.dict()] * 2,
        )

    expected = (
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, World\n'
        '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Disabling the '
        'logging as maximum number of logged characters was reached: 68.\n'
    ) * 2

    assert capsys.readouterr().out == expected
