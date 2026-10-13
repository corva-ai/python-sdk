import contextvars
import datetime
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import cast
from unittest.mock import MagicMock

import freezegun
import pytest
from pytest_mock import MockerFixture

from corva import Logger
from corva.configuration import SETTINGS
from corva.handlers import scheduled, stream, task
from corva.logger import LoggingContext
from corva.models.context import CorvaContext
from corva.models.scheduled.raw import RawScheduledDataTimeEvent, RawScheduledEvent
from corva.models.scheduled.scheduler_type import SchedulerType
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


@pytest.mark.parametrize(
    'max_message_size,expected',
    [
        (68, '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, W ...\n'),
        (6, '2 ...\n'),
        (5, ''),
        (4, ''),
    ],
)
def test_long_message_gets_truncated(
    max_message_size, expected, mocker: MockerFixture, context, capsys
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

    mocker.patch.object(SETTINGS, 'LOG_THRESHOLD_MESSAGE_SIZE', max_message_size)

    with freezegun.freeze_time(datetime.datetime(2021, 1, 2, 3, 4, 5, 678910)):
        app([event.dict()], context)

    assert capsys.readouterr().out == expected


@pytest.mark.parametrize(
    'max_message_count, expected',
    [
        (
            0,
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | '
            'Disabling the logging as maximum number of logged messages '
            'was reached: 0.\n',
        ),
        (
            1,
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | Hello, World!\n'
            '2021-01-02T03:04:05.678Z qwerty WARNING ASSET=0 AC=1 | '
            'Disabling the logging as maximum number of logged messages '
            'was reached: 1.\n',
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


def test_lambda_exceptions_are_logged(context, capsys, mocker: MockerFixture):
    @task
    def app(event, api):
        pass

    raw_event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        CorvaContext,
        'from_aws',
        side_effect=Exception('test_task_logging2'),
    )

    with pytest.raises(Exception, match=r'^test_task_logging2$'):
        app(raw_event, context)

    captured = capsys.readouterr()

    assert 'The app failed to execute.' in captured.out


def test_lambda_exceptions_are_logged_using_custom_log_handler(
    context, capsys, mocker: MockerFixture
):
    @task(handler=logging.StreamHandler())
    def app(event, api):
        pass

    raw_event = RawTaskEvent(task_id='0', version=2).dict()

    mocker.patch.object(
        CorvaContext,
        'from_aws',
        side_effect=Exception('test_task_logging2'),
    )

    with pytest.raises(Exception, match=r'^test_task_logging2$'):
        app(raw_event, context)

    captured = capsys.readouterr()

    assert 'The app failed to execute.' in captured.out
    assert 'The app failed to execute.' in captured.err


@pytest.mark.parametrize("cls_name", ("opentelemetry", "otel"))
def test__otel_handler_passed_to_logging_context__success(monkeypatch, cls_name):

    otel_handler = MagicMock()
    otel_handler.__class__.__name__ = cls_name

    # Attach to the root logger
    logging.getLogger().addHandler(otel_handler)

    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")

    try:
        with LoggingContext(
            aws_request_id=MagicMock(),
            asset_id=MagicMock(),
            app_connection_id=MagicMock(),
            handler=MagicMock(),
            user_handler=MagicMock(),
            logger=MagicMock(),
        ) as context:
            assert otel_handler in context.handlers
    finally:
        logging.getLogger().removeHandler(otel_handler)


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(f"{record.aws_request_id}:{record.asset_id}:{record.getMessage()}")


@pytest.fixture
def isolated_logger():
    logger = logging.getLogger("test-logging-context-isolated")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield logger
    logger.handlers = []


def _logging_context(request_id: str, asset_id: int, logger: logging.Logger):
    return LoggingContext(
        aws_request_id=request_id,
        asset_id=asset_id,
        app_connection_id=None,
        handler=_ListHandler(),
        user_handler=None,
        logger=logger,
    )


def _lines(ctx: LoggingContext) -> list[str]:
    return cast(_ListHandler, ctx.handler).lines


@pytest.mark.parametrize("a_exits_first", (False, True))
def test_concurrent_logging_contexts_do_not_mix(isolated_logger, a_exits_first):
    """Contexts entered in different threads must not affect each other."""
    a_entered, b_entered, a_logged, a_exited, b_logged = (
        threading.Event() for _ in range(5)
    )
    ctx_a = _logging_context("A", 1, isolated_logger)
    ctx_b = _logging_context("B", 2, isolated_logger)

    def thread_a():
        with ctx_a:
            isolated_logger.info("a-before")
            a_entered.set()
            assert b_entered.wait(5)
            isolated_logger.info("a-during")
            a_logged.set()
            if not a_exits_first:
                assert b_logged.wait(5)
        a_exited.set()

    def thread_b():
        assert a_entered.wait(5)
        with ctx_b:
            b_entered.set()
            assert a_logged.wait(5)
            isolated_logger.info("b-during")
            if a_exits_first:
                assert a_exited.wait(5)
                isolated_logger.info("b-after-a-exited")
            b_logged.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        for future in [pool.submit(thread_a), pool.submit(thread_b)]:
            future.result(timeout=15)

    # nothing is left attached to the logger after the contexts exit
    isolated_logger.info("outside")

    assert _lines(ctx_a) == ["A:1:a-before", "A:1:a-during"]
    assert _lines(ctx_b) == ["B:2:b-during"] + (
        ["B:2:b-after-a-exited"] if a_exits_first else []
    )


def test_nested_logging_contexts_restore_outer_context(isolated_logger):
    outer = _logging_context("outer", 1, isolated_logger)
    inner = _logging_context("inner", 2, isolated_logger)

    with outer:
        isolated_logger.info("1")
        with inner:
            isolated_logger.info("2")
        isolated_logger.info("3")

    assert _lines(outer) == ["outer:1:1", "outer:1:3"]
    assert _lines(inner) == ["inner:2:2"]


def test_logging_context_is_used_by_threads_started_inside_it(isolated_logger):
    """Threads spawned by app code do not inherit contextvars, but still log."""
    ctx = _logging_context("req", 1, isolated_logger)

    with ctx:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(lambda: isolated_logger.info("from-worker")).result(timeout=5)

    assert _lines(ctx) == ["req:1:from-worker"]


def test_logging_context_propagates_to_copied_context_threads(isolated_logger):
    ctx_a = _logging_context("A", 1, isolated_logger)
    ctx_b = _logging_context("B", 2, isolated_logger)

    with ctx_a:
        copied = contextvars.copy_context()
        with ctx_b:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(
                    lambda: copied.run(isolated_logger.info, "from-a-worker")
                ).result(timeout=5)

    assert _lines(ctx_a) == ["A:1:from-a-worker"]
    assert _lines(ctx_b) == []


def test_logging_context_asset_id_can_be_updated_inside_context(isolated_logger):
    ctx = _logging_context("req", 1, isolated_logger)

    with ctx:
        ctx.asset_id = 5
        isolated_logger.info("msg")

    assert _lines(ctx) == ["req:5:msg"]
