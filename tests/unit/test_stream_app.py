import logging

import pytest
from pytest_mock import MockerFixture

from corva import Logger
from corva.configuration import SETTINGS
from corva.handlers import stream
from corva.models.rerun import RerunTime, RerunTimeRange
from corva.models.stream.log_type import LogType
from corva.models.stream.raw import (
    RawAppMetadata,
    RawDepthRecord,
    RawMetadata,
    RawStreamDepthEvent,
    RawStreamEvent,
    RawStreamTimeEvent,
    RawTimeRecord,
)
from corva.models.stream.stream import StreamDepthEvent, StreamEvent, StreamTimeEvent


@pytest.mark.parametrize('attr', ('asset_id', 'company_id'))
@pytest.mark.parametrize('value', (1, 2))
def test_set_attr_in_raw_stream_event(attr, value, context):
    @stream
    def stream_app(event, api, cache):
        return event

    event = [
        RawStreamTimeEvent(
            records=[
                RawTimeRecord(
                    collection=str(),
                    timestamp=int(),
                    **{'asset_id': int(), 'company_id': int(), **{attr: value}},
                )
            ],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    result_event: StreamEvent = stream_app(event, context)[0]

    assert getattr(result_event, attr) == value


@pytest.mark.parametrize(
    'last_value,event,expected',
    [
        (
            -1,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=int(),
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection='wits.completed',
                            timestamp=int(),
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
            [
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    timestamp=int(),
                )
            ],
        ),
        (
            -1,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection='wits.completed',
                            timestamp=int(),
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=int(),
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
            [
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection='wits.completed',
                    timestamp=int(),
                ),
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    timestamp=int(),
                ),
            ],
        ),
        (
            None,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=int(),
                        )
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
            [
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    timestamp=int(),
                )
            ],
        ),
        (
            1,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=0,
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=1,
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=2,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
            [
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    timestamp=2,
                )
            ],
        ),
        (
            1,
            [
                RawStreamDepthEvent(
                    records=[
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=0,
                        ),
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=1,
                        ),
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=2,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.depth,
                    ),
                ).dict()
            ],
            [
                RawDepthRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    measured_depth=2,
                ),
            ],
        ),
    ],
    ids=[
        'is completed record last - should be filtered',
        'is completed record not last - should not be filtered',
        'last value is None - filtering doesnt fail',
        'time event filtered',
        'depth event filtered',
    ],
)
def test_filter_records(last_value, event, expected, mocker: MockerFixture, context):
    @stream
    def stream_app(event, api, cache):
        pass

    spy = mocker.spy(RawStreamEvent, 'filter_records')
    mocker.patch.object(
        RawStreamEvent, 'get_cached_max_record_value', return_value=last_value
    )

    stream_app(event, context)

    assert spy.spy_return == expected


def test_early_return_if_no_records_after_filtering(mocker: MockerFixture, context):
    @stream
    def stream_app(event, api, cache):
        pass

    event = [
        RawStreamTimeEvent(
            records=[
                RawTimeRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    timestamp=int(),
                )
            ],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    filter_patch = mocker.patch.object(
        RawStreamEvent, 'filter_records', return_value=[]
    )
    spy = mocker.Mock(stream_app, wraps=stream_app)

    stream_app(event, context)

    filter_patch.assert_called_once()
    spy.assert_not_called()


@pytest.mark.parametrize(
    'expected,event',
    [
        (
            1,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=0,
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=1,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
        ),
        (
            2,
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=0,
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=1,
                        ),
                        RawTimeRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            timestamp=2,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.time,
                    ),
                ).dict()
            ],
        ),
        (
            1,
            [
                RawStreamDepthEvent(
                    records=[
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=0,
                        ),
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=1,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.depth,
                    ),
                ).dict()
            ],
        ),
        (
            2,
            [
                RawStreamDepthEvent(
                    records=[
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=0,
                        ),
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=1,
                        ),
                        RawDepthRecord(
                            asset_id=int(),
                            company_id=int(),
                            collection=str(),
                            measured_depth=2,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=int(),
                        apps={
                            SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())
                        },
                        log_type=LogType.depth,
                    ),
                ).dict()
            ],
        ),
    ],
)
def test_last_processed_value_saved_to_cache(
    expected, event, context, mocker: MockerFixture
):
    @stream
    def stream_app(event, api, cache):
        pass

    spy = mocker.spy(RawStreamEvent, 'get_cached_max_record_value')
    stream_app(event * 2, context)

    assert spy.call_count == 2
    assert spy.spy_return == expected


def test_set_cached_max_record_value_should_not_fail_lambda(
    mocker: MockerFixture, context
):
    @stream
    def stream_app(event, api, cache):
        pass

    event = [
        RawStreamTimeEvent(
            records=[
                RawTimeRecord(
                    collection=str(),
                    timestamp=int(),
                    asset_id=int(),
                    company_id=int(),
                )
            ],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    patch = mocker.patch.object(
        RawStreamEvent, 'set_cached_max_record_value', side_effect=Exception
    )

    stream_app(event, context)

    patch.assert_called_once()


def test_log_if_unable_to_set_cached_max_record_value(
    mocker: MockerFixture, context, capsys
):
    @stream
    def stream_app(event, api, cache):
        pass

    event = [
        RawStreamTimeEvent(
            records=[
                RawTimeRecord(
                    collection=str(),
                    timestamp=int(),
                    asset_id=int(),
                    company_id=int(),
                )
            ],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    patch = mocker.patch.object(
        RawStreamEvent, 'set_cached_max_record_value', side_effect=Exception('Oops!')
    )

    stream_app(event, context)

    captured = capsys.readouterr()

    assert 'ASSET=0 AC=0' in captured.out
    assert 'Could not save data to cache. Details: Oops!.' in captured.out
    patch.assert_called_once()


def test_custom_log_handler(context, capsys):
    @stream(handler=logging.StreamHandler())
    def app(event, api, cache):
        Logger.info('Info message!')

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

    app([event.dict()], context)

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
def test_rerun_time_cast_from_ms_to_s(time: int, expected: int, context):
    @stream
    def app(event, api, cache):
        return event

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
        rerun=RerunTime(
            range=RerunTimeRange(start=time, end=time), invoke=10, total=20
        ),
    )

    result_event: StreamTimeEvent = app([event.dict()], context)[0]

    assert result_event.rerun is not None  # for mypy to not complain.
    assert result_event.rerun.range.start == expected
    assert result_event.rerun.range.end == expected


def test_stream_depth_app_gets_log_identifier(context):
    """Stream depth apps must receive `log_identifier` field."""

    @stream
    def stream_app(event, api, cache):
        return event

    event = [
        RawStreamDepthEvent(
            records=[
                RawDepthRecord(
                    asset_id=int(),
                    company_id=int(),
                    collection=str(),
                    measured_depth=float(),
                )
            ],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.depth,
                log_identifier='log_identifier',
            ),
        ).dict()
    ]

    result_event: StreamDepthEvent = stream_app(event, context)[0]

    assert result_event.log_identifier == 'log_identifier'


def test_raw_stream_event_with_none_data_field_returns_expected_result(context):
    """Make sure that raw stream event with empty data field
    can be handled without validation exception.
    """

    @stream
    def stream_app(event, api, cache):
        return event

    event = [
        {
            "metadata": {
                "app_stream_id": 123,
                "apps": {"test-provider.test-app-name": {"app_connection_id": 456}},
                "log_type": "time",
                "source_type": "drilling",
            },
            "records": [
                {
                    "app": "corva.wits-historical-import",
                    "asset_id": 1,
                    "collection": "wits.completed",
                    "company_id": 80,
                    "data": None,
                    "provider": "corva",
                    "timestamp": 1688999883,
                    "version": 1,
                }
            ],
        }
    ]

    result_event: StreamTimeEvent = stream_app(event, context)[0]

    assert result_event is None
