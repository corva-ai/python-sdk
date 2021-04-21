import pytest
from pytest_mock import MockerFixture

from corva.configuration import SETTINGS
from corva.handlers import stream
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
from corva.models.stream.stream import StreamEvent


@pytest.mark.parametrize('attr', ('asset_id', 'company_id'))
@pytest.mark.parametrize('value', (1, 2))
def test_set_attr_in_raw_stream_event(attr, value, context, mocker: MockerFixture):
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
