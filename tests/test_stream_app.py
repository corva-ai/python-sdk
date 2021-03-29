import pydantic
import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.configuration import SETTINGS
from corva.models.stream import (
    BaseStreamContext,
    LogType,
    RawAppMetadata,
    RawDepthRecord,
    RawMetadata,
    RawStreamDepthEvent,
    RawStreamEvent,
    RawStreamTimeEvent,
    RawTimeRecord,
)


def test_require_at_least_one_record_in_raw_stream_event(context):
    def stream_app(event, api, cache):
        pass

    event = [
        RawStreamTimeEvent.construct(
            records=[],
            metadata=RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    exc = pytest.raises(
        pydantic.ValidationError,
        Corva(context=context).stream,
        stream_app,
        event,
    )

    assert len(exc.value.raw_errors) == 1
    assert str(exc.value.raw_errors[0].exc) == 'At least one record should be provided.'


@pytest.mark.parametrize('attr', ('asset_id', 'company_id'))
@pytest.mark.parametrize('value', (1, 2))
def test_set_attr_in_raw_stream_event(attr, value, context, mocker: MockerFixture):
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

    mocker.patch('corva.application.stream_runner', lambda fn, context: context.event)

    result_event: RawStreamTimeEvent = Corva(context=context).stream(stream_app, event)[
        0
    ]

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
    def stream_app(event, api, cache):
        pass

    spy = mocker.spy(RawStreamEvent, 'filter_records')
    mocker.patch.object(BaseStreamContext, 'get_last_value', return_value=last_value)

    Corva(context=context).stream(stream_app, event)

    assert spy.spy_return == expected


@pytest.mark.parametrize(
    'app_key,raises',
    (['', True], [SETTINGS.APP_KEY, False]),
    ids=['no app key exc', 'correct event'],
)
def test_require_app_key_in_metadata_apps(app_key: str, raises, context):
    def stream_app(event, api, cache):
        pass

    event = [
        RawStreamTimeEvent.construct(
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
                apps={app_key: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            ),
        ).dict()
    ]

    corva = Corva(context=context)

    if raises:
        exc = pytest.raises(pydantic.ValidationError, corva.stream, stream_app, event)
        assert len(exc.value.raw_errors) == 1
        assert (
            str(exc.value.raw_errors[0].exc)
            == 'metadata.apps dict must contain an app key.'
        )
        return

    corva.stream(stream_app, event)


def test_early_return_if_no_records_after_filtering(mocker: MockerFixture, context):
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

    Corva(context=context).stream(stream_app, event)

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
    def stream_app(event, api, cache):
        pass

    spy = mocker.spy(BaseStreamContext, 'get_last_value')
    Corva(context=context).stream(stream_app, event)
    Corva(context=context).stream(stream_app, event)

    assert spy.call_count == 2
    assert spy.spy_return == expected
