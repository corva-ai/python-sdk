from types import SimpleNamespace

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.configuration import SETTINGS
from corva.models.stream import FilterMode, StreamContext, StreamEvent, StreamStateData


def stream_app(event, api, cache):
    return event


@pytest.mark.parametrize(
    'collection, is_completed',
    [('wits.completed', True), ('random', False)],
    ids=['is_completed True', 'is_completed False'],
)
def test_is_completed_record_deleted(collection, is_completed, mocker: MockerFixture):
    event = [
        {
            "records": [{"asset_id": 0, "collection": collection, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    spy = mocker.Mock(stream_app, wraps=stream_app)

    results = corva.stream(spy, event)

    if is_completed:
        spy.assert_not_called()
        return

    assert len(results[0].records) == 1
    assert not results[0].is_completed


@pytest.mark.parametrize(
    'filter_mode,record_attr,last_value,expected',
    [
        (None, 'timestamp', None, 2),
        (FilterMode.timestamp, 'timestamp', None, 2),
        (None, 'timestamp', 0, 2),
        (FilterMode.timestamp, 'timestamp', 0, 1),
        (FilterMode.depth, 'measured_depth', 0, 1),
    ],
    ids=[
        'filtering skipped',
        'filtering skipped',
        'filtering skipped',
        'filtering done',
        'filtering done',
    ],
)
def test_filter_records(
    filter_mode, record_attr, last_value, expected, mocker: MockerFixture
):
    def filter_records_decor(func):
        def decor(**kwargs):
            kwargs['filter_mode'] = filter_mode
            kwargs['last_value'] = last_value
            return func(**kwargs)

        return decor

    event = [
        {
            "records": [
                {record_attr: 0, "asset_id": 0},
                {record_attr: 1, "asset_id": 0},
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    mocker.patch.object(
        StreamEvent, 'filter_records', filter_records_decor(StreamEvent.filter_records)
    )
    results = corva.stream(stream_app, event, filter_mode=filter_mode)

    assert len(results[0].records) == expected


@pytest.mark.parametrize(
    'filter_mode,record_attr',
    [
        ('timestamp', 'timestamp'),
        ('depth', 'measured_depth'),
    ],
)
def test_last_processed_value_saved_to_cache(filter_mode, record_attr):
    event = [
        {
            "records": [],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]
    records_list = [
        [{record_attr: 0, 'asset_id': 0}, {record_attr: 1, "asset_id": 0}],
        [
            {record_attr: 0, 'asset_id': 0},
            {record_attr: 1, "asset_id": 0},
            {record_attr: 2, "asset_id": 0},
        ],
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    for idx, records in enumerate(records_list):
        event[0]['records'] = records

        results = corva.stream(stream_app, event, filter_mode=filter_mode)

        if idx == 0:
            # cache: record_attr = 1
            continue

        if idx == 1:
            # cache: record_attr = 2
            assert len(results[0].records) == 1
            assert getattr(results[0].records[0], record_attr) == 2


def test_default_last_processed_value_taken_from_cache():
    event = [
        {
            "records": [],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]
    records_list = [
        [{'timestamp': 0, 'asset_id': 0}, {'measured_depth': 0, 'asset_id': 0}],
        [{'timestamp': 1, 'asset_id': 0}],
        [{'measured_depth': 0, 'asset_id': 0}, {'measured_depth': 1, 'asset_id': 0}],
        [{'timestamp': 1, 'asset_id': 0}, {'timestamp': 2, 'asset_id': 0}],
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    for idx, records in enumerate(records_list):
        event[0]['records'] = records

        if idx == 0:
            corva.stream(stream_app, event, filter_mode=None)
            # cache: last_processed_timestamp = 0, last_processed_depth = 0
            continue

        if idx == 1:
            corva.stream(stream_app, event, filter_mode=None)
            # cache:
            #   last_processed_timestamp = 1 - new value
            #   last_processed_depth     = 0 - old value persisted, although there were
            #     no measured_depth in records
            continue

        if idx == 2:
            results = corva.stream(stream_app, event, filter_mode='depth')
            # cache:
            #   last_processed_timestamp = 1 - old value persisted, although there were
            #     no timestamp in records
            #   last_processed_depth     = 1 - new value

            assert len(results[0].records) == 1
            assert results[0].records[0].measured_depth == 1

        if idx == 3:
            results = corva.stream(stream_app, event, filter_mode='timestamp')
            # cache:
            #   last_processed_timestamp = 2 - new value
            #   last_processed_depth     = 1 - old value persisted

            assert len(results[0].records) == 1
            assert results[0].records[0].timestamp == 2


@pytest.mark.parametrize(
    'records,raises',
    [
        (
            [{"asset_id": 0}],
            True,
        ),
        (
            [{"asset_id": 0, "timestamp": 0, "measured_depth": 0}],
            False,
        ),
        (
            [{"asset_id": 0, "measured_depth": 0}],
            False,
        ),
        (
            [{"asset_id": 0, "timestamp": 0}],
            False,
        ),
    ],
    ids=[
        'no timestamp and no measured_depth provided',
        'both timestamp and measured_depth provided',
        'only measured_depth provided',
        'only timestamp provided',
    ],
)
def test_require_timestamp_or_measured_depth(records, raises):
    event = [
        {
            "records": records,
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert 'At least one of timestamp or measured_depth is required' in str(
            exc.value
        )
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'raises',
    [True, False],
    ids=['no app key exc', 'correct event'],
)
def test_require_app_key_in_metadata_apps(raises):
    apps = {} if raises else {SETTINGS.APP_KEY: {"app_connection_id": 0}}
    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {"app_stream_id": 0, "apps": apps},
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert 'metadata.apps dict must contain an app key.' in str(exc.value)
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'event_update,raises',
    [({'app_key': ''}, True), ({}, False)],
    ids=['app key set manually exc', 'correct event'],
)
def test_app_key_cant_be_set_manually(event_update, raises):
    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
            **event_update,
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert (
            'app_key can\'t be set manually, it is extracted from env and set automatically.'
            in str(exc.value)
        )
        return

    corva.stream(stream_app, event)


def test_early_return_if_no_records_after_filtering(mocker: MockerFixture):
    event = [
        {
            "records": [
                {"asset_id": 0, "collection": 'wits.completed', "timestamp": 0}
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    spy = mocker.Mock(stream_app, wraps=stream_app)

    corva.stream(spy, event)

    spy.assert_not_called()


def test_store_cache_data_for_empty_cache_data(mocker: MockerFixture):
    def stream_runner_mock(fn, context):
        return context

    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    mocker.patch('corva.application.stream_runner', stream_runner_mock)

    results = corva.stream(stream_app, event)

    context = results[0]  # type: StreamContext

    assert context.store_cache_data(cache_data=StreamStateData()) == 0
