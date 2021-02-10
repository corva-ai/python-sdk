from types import SimpleNamespace

import pytest

from corva.application import Corva
from corva.configuration import SETTINGS


def stream_app(event, api, cache):
    return event


@pytest.mark.parametrize(
    'collection, expected',
    [('wits.completed', 0), ('random', 1)],
    ids=['is_completed True', 'is_completed False'],
)
def test_is_completed(collection, expected):
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

    results = corva.stream(stream_app, event)

    assert len(results[0].records) == expected
    assert not results[0].is_completed


@pytest.mark.parametrize(
    'filter_mode,record_attr',
    [('timestamp', 'timestamp'), ('depth', 'measured_depth')],
)
def test_filter_mode(filter_mode, record_attr):
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

    results = corva.stream(stream_app, event, filter_mode=filter_mode)

    assert len(results[0].records) == 2


@pytest.mark.parametrize(
    'filter_mode,record_attr',
    [('timestamp', 'timestamp'), ('depth', 'measured_depth')],
)
def test_filter_mode_value_saved_for_next_run(filter_mode, record_attr):
    # first invocation
    event_1 = [
        {
            "records": [
                {record_attr: 0, "asset_id": 0},
                {record_attr: 1, "asset_id": 0},
                {record_attr: 2, "asset_id": 0},
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    results_1 = corva.stream(stream_app, event_1, filter_mode=filter_mode)

    assert len(results_1[0].records) == 3

    # second invocation
    event_2 = [
        {
            "records": [
                {record_attr: 0, "asset_id": 0},
                {record_attr: 1, "asset_id": 0},
                {record_attr: 2, "asset_id": 0},
                {record_attr: 3, "asset_id": 0},
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    results_2 = corva.stream(stream_app, event_2, filter_mode=filter_mode)

    assert len(results_2[0].records) == 1
    assert getattr(results_2[0].records[0], record_attr) == 3

    # additional invocations
    # after run event_2 should be filtered and have no records
    # verify, that in case of empty records, old values are persisted in cache
    for _ in range(2):
        results_3 = corva.stream(stream_app, event_2, filter_mode=filter_mode)
        assert len(results_3[0].records) == 0


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
        'no timestamp and no measured_depth provided exc',
        'both timestamp and measured_depth provided exc ',
        'only measured_depth provided (correct event)',
        'only timestamp provided (correct event)',
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
    'event_update,raises,exc_msg',
    [
        (
            {'asset_id': 0},
            True,
            'asset_id can\'t be set manually, it is extracted from records automatically.',
        ),
        (
            {'records': []},
            True,
            'Can\'t set asset_id as records are empty (which should not happen).',
        ),
        (
            {},
            False,
            '',
        ),
    ],
    ids=['asset_id set manually exc', 'empty records exc', 'correct event'],
)
def test_set_asset_id(event_update, raises, exc_msg):
    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]
    event[0].update(event_update)

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert exc_msg in str(exc.value)
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
