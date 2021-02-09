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
    event = (
        '[{"records": [{"asset_id": 0, "collection": "%s", "timestamp": 0}], "metadata": {"app_stream_id": 0, '
        '"apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % (collection, SETTINGS.APP_KEY)

    corva = Corva(SimpleNamespace(client_context=None))

    results = corva.stream(stream_app, event)

    assert len(results[0].records) == expected
    assert not results[0].is_completed


@pytest.mark.parametrize(
    'filter_by,record_attr',
    [('filter_by_timestamp', 'timestamp'), ('filter_by_depth', 'measured_depth')],
)
def test_filter_by(filter_by, record_attr):
    event = (
        '[{"records": [{"%s": -2, "asset_id": 0}, '
        '{"%s": -1, "asset_id": 0}, '
        '{"%s": 0, "asset_id": 0}], '
        '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % (record_attr, record_attr, record_attr, SETTINGS.APP_KEY)

    corva = Corva(SimpleNamespace(client_context=None))

    results = corva.stream(stream_app, event, **{filter_by: True})

    assert len(results[0].records) == 1
    assert getattr(results[0].records[0], record_attr) == 0


@pytest.mark.parametrize(
    'filter_by,record_attr',
    [('filter_by_timestamp', 'timestamp'), ('filter_by_depth', 'measured_depth')],
)
def test_filter_by_value_saved_for_next_run(filter_by, record_attr):
    # first invocation
    event_1 = (
        '[{"records": [{"%s": 0, "asset_id": 0}, '
        '{"%s": 1, "asset_id": 0}, '
        '{"%s": 2, "asset_id": 0}], '
        '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % (record_attr, record_attr, record_attr, SETTINGS.APP_KEY)

    corva = Corva(SimpleNamespace(client_context=None))

    results_1 = corva.stream(stream_app, event_1, **{filter_by: True})

    assert len(results_1[0].records) == 3

    # second invocation
    event_2 = (
        '[{"records": [{"%s": 0, "asset_id": 0}, '
        '{"%s": 1, "asset_id": 0}, '
        '{"%s": 2, "asset_id": 0}, '
        '{"%s": 3, "asset_id": 0}], '
        '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % (record_attr, record_attr, record_attr, record_attr, SETTINGS.APP_KEY)

    results_2 = corva.stream(stream_app, event_2, **{filter_by: True})

    assert len(results_2[0].records) == 1
    assert getattr(results_2[0].records[0], record_attr) == 3

    # additional invocations
    # after run event_2 should be filtered and have no records
    # verify, that in case of empty records, old values are persisted in cache
    for _ in range(2):
        results_3 = corva.stream(stream_app, event_2, **{filter_by: True})
        assert len(results_3[0].records) == 0


@pytest.mark.parametrize(
    'event,raises',
    [
        (
            '[{"records": [], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]',
            True,
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
            '"apps": {"%s": {"app_connection_id": 0}}}}]',
            False,
        ),
    ],
    ids=['empty records exc', 'correct event'],
)
def test_empty_records_error(event, raises):
    event %= SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert (
            'Can\'t set asset_id as records are empty (which should not happen).'
            in str(exc.value)
        )
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'kwargs,raises',
    [
        (
            {'filter_by_timestamp': True, 'filter_by_depth': True},
            True,
        ),
        ({'filter_by_timestamp': True}, False),
        ({'filter_by_depth': True}, False),
        ({}, False),
    ],
    ids=[
        'two filters',
        'one filter (correct)',
        'one filter (correct)',
        'no filters (correct)',
    ],
)
def test_check_one_active_filter_at_most(kwargs, raises):
    event = (
        '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
        '"apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event, **kwargs)
        assert (
            'filter_by_timestamp and filter_by_depth can\'t be set to True together.'
            in str(exc.value)
        )


@pytest.mark.parametrize(
    'event,raises',
    [
        (
            '[{"records": [{"asset_id": 0}], "metadata": {"app_stream_id": 0, "apps": '
            '{"%s": {"app_connection_id": 0}}}}]',
            True,
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0, "measured_depth": 0}], "metadata": '
            '{"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]',
            True,
        ),
        (
            '[{"records": [{"asset_id": 0, "measured_depth": 0}], "metadata": {"app_stream_id": 0, "apps": '
            '{"%s": {"app_connection_id": 0}}}}]',
            False,
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, "apps": '
            '{"%s": {"app_connection_id": 0}}}}]',
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
def test_require_timestamp_or_measured_depth(event, raises):
    event %= SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert 'Either timestamp or measured_depth is required' in str(exc.value)
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'event,raises,format_event',
    [
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, "apps": {}}}]',
            True,
            False,
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, "apps": '
            '{"%s": {"app_connection_id": 0}}}}]',
            False,
            True,
        ),
    ],
    ids=['no app key exc', 'correct event'],
)
def test_require_app_key_in_metadata_apps(event, raises, format_event):
    if format_event:
        event %= SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert 'metadata.apps dict must contain an app key.' in str(exc.value)
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'event,raises,exc_msg',
    [
        (
            '[{"asset_id": 0, "records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
            '"apps": {"%s": {"app_connection_id": 0}}}}]',
            True,
            'asset_id can\'t be set manually, it is extracted from records automatically.',
        ),
        (
            '[{"records": [], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]',
            True,
            'Can\'t set asset_id as records are empty (which should not happen).',
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
            '"apps": {"%s": {"app_connection_id": 0}}}}]',
            False,
            '',
        ),
    ],
    ids=['asset_id set manually exc', 'empty records exc', 'correct event'],
)
def test_set_asset_id(event, raises, exc_msg):
    event %= SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert exc_msg in str(exc.value)
        return

    corva.stream(stream_app, event)


@pytest.mark.parametrize(
    'event,raises',
    [
        (
            '[{"app_key": "", "records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
            '"apps": {"%s": {"app_connection_id": 0}}}}]',
            True,
        ),
        (
            '[{"records": [{"asset_id": 0, "timestamp": 0}], "metadata": {"app_stream_id": 0, '
            '"apps": {"%s": {"app_connection_id": 0}}}}]',
            False,
        ),
    ],
    ids=['app key set manually exc', 'correct event'],
)
def test_app_key_cant_be_set_manually(event, raises):
    event %= SETTINGS.APP_KEY

    corva = Corva(SimpleNamespace(client_context=None))

    if raises:
        exc = pytest.raises(ValueError, corva.stream, stream_app, event)
        assert (
            'app_key can\'t be set manually, it is extracted from env and set automatically.'
            in str(exc.value)
        )
        return

    corva.stream(stream_app, event)
