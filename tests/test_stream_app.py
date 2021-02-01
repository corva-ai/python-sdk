from types import SimpleNamespace

import pytest

from corva.application import Corva


def stream_app(event, api, cache):
    return event


@pytest.mark.parametrize(
    'collection, expected',
    [
        ('wits.completed', 0),
        ('random', 1)
    ]
)
def test_is_completed(collection, expected, settings):
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "%s", "data": {}}],'
                ' "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}}]'
            ) % (collection, settings.APP_KEY)
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    results = app.stream(stream_app, event)

    assert len(results[0].records) == expected


def test_asset_id_persists_after_no_records_left_after_filtering(settings):
    event = (
                '[{"records": [{"asset_id": 123, "company_id": 0, "version": 0, "collection": "wits.completed", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, '
                '"app_version": 0}}}}]'
            ) % settings.APP_KEY
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    results = app.stream(stream_app, event)

    assert len(results[0].records) == 0
    assert results[0].asset_id == 123


@pytest.mark.parametrize(
    'filter_by,record_attr',
    [
        ('filter_by_timestamp', 'timestamp'),
        ('filter_by_depth', 'measured_depth')
    ]
)
def test_filter_by(filter_by, record_attr, settings):
    event = (
                '[{"records": [{"%s": -2, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}, {"%s": -1, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}, {"%s": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, '
                '"app_version": 0}}}}]'
            ) % (record_attr, record_attr, record_attr, settings.APP_KEY)
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    results = app.stream(stream_app, event, **{filter_by: True})

    assert len(results[0].records) == 1
    assert getattr(results[0].records[0], record_attr) == 0


@pytest.mark.parametrize(
    'filter_by,record_attr',
    [
        ('filter_by_timestamp', 'timestamp'),
        ('filter_by_depth', 'measured_depth')
    ]
)
def test_filter_by_value_saved_for_next_run(filter_by, record_attr, settings):
    # first invocation
    event_1 = (
                  '[{"records": [{"%s": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}, {"%s": 1, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}, {"%s": 2, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, '
                  '"app_version": 0}}}}]'
              ) % (record_attr, record_attr, record_attr, settings.APP_KEY)
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    results_1 = app.stream(stream_app, event_1, **{filter_by: True})

    assert len(results_1[0].records) == 3

    # second invocation
    event_2 = (
                  '[{"records": [{"%s": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}, {"%s": 1, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}, {"%s": 2, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}, {"%s": 3, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                  '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, '
                  '"app_version": 0}}}}]'
              ) % (record_attr, record_attr, record_attr, record_attr, settings.APP_KEY)

    results_2 = app.stream(stream_app, event_2, **{filter_by: True})

    assert len(results_2[0].records) == 1
    assert getattr(results_2[0].records[0], record_attr) == 3

    # additional invocations
    # after run event_2 should be filtered and have no records
    # verify, that in case of empty records, old values are persisted in cache
    for _ in range(2):
        results_3 = app.stream(stream_app, event_2, **{filter_by: True})
        assert len(results_3[0].records) == 0


def test_empty_records_error(settings):
    event = (
                '[{"records": [], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, '
                '"app_version": 0}}}}]'
            ) % settings.APP_KEY
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    with pytest.raises(ValueError) as exc:
        app.stream(stream_app, event)

    assert '1 validation error' in str(exc.value)
    assert 'Can\'t set asset_id as records are empty (which should not happen).' in str(exc.value)


def test_only_one_filter_allowed_at_a_time(settings):
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}}]'
            ) % settings.APP_KEY
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    with pytest.raises(ValueError) as exc:
        app.stream(stream_app, event, filter_by_timestamp=True, filter_by_depth=True)

    assert '1 validation error' in str(exc.value)
    assert 'filter_by_timestamp and filter_by_depth can\'t be set to True together.' in str(exc.value)
