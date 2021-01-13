import pytest

from corva.application import Corva

app = Corva()


def stream_app(event, api, state):
    return event


def test_run(settings):
    """Test that both usages of decorator run successfully"""

    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % settings.APP_KEY

    app.stream()(stream_app)(event)
    app.stream(stream_app)(event)


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
                ' "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % (collection, settings.APP_KEY)

    results = app.stream(func=stream_app)(event)

    assert len(results[0].records) == expected


def test_asset_id_persists_after_no_records_left_after_filtering(settings):
    event = (
                '[{"records": [{"asset_id": 123, "company_id": 0, "version": 0, "collection": "wits.completed", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                '"asset_id": 123}]'
            ) % settings.APP_KEY

    results = app.stream(func=stream_app)(event)

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
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                '"asset_id": 0}]'
            ) % (record_attr, record_attr, record_attr, settings.APP_KEY)

    results = app.stream(func=stream_app, **{filter_by: True})(event)

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
    event = (
                '[{"records": [{"%s": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}, {"%s": 1, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}, {"%s": 2, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                '"asset_id": 0}]'
            ) % (record_attr, record_attr, record_attr, settings.APP_KEY)

    results = app.stream(func=stream_app, **{filter_by: True})(event)

    assert len(results[0].records) == 3

    # second invocation
    next_event = (
                     '[{"records": [{"%s": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                     '"data": {}}, {"%s": 1, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                     '"data": {}}, {"%s": 2, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                     '"data": {}}, {"%s": 3, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                     '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                     '"asset_id": 0}]'
                 ) % (record_attr, record_attr, record_attr, record_attr, settings.APP_KEY)

    next_results = app.stream(func=stream_app, **{filter_by: True})(next_event)

    assert len(next_results[0].records) == 1
    assert getattr(next_results[0].records[0], record_attr) == 3


def test_empty_records_error(settings):
    event = (
                '[{"records": [], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                '"asset_id": 0}]'
            ) % settings.APP_KEY

    with pytest.raises(ValueError):
        app.stream(func=stream_app)(event)


def test_only_one_filter_allowed_at_a_time(settings):
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % settings.APP_KEY

    with pytest.raises(ValueError):
        app.stream(func=stream_app, filter_by_timestamp=True, filter_by_depth=True)(event)
