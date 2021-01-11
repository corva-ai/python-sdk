import pytest

from corva.application import Corva
from tests.conftest import StreamDataMixer

app = Corva()


def stream_app(event, api, state):
    return event


@pytest.mark.parametrize(
    'collection, expected',
    [
        ('wits.completed', 0),
        ('random', 1)
    ]
)
def test_is_completed(collection, expected, settings):
    stream_event_metadata = StreamDataMixer.stream_event_metadata(
        apps={settings.APP_KEY: StreamDataMixer.app_metadata()}
    )
    records = [StreamDataMixer.record(collection=collection)]
    stream_event = StreamDataMixer.stream_event(records=records, metadata=stream_event_metadata)
    raw_event = StreamDataMixer.to_raw_event(stream_event)

    results = app.stream(func=stream_app)(raw_event)

    assert len(results[0].records) == expected


def test_asset_id_persists_after_no_records_left_after_filtering(settings):
    stream_event_metadata = StreamDataMixer.stream_event_metadata(
        apps={settings.APP_KEY: StreamDataMixer.app_metadata()}
    )
    records = [StreamDataMixer.record(collection='wits.completed', asset_id=123)]  # will be emptied by filtering
    stream_event = StreamDataMixer.stream_event(records=records, metadata=stream_event_metadata)
    raw_event = StreamDataMixer.to_raw_event(stream_event)

    results = app.stream(func=stream_app)(raw_event)

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
    stream_event_metadata = StreamDataMixer.stream_event_metadata(
        apps={settings.APP_KEY: StreamDataMixer.app_metadata()}
    )
    records = [StreamDataMixer.record(**{record_attr: val}) for val in [-2, -1, 0]]
    stream_event = StreamDataMixer.stream_event(records=records, metadata=stream_event_metadata)
    raw_event = StreamDataMixer.to_raw_event(stream_event)

    results = app.stream(func=stream_app, **{filter_by: True})(raw_event)

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
    stream_event_metadata = StreamDataMixer.stream_event_metadata(
        apps={settings.APP_KEY: StreamDataMixer.app_metadata()}
    )

    records = [StreamDataMixer.record(**{record_attr: val}) for val in [0, 1, 2]]
    stream_event = StreamDataMixer.stream_event(records=records, metadata=stream_event_metadata)
    raw_event = StreamDataMixer.to_raw_event(stream_event)

    results = app.stream(func=stream_app, **{filter_by: True})(raw_event)

    assert len(results[0].records) == 3

    next_records = [StreamDataMixer.record(**{record_attr: val}) for val in [0, 1, 2, 3]]
    next_stream_event = StreamDataMixer.stream_event(records=next_records, metadata=stream_event_metadata)
    next_raw_event = StreamDataMixer.to_raw_event(next_stream_event)

    next_results = app.stream(func=stream_app, **{filter_by: True})(next_raw_event)

    assert len(next_results[0].records) == 1
    assert getattr(next_results[0].records[0], record_attr) == 3


def test_empty_records_error(settings):
    stream_event_metadata = StreamDataMixer.stream_event_metadata(
        apps={settings.APP_KEY: StreamDataMixer.app_metadata()}
    )
    stream_event = StreamDataMixer.stream_event(records=[StreamDataMixer.record()], metadata=stream_event_metadata)
    stream_event.records = []  # ignore validation
    raw_event = StreamDataMixer.to_raw_event(stream_event)

    with pytest.raises(ValueError):
        app.stream(func=stream_app)(raw_event)
