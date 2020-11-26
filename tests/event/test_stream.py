import json

import pytest

from corva.event.base import BaseEvent
from corva.event.stream import StreamEvent


def test_get_state_key(stream_event_str):
    provider = 'corva'
    app_key = 'corva.wits-depth-summary'
    asset_id = 1
    app_stream_id = 294712
    app_connection_id = 1
    state_key = StreamEvent.get_state_key(event=stream_event_str, app_key=app_key)
    expected = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
    assert state_key == expected


def test_get_asset_id_from_dict(stream_event):
    assert StreamEvent.GetAssetId.from_dict(data=stream_event[0]) == 1


def test_get_asset_id_from_dict_index_exc(stream_event):
    data = {'records': []}
    with pytest.raises(ValueError) as exc:
        StreamEvent.GetAssetId.from_dict(data=data)
    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test_get_asset_id_from_dict_key_exc(stream_event):
    data = {}
    with pytest.raises(ValueError) as exc:
        StreamEvent.GetAssetId.from_dict(data=data)
    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test_get_asset_id_from_event(stream_event_str):
    asset_id = StreamEvent.GetAssetId.from_event(event=stream_event_str)
    assert asset_id == 1


def test_get_asset_id_from_event_index_exc():
    event_str = '[]'
    with pytest.raises(ValueError) as exc:
        StreamEvent.GetAssetId.from_event(event=event_str)
    assert str(exc.value) == f'Could not find asset id in event: {BaseEvent._load(event=event_str)}.'


def test_get_app_connection(stream_event_str):
    event = BaseEvent._load(event=stream_event_str)
    for subdata, app_key, expected in zip(event, ['corva.wits-depth-summary', 'other.oil-price-app'], [1, 2]):
        app_connection_id = StreamEvent._get_app_connection_id(subdata=subdata, app_key=app_key)
        assert app_connection_id == expected


def test_get_app_connection_key_error(stream_event_str):
    event = BaseEvent._load(event=stream_event_str)
    with pytest.raises(ValueError) as exc:
        StreamEvent._get_app_connection_id(subdata=event[0], app_key='random')
    assert str(exc.value) == 'Can\'t get random from metadata.apps.'


def test_filter_records_is_completed():
    records = [{}, {'collection': 'wits.completed'}]
    assert StreamEvent._filter_records(records=records) == [{}]

    records = [{'collection': 'wits.completed'}, {}]
    assert StreamEvent._filter_records(records=records) == records


def test_filter_records_with_last_processed_timestamp():
    last_processed_timestamp = 1
    records = [{'timestamp': t} for t in [0, 1, 2]]
    records = StreamEvent._filter_records(records=records, last_processed_timestamp=last_processed_timestamp)
    assert records == [{'timestamp': 2}]


def test_filter_records_with_last_processed_depth():
    last_processed_depth = 1
    records = [{'measured_depth': d} for d in [0, 1, 2]]
    records = StreamEvent._filter_records(records=records, last_processed_depth=last_processed_depth)
    assert records == [{'measured_depth': 2}]


def test_filter_records_with_all_filters():
    last_processed_timestamp = 1
    last_processed_depth = 1
    records = [
        {'timestamp': 0, 'measured_depth': 2},
        {'timestamp': 1, 'measured_depth': 2},
        {'timestamp': 2, 'measured_depth': 2},
        {'timestamp': 2, 'measured_depth': 0},
        {'timestamp': 2, 'measured_depth': 1},
    ]
    records = StreamEvent._filter_records(
        records=records,
        last_processed_timestamp=last_processed_timestamp,
        last_processed_depth=last_processed_depth
    )
    assert records == [{'timestamp': 2, 'measured_depth': 2}]


@pytest.mark.parametrize(
    'records, expected',
    (
         ([{'collection': 'wits.completed'}, {}], False),
         ([{}, {'collection': 'wits.completed'}], True)
    )
)
def test_get_is_completed(records, expected):
    assert StreamEvent._get_is_completed(records=records) == expected


def test_get_is_completed_index_exc():
    records = []
    with pytest.raises(ValueError) as exc:
        StreamEvent._get_is_completed(records=records)
    assert str(exc.value) == f'Records are empty: {records}'


def test_load_app_key_not_in_kwargs(stream_event_str):
    with pytest.raises(ValueError) as exc:
        StreamEvent.load(event=stream_event_str)
    assert str(exc.value) == 'Missing app_key in kwargs.'


def test_load():
    app_key = 'myapp'
    metadata = {
        'apps': {
            app_key: {
                'app_connection_id': 1
            }
        },
        'app_stream_id': 2
    }
    event = [
        {
            'metadata': metadata,
            'records': [
                {
                    'asset_id': 3,
                    'timestamp': 101,
                    'company_id': 4,
                    'version': 5,
                    'data': {},
                    'measured_depth': 1.
                },
                {
                    'asset_id': 3,
                    'timestamp': 100,
                    'company_id': 4,
                    'version': 5,
                    'data': {},
                    'measured_depth': 2.
                },
                {
                    'asset_id': 3,
                    'timestamp': 101,
                    'company_id': 4,
                    'version': 5,
                    'data': {},
                    'measured_depth': 2.
                },
            ]
        }
    ]

    loaded_event: StreamEvent = StreamEvent.load(
        event=json.dumps(event),
        app_key=app_key,
        last_processed_timestamp=100,
        last_processed_depth=1.
    )
    assert len(loaded_event[0].records) == 1
    assert (
         loaded_event[0].records[0].timestamp == event[0]['records'][2]['timestamp']
         and
         loaded_event[0].records[0].measured_depth == event[0]['records'][2]['measured_depth']
    )


@pytest.mark.parametrize(
    'records, is_completed, expected_len',
    (
         ([{'collection': 'wits.completed', 'asset_id': 3}], True, 0),
         ([{'asset_id': 3, 'timestamp': int(), 'company_id': int(), 'version': int(), 'data': {}}], False, 1)
    )
)
def test_load_is_completed(records, is_completed, expected_len):
    app_key = 'myapp'
    metadata = {
        'apps': {
            app_key: {
                'app_connection_id': 1
            }
        },
        'app_stream_id': 2
    }
    event_str = [
        {
            'metadata': metadata,
            'records': records
        }
    ]

    loaded_event: StreamEvent = StreamEvent.load(
        event=json.dumps(event_str),
        app_key=app_key
    )

    assert loaded_event[-1].is_completed == is_completed
    assert len(loaded_event[-1].records) == expected_len


def test_load_from_file(stream_event_str):
    """Tests that stream event is loaded from file without exceptions."""

    StreamEvent.load(event=stream_event_str, app_key='corva.wits-depth-summary')
