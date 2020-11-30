import json

import pytest

from corva.constants import STREAM_EVENT_TYPE
from corva.event.base import BaseEvent
from corva.event.stream import StreamEvent


def test_get_asset_id(stream_event):
    assert StreamEvent.get_asset_id(data=stream_event[0]) == 1


def test_get_asset_id_index_exc(stream_event):
    data = {'records': []}
    with pytest.raises(ValueError) as exc:
        StreamEvent.get_asset_id(data=data)
    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test_get_asset_id_key_exc(stream_event):
    data = {}
    with pytest.raises(ValueError) as exc:
        StreamEvent.get_asset_id(data=data)
    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test_get_app_connection(stream_event_str):
    event: STREAM_EVENT_TYPE = BaseEvent._load(event=stream_event_str)
    for subdata, app_key, expected in zip(event, ['corva.wits-depth-summary', 'other.oil-price-app'], [1, 2]):
        app_connection_id = StreamEvent._get_app_connection_id(subdata=subdata, app_key=app_key)
        assert app_connection_id == expected


def test_get_app_connection_key_error(stream_event_str):
    event: STREAM_EVENT_TYPE = BaseEvent._load(event=stream_event_str)
    with pytest.raises(ValueError) as exc:
        StreamEvent._get_app_connection_id(subdata=event[0], app_key='random')
    assert str(exc.value) == 'Can\'t get random from metadata.apps.'


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


def test_load_is_completed():
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
            'records': [
                {
                    'collection': 'wits.completed',
                    'asset_id': 3,
                    'timestamp': int(),
                    'company_id': int(),
                    'version': int(),
                    'data': {}
                }
            ]
        }
    ]

    loaded_event: StreamEvent = StreamEvent.load(
        event=json.dumps(event_str),
        app_key=app_key
    )

    assert loaded_event[-1].is_completed
    assert len(loaded_event[-1].records) == 1


def test_load_from_file(stream_event_str):
    """Tests that stream event is loaded from file without exceptions."""

    StreamEvent.load(event=stream_event_str, app_key='corva.wits-depth-summary')
