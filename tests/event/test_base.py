import json
from unittest.mock import patch

import pytest

from corva.event.base import BaseEvent
from corva.event.data.base import BaseEventData


def test_load_empty_event():
    event = ''
    with pytest.raises(ValueError) as exc:
        BaseEvent._load(event=event)
    assert str(exc.value) == 'Empty event'


def test_load_wrong_event_type():
    event = {'': ''}
    with pytest.raises(TypeError) as exc:
        BaseEvent._load(event=event)
    assert str(exc.value) == f'Unknown event type {type(event)}'


def test_load_invalid_json():
    event = '{}'
    with patch('corva.event.base.json.loads', side_effect=ValueError):
        with pytest.raises(ValueError) as exc:
            BaseEvent._load(event=event)
    assert str(exc.value) == 'Invalid event JSON'


def test_load():
    event = {'key1': 'val1'}

    loaded = BaseEvent._load(event=json.dumps(event))
    assert loaded == event


def test_iter(patch_base_event):
    data1 = BaseEventData()
    data2 = BaseEventData(a=1)
    e2 = BaseEvent(data=[data1, data2])
    for idx, data in enumerate(e2):
        assert e2.data[idx] == data


def test_eq(patch_base_event):
    class CustomEventData(BaseEventData):
        pass

    # different len
    e1 = BaseEvent(data=[])
    e2 = BaseEvent(data=[BaseEventData()])
    assert e1 != e2

    # different fields
    e1 = BaseEvent(data=[BaseEventData()])
    e2 = BaseEvent(data=[BaseEventData(a=1)])
    assert e1 != e2

    # different order
    e1 = BaseEvent(data=[BaseEventData(), BaseEventData(a=1)])
    e2 = BaseEvent(data=[BaseEventData(a=1), BaseEventData()])
    assert e1 != e2

    # different types
    e1 = BaseEvent(data=[CustomEventData()])
    e2 = BaseEvent(data=[BaseEventData()])
    assert e1 != e2

    # success
    e1 = BaseEvent(data=[BaseEventData(), CustomEventData(a=1)])
    e2 = BaseEvent(data=[BaseEventData(), CustomEventData(a=1)])
    assert e1 == e2


def test_len(patch_base_event):
    assert len(BaseEvent(data=[])) == 0
    assert len(BaseEvent(data=[BaseEventData()])) == 1
    assert len(BaseEvent(data=[BaseEventData(), BaseEventData()])) == 2


def test_abstractmethods():
    assert (
         getattr(BaseEvent, '__abstractmethods__')
         ==
         frozenset(['load', 'get_asset_id', 'get_app_stream_id', 'get_app_connection_id'])
    )
    with pytest.raises(TypeError):
        BaseEvent(data=[])


def test_get_state_key(patch_base_event):
    base_event_path = 'corva.event.base'

    provider = 'provider'
    app_key = f'{provider}.app-key'
    asset_id = 1
    app_stream_id = 2
    app_connection_id = 3

    with patch.object(BaseEvent, '_load'), \
         patch(f'{base_event_path}.get_provider', return_value=provider), \
         patch.object(BaseEvent, 'get_asset_id', return_value=asset_id), \
         patch.object(BaseEvent, 'get_app_stream_id', return_value=app_stream_id), \
         patch.object(BaseEvent, 'get_app_connection_id', return_value=app_connection_id):
        state_key = BaseEvent.get_state_key(event='', app_key=app_key)
        assert state_key == f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
