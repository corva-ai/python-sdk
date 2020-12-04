import json
from unittest.mock import patch

import pytest

from corva.event.event import Event
from corva.event.data.base import BaseEventData


def test_load_empty_event():
    event = ''
    with pytest.raises(ValueError) as exc:
        Event._load(event=event)
    assert str(exc.value) == 'Empty event'


def test_load_wrong_event_type():
    event = {'': ''}
    with pytest.raises(TypeError) as exc:
        Event._load(event=event)
    assert str(exc.value) == f'Unknown event type {type(event)}'


def test_load_invalid_json():
    event = '{}'
    with patch('corva.event.base.json.loads', side_effect=ValueError):
        with pytest.raises(ValueError) as exc:
            Event._load(event=event)
    assert str(exc.value) == 'Invalid event JSON'


def test_load():
    event = {'key1': 'val1'}

    loaded = Event._load(event=json.dumps(event))
    assert loaded == event


def test_iter(patch_base_event):
    data1 = BaseEventData()
    data2 = BaseEventData(a=1)
    e2 = Event(data=[data1, data2])
    for idx, data in enumerate(e2):
        assert e2.data[idx] == data


def test_eq(patch_base_event):
    class CustomEventData(BaseEventData):
        pass

    # different len
    e1 = Event(data=[])
    e2 = Event(data=[BaseEventData()])
    assert e1 != e2

    # different fields
    e1 = Event(data=[BaseEventData()])
    e2 = Event(data=[BaseEventData(a=1)])
    assert e1 != e2

    # different order
    e1 = Event(data=[BaseEventData(), BaseEventData(a=1)])
    e2 = Event(data=[BaseEventData(a=1), BaseEventData()])
    assert e1 != e2

    # different types
    e1 = Event(data=[CustomEventData()])
    e2 = Event(data=[BaseEventData()])
    assert e1 != e2

    # success
    e1 = Event(data=[BaseEventData(), CustomEventData(a=1)])
    e2 = Event(data=[BaseEventData(), CustomEventData(a=1)])
    assert e1 == e2


def test_len(patch_base_event):
    assert len(Event(data=[])) == 0
    assert len(Event(data=[BaseEventData()])) == 1
    assert len(Event(data=[BaseEventData(), BaseEventData()])) == 2


def test_abstractmethods():
    assert getattr(Event, '__abstractmethods__') == frozenset(['load'])
    with pytest.raises(TypeError):
        Event(data=[])
