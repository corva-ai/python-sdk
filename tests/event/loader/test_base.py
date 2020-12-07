import json

import pytest
from pytest_mock import MockerFixture

from corva.event.event import Event
from corva.event.loader.base import BaseLoader

BASE_LOADER_PATH = 'corva.event.loader.base'


def test__load_json_empty_event():
    event = ''

    with pytest.raises(ValueError) as exc:
        BaseLoader._load_json(event=event)

    assert str(exc.value) == 'Empty event'


def test__load_json_wrong_event_type():
    event = {'': ''}

    with pytest.raises(TypeError) as exc:
        BaseLoader._load_json(event=event)

    assert str(exc.value) == f'Unknown event type {type(event)}'


def test__load_json_invalid_json(mocker: MockerFixture):
    event = '{}'

    mocker.patch(f'{BASE_LOADER_PATH}.json.loads', side_effect=ValueError)

    with pytest.raises(ValueError) as exc:
        BaseLoader._load_json(event=event)

    assert str(exc.value) == 'Invalid event JSON'


def test__load_json():
    event = {'key1': 'val1'}

    loaded = BaseLoader._load_json(event=json.dumps(event))

    assert loaded == event


def test_abstractmethods():
    assert getattr(Event, '__abstractmethods__') == frozenset(['load'])
    with pytest.raises(TypeError):
        Event(data=[])
