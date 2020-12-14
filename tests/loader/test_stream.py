import json

import pytest

from corva.loader.stream import StreamLoader
from corva.types import STREAM_EVENT_TYPE
from tests.conftest import DATA_PATH


@pytest.fixture(scope='module')
def stream_event_str() -> str:
    with open(DATA_PATH / 'stream_event.json') as stream_event:
        return stream_event.read()


def test_get_asset_id():
    data = {'records': [{'asset_id': 1}]}

    assert StreamLoader.get_asset_id(data=data) == 1


def test_get_asset_id_index_exc():
    data = {'records': []}

    with pytest.raises(ValueError) as exc:
        StreamLoader.get_asset_id(data=data)

    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test_get_asset_id_key_exc():
    data = {}

    with pytest.raises(ValueError) as exc:
        StreamLoader.get_asset_id(data=data)

    assert str(exc.value) == f'Could not find an asset id in data: {data}.'


def test__get_app_connection(stream_event_str):
    event: STREAM_EVENT_TYPE = StreamLoader._load_json(event=stream_event_str)

    for subdata, app_key, expected in zip(event, ['corva.wits-depth-summary', 'other.oil-price-app'], [1, 2]):
        app_connection_id = StreamLoader._get_app_connection_id(subdata=subdata, app_key=app_key)
        assert app_connection_id == expected


def test__get_app_connection_key_error(stream_event_str):
    event: STREAM_EVENT_TYPE = StreamLoader._load_json(event=stream_event_str)

    with pytest.raises(ValueError) as exc:
        StreamLoader._get_app_connection_id(subdata=event[0], app_key='random')

    assert str(exc.value) == 'Can\'t get random from metadata.apps.'


@pytest.mark.parametrize(
    'records, expected',
    (
         ([{'collection': 'wits.completed'}, {}], False),
         ([{}, {'collection': 'wits.completed'}], True)
    )
)
def test__get_is_completed(records, expected):
    assert StreamLoader._get_is_completed(records=records) == expected


def test_get_is_completed_index_exc():
    records = []

    with pytest.raises(ValueError) as exc:
        StreamLoader._get_is_completed(records=records)

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

    loaded_event = StreamLoader(app_key=app_key).load(event=json.dumps(event_str))

    assert loaded_event[-1].is_completed
    assert len(loaded_event[-1].records) == 1


def test_load_from_file(stream_event_str):
    """Tests that stream event is loaded from file without exceptions."""

    event = StreamLoader(app_key='corva.wits-depth-summary').load(event=stream_event_str)

    assert len(event) == 1
