import pytest

from corva.app.stream import StreamApp
from corva.event import Event
from corva.models.stream import StreamContext, StreamEventData, Record
from tests.conftest import APP_KEY, CACHE_URL


@pytest.fixture(scope='function')
def stream_app(api):
    return StreamApp(api=api, app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='module')
def stream_event_data_factory(record_factory):
    def _stream_event_data_factory(**kwargs):
        default_params = {
            'records': [],
            'metadata': {},
            'asset_id': int(),
            'app_connection_id': int(),
            'app_stream_id': int(),
            'is_completed': False
        }
        default_params.update(kwargs)

        return StreamEventData(**default_params)

    return _stream_event_data_factory


@pytest.fixture(scope='module')
def record_factory():
    def _record_factory(**kwargs):
        default_params = {
            'timestamp': int(),
            'asset_id': int(),
            'company_id': int(),
            'version': int(),
            'data': {},
            'collection': str()
        }
        default_params.update(kwargs)

        return Record(**default_params)

    return _record_factory


@pytest.fixture(scope='function')
def stream_context_factory(stream_event_data_factory, redis):
    def _stream_context_factory(**kwargs):
        default_params = {
            'event': Event([stream_event_data_factory()]),
            'state': redis
        }
        default_params.update(kwargs)

        return StreamContext(**default_params)

    return _stream_context_factory


@pytest.mark.parametrize(
    'attr_name,expected', (('DEFAULT_LAST_PROCESSED_VALUE', -1), ('group_by_field', 'app_connection_id'))
)
def test_default_values(attr_name, expected):
    assert getattr(StreamApp, attr_name) == expected
