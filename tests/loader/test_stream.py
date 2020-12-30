import pytest

from corva.models.stream import StreamEvent
from tests.conftest import DATA_PATH


@pytest.fixture(scope='module')
def stream_event_str() -> str:
    with open(DATA_PATH / 'stream_event.json') as stream_event:
        return stream_event.read()


def test_load_from_file(stream_event_str):
    """Tests that stream event is loaded from file without exceptions."""

    event = StreamEvent.from_raw_event(event=stream_event_str, app_key='corva.wits-depth-summary')

    assert len(event) == 1
