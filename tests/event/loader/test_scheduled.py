import pytest

from corva.event.event import Event
from corva.event.loader.scheduled import ScheduledLoader
from tests.conftest import SCHEDULED_EVENT_FILE_PATH


@pytest.fixture(scope='session')
def scheduled_event_str() -> str:
    with open(SCHEDULED_EVENT_FILE_PATH) as scheduled_event:
        return scheduled_event.read()


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledLoader().load(event=scheduled_event_str)

    assert len(event) == 3
    assert isinstance(event, Event)
