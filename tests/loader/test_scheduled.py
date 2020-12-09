import pytest

from corva.event import Event
from corva.loader.scheduled import ScheduledLoader


@pytest.fixture(scope='module')
def scheduled_event_str() -> str:
    with open('data/tests/scheduled_event.json') as scheduled_event:
        return scheduled_event.read()


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledLoader().load(event=scheduled_event_str)

    assert len(event) == 3
    assert isinstance(event, Event)
