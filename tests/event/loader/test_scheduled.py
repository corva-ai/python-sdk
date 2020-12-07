from corva.event.event import Event
from corva.event.loader.scheduled import ScheduledLoader


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledLoader().load(event=scheduled_event_str)

    assert len(event) == 3
    assert isinstance(event, Event)
