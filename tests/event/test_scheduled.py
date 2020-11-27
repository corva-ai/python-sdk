from corva.event.scheduled import ScheduledEvent


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledEvent.load(event=scheduled_event_str)
    assert len(event) == 3
