from corva.event.scheduled import ScheduledEvent

SCHEDULED_EVENT_FILE_PATH = 'data/tests/scheduled_event.json'


def test_load():
    """test that sample scheduled event loaded without exceptions"""

    with open(SCHEDULED_EVENT_FILE_PATH) as scheduled_event:
        event_str = scheduled_event.read()
    event = ScheduledEvent.load(event=event_str)
    assert len(event) == 3
