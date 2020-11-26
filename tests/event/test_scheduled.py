from corva.event.scheduled import ScheduledEvent


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledEvent.load(event=scheduled_event_str)
    assert len(event) == 3


def test_get_state_key(scheduled_event_str):
    provider = 'provider'
    app_key = f'{provider}.app-key'
    asset_id = 39293110
    app_stream_id = 11792
    app_connection_id = 269616
    state_key = ScheduledEvent.get_state_key(event=scheduled_event_str, app_key=app_key)
    expected = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
    assert state_key == expected
