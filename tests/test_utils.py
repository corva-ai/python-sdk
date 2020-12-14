from corva.event import Event
from corva.models.base import BaseEventData
from corva.utils import GetStateKey

PROVIDER = 'provider'
APP_KEY = f'{PROVIDER}.app-key'
ASSET_ID = 1
APP_STREAM_ID = 2
APP_CONNECTION_ID = 3
STATE_KEY = f'{PROVIDER}/well/{ASSET_ID}/stream/{APP_STREAM_ID}/{APP_KEY}/{APP_CONNECTION_ID}'


def test_GetStateKey__get_provider():
    assert GetStateKey._get_provider(app_key=APP_KEY) == PROVIDER


def test_GetStateKey__get_key():
    state_key = GetStateKey._get_key(
        asset_id=ASSET_ID,
        app_stream_id=APP_STREAM_ID,
        app_key=APP_KEY,
        app_connection_id=APP_CONNECTION_ID
    )
    assert state_key == STATE_KEY


def test_GetStateKey_from_event():
    event = Event(
        [BaseEventData(asset_id=ASSET_ID, app_stream_id=APP_STREAM_ID, app_connection_id=APP_CONNECTION_ID)]
    )
    assert GetStateKey.from_event(event=event, app_key=APP_KEY) == STATE_KEY
