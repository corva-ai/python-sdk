from corva.loader.base import BaseLoader
from corva.models.stream import StreamEvent
from corva.types import STREAM_EVENT_TYPE


class StreamLoader(BaseLoader[StreamEvent, STREAM_EVENT_TYPE]):
    def __init__(self, app_key: str):
        self.app_key = app_key

    def load(self, event: str) -> StreamEvent:
        event = super().load(event=event)  # type: StreamEvent

        for subevent in event:
            subevent.app_key = self.app_key

        return event
