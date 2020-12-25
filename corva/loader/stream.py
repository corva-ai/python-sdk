from typing import List

from corva.loader.base import BaseLoader
from corva.models.stream import StreamEvent, StreamEventData


class StreamLoader(BaseLoader):
    parse_as_type = List[StreamEventData]

    def __init__(self, app_key: str):
        self.app_key = app_key

    def load(self, event: str) -> StreamEvent:
        parsed = self.parse(event=event)  # type: StreamLoader.parse_as_type

        for data in parsed:
            data.app_key = self.app_key

        return StreamEvent(event)
