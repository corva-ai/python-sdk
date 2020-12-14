from __future__ import annotations

from itertools import chain
from typing import List

from corva.models.scheduled import ScheduledEventData
from corva.event import Event
from corva.loader.base import BaseLoader


class ScheduledLoader(BaseLoader):
    def load(self, event: str) -> Event:
        event: List[List[dict]] = super()._load_json(event=event)
        event: List[dict] = list(chain(*event))

        data = []
        for subdata in event:
            subdata['app_connection_id'] = subdata.pop('app_connection')
            subdata['app_stream_id'] = subdata.pop('app_stream')
            data.append(ScheduledEventData(**subdata))

        return Event(data)
