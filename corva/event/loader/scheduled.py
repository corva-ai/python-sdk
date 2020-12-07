from __future__ import annotations

from itertools import chain
from typing import List

from corva.event.data.scheduled import ScheduledEventData
from corva.event.event import Event
from corva.event.loader.base import BaseLoader


class ScheduledLoader(BaseLoader):
    def load(self, event: str) -> Event:
        event: List[List[dict]] = super()._load_json(event=event)
        event: List[dict] = list(chain(*event))

        data = []
        for subdata in event:
            subdata['schedule_start'] /= 1000  # from ms to seconds
            subdata['schedule_end'] /= 1000  # from ms to seconds
            subdata['app_connection_id'] = subdata.pop('app_connection')
            subdata['app_stream_id'] = subdata.pop('app_stream')
            data.append(ScheduledEventData(**subdata))

        return Event(data=data)