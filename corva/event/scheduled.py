from __future__ import annotations

from itertools import chain
from typing import List

from worker.constants import RAW_EVENT_TYPE
from worker.event.base import BaseEvent
from worker.event.data.scheduled import ScheduledEventData


class ScheduledEvent(BaseEvent):
    @classmethod
    def load(cls, event: RAW_EVENT_TYPE, **kwargs) -> ScheduledEvent:
        event: List[List[dict]] = super()._load(event=event)
        data = [
            ScheduledEventData(**subdata)
            for subdata in chain(*event)
        ]
        return ScheduledEvent(data=data)
