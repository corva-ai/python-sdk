from __future__ import annotations

from itertools import chain
from typing import List

from corva.event.base import BaseEvent
from corva.event.data.scheduled import ScheduledEventData


class ScheduledEvent(BaseEvent):
    @classmethod
    def load(cls, event: str, **kwargs) -> ScheduledEvent:
        event: List[List[dict]] = super()._load(event=event)
        data = [
            ScheduledEventData(**subdata)
            for subdata in chain(*event)
        ]
        return ScheduledEvent(data=data)
