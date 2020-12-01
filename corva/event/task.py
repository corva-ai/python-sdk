from __future__ import annotations

from corva.event.base import BaseEvent
from corva.event.data.task import TaskEventData


class TaskEvent(BaseEvent):
    @classmethod
    def load(cls, event: str, **kwargs) -> TaskEvent:
        event: dict = super()._load(event=event)

        data = [TaskEventData(**event)]

        return TaskEvent(data=data)
