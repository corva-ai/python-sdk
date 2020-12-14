from corva.models.task import TaskEventData
from corva.event import Event
from corva.loader.base import BaseLoader


class TaskLoader(BaseLoader):
    def load(self, event: str) -> Event:
        event: dict = super()._load_json(event=event)

        data = [TaskEventData(**event)]

        return Event(data)
