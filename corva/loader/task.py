from corva.loader.base import BaseLoader
from corva.models.task import TaskEvent


class TaskLoader(BaseLoader):
    parse_as_type = TaskEvent

    def load(self, event: str) -> TaskEvent:
        return self.parse(event=event)
