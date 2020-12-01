from typing import Optional

from corva.event.data.base import BaseEventData


class TaskEventData(BaseEventData):
    id: Optional[str] = None
    task_id: str
    version: int
