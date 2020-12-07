from typing import Optional

from pydantic import conint

from corva.event.data.base import BaseEventData


class TaskEventData(BaseEventData):
    id: Optional[str] = None
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported
