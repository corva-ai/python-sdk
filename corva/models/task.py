from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, parse_raw_as
from pydantic.types import conint

from corva.models.base import BaseContext, BaseData, BaseEvent


class TaskStatus(Enum):
    fail = 'fail'
    success = 'success'


class TaskState(Enum):
    running = 'running'
    failed = 'failed'
    succeeded = 'succeeded'


class TaskData(BaseModel):
    id: str
    state: TaskState
    fail_reason: Optional[Any] = None
    asset_id: int
    company_id: int
    app_id: int
    document_bucket: str
    properties: Dict[str, Any]
    payload: Dict[str, Any]


class UpdateTaskData(BaseModel):
    fail_reason: Optional[str] = None
    payload: dict = {}


class TaskEventData(BaseData):
    id: Optional[str] = None
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported


class TaskEvent(BaseEvent, TaskEventData):
    @staticmethod
    def from_raw_event(event: str, **kwargs) -> TaskEvent:
        return parse_raw_as(TaskEvent, event)


class TaskContext(BaseContext[TaskEvent, BaseData]):
    pass
