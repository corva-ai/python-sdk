from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

import pydantic
from pydantic.types import conint

from corva.models.base import BaseContext, BaseEvent


class TaskStatus(Enum):
    fail = 'fail'
    success = 'success'


class TaskState(Enum):
    running = 'running'
    failed = 'failed'
    succeeded = 'succeeded'


class TaskData(pydantic.BaseModel):
    id: str
    state: TaskState
    fail_reason: Optional[Any] = None
    asset_id: int
    company_id: int
    app_id: int
    document_bucket: str
    properties: Dict[str, Any]
    payload: Dict[str, Any]


class UpdateTaskData(pydantic.BaseModel):
    fail_reason: Optional[str] = None
    payload: dict = {}


class TaskEvent(BaseEvent):
    id: Optional[str] = None
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported

    @staticmethod
    def from_raw_event(event: dict) -> TaskEvent:
        return pydantic.parse_obj_as(TaskEvent, event)


class TaskContext(BaseContext[TaskEvent]):
    pass
