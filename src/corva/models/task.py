from __future__ import annotations

from enum import Enum
from typing import Optional

import pydantic
from pydantic.types import conint

from corva.models.base import BaseContext, BaseEvent, CorvaBaseModel


class TaskStatus(Enum):
    fail = 'fail'
    success = 'success'


class TaskState(Enum):
    running = 'running'
    failed = 'failed'
    succeeded = 'succeeded'


class TaskEvent(CorvaBaseModel):
    id: str
    asset_id: int
    company_id: int
    state: TaskState
    app_id: int
    document_bucket: str
    properties: dict = {}
    payload: dict = {}
    fail_reason: Optional[str] = None


class RawTaskEvent(BaseEvent):
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported

    @staticmethod
    def from_raw_event(event: dict) -> RawTaskEvent:
        return pydantic.parse_obj_as(RawTaskEvent, event)


class TaskContext(BaseContext[RawTaskEvent]):
    pass
