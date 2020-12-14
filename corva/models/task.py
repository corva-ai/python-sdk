from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel
from pydantic.types import conint

from corva.models.base import BaseContext, BaseEventData


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


class TaskContext(BaseContext):
    task: TaskData
    task_result: dict = {}


class TaskEventData(BaseEventData):
    id: Optional[str] = None
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported
