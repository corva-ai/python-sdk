from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel
from pydantic.types import conint

from corva.models.base import BaseContext, BaseEventData


class TaskData(BaseModel):
    id: str
    state: Literal['running', 'failed', 'succeeded']
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
    save_data: dict = {}


class TaskEventData(BaseEventData):
    id: Optional[str] = None
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported
