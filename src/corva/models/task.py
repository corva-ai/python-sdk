from __future__ import annotations

import enum
from typing import List

import pydantic
import requests

from corva.api import Api
from corva.models.base import CorvaBaseEvent, RawBaseEvent


class TaskStatus(enum.Enum):
    fail = 'fail'
    success = 'success'


class TaskEvent(CorvaBaseEvent):
    """Task event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        properties: custom task data.
    """

    asset_id: int
    company_id: int
    properties: dict = {}


class RawTaskEvent(CorvaBaseEvent, RawBaseEvent):
    task_id: str
    has_secrets: bool = False
    version: int = pydantic.Field(..., le=2, ge=2)  # only utils API v2 supported

    @staticmethod
    def from_raw_event(event: dict) -> List[RawTaskEvent]:
        return [pydantic.parse_obj_as(RawTaskEvent, event)]

    def get_task_event(self, api: Api) -> TaskEvent:
        response = api.get(path=f'v2/tasks/{self.task_id}')
        response.raise_for_status()

        return TaskEvent(**response.json())

    def update_task_data(
        self, api: Api, status: TaskStatus, data: dict
    ) -> requests.Response:
        """Updates the task."""

        return api.put(path=f'v2/tasks/{self.task_id}/{status.value}', data=data)
