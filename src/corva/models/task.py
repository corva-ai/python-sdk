from __future__ import annotations

import pydantic
from pydantic.types import conint

from corva.models.base import BaseContext, CorvaBaseModel, RawBaseEvent


class TaskEvent(CorvaBaseModel):
    """Task event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        properties: custom task data.
    """

    asset_id: int
    company_id: int
    properties: dict = {}


class RawTaskEvent(RawBaseEvent):
    task_id: str
    version: conint(ge=2, le=2)  # only utils API v2 supported

    @staticmethod
    def from_raw_event(event: dict) -> RawTaskEvent:
        return pydantic.parse_obj_as(RawTaskEvent, event)


class TaskContext(BaseContext[RawTaskEvent]):
    pass
