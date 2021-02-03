from __future__ import annotations

import itertools
from datetime import datetime
from typing import List

import pydantic

from corva.models.base import BaseContext, BaseEvent


class ScheduledEvent(BaseEvent):
    asset_id: int
    interval: int = pydantic.Field(
        ..., description='Scheduled interval (parsed cron string in seconds)'
    )
    schedule_id: int = pydantic.Field(..., alias='schedule')
    schedule_start: datetime
    schedule_end: datetime

    @staticmethod
    def from_raw_event(event: str, **kwargs) -> List[ScheduledEvent]:
        events = pydantic.parse_raw_as(List[List[ScheduledEvent]], event)

        # raw event from queue comes in from of 2d array of ScheduledEvent
        # flatten parsed event into 1d array of ScheduledEvent, which is an expected return type
        events = list(itertools.chain(*events))

        return events


class ScheduledContext(BaseContext[ScheduledEvent]):
    pass
