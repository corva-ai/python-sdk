from __future__ import annotations

import itertools
from datetime import datetime
from typing import List, Optional

import pydantic

from corva.models.base import BaseContext, BaseEvent, CorvaBaseModel


class ScheduledEventData(CorvaBaseModel):
    type: Optional[str] = None
    collection: Optional[str] = None
    cron_string: str
    environment: str
    app: int
    app_key: str
    app_connection_id: int = pydantic.Field(alias='app_connection')
    app_stream_id: int = pydantic.Field(alias='app_stream')
    source_type: str
    company: int
    provider: str
    api_url: Optional[str] = None
    api_key: Optional[str] = None
    schedule: int
    interval: int
    schedule_start: datetime
    schedule_end: datetime
    asset_id: int
    asset_name: str
    asset_type: str
    timezone: str
    log_type: str
    log_identifier: Optional[str] = None
    day_shift_start: Optional[str] = None


class ScheduledEvent(BaseEvent, ScheduledEventData):
    @staticmethod
    def from_raw_event(event: str, **kwargs) -> List[ScheduledEvent]:
        events = pydantic.parse_raw_as(List[List[ScheduledEvent]], event)

        # raw event from queue comes in from of 2d array of ScheduledEvent
        # flatten parsed event into 1d array of ScheduledEvent, which is an expected return type
        events = list(itertools.chain(*events))

        return events


class ScheduledContext(BaseContext[ScheduledEvent]):
    pass
