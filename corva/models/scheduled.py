from __future__ import annotations

from datetime import datetime
from itertools import chain
from typing import List, Optional

from pydantic import Field, parse_raw_as

from corva.models.base import BaseContext, BaseData, BaseEvent


class ScheduledEventData(BaseData):
    type: Optional[str] = None
    collection: Optional[str] = None
    cron_string: str
    environment: str
    app: int
    app_key: str
    app_version: Optional[int]
    app_connection_id: int = Field(alias='app_connection')
    app_stream_id: int = Field(alias='app_stream')
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
        events = parse_raw_as(List[List[ScheduledEvent]], event)

        # raw event from queue comes in from of 2d array of ScheduledEvent
        # flatten parsed event into 1d array of ScheduledEvent, which is an expected return type
        events = list(chain(*events))

        return events


class ScheduledContext(BaseContext[ScheduledEvent, BaseData]):
    pass
