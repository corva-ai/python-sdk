from __future__ import annotations

import itertools
from datetime import datetime
from typing import List, Literal, Optional

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

    # optional fields
    cron_string: Optional[str] = pydantic.Field(
        None, description='Cron expression representing the schedule'
    )
    environment: Optional[Literal['qa', 'staging', 'production']] = None
    app_id: Optional[int] = pydantic.Field(None, alias='app')
    app_key: Optional[str] = pydantic.Field(None, description='Unique app identifier')
    app_connection_id: Optional[int] = pydantic.Field(None, alias='app_connection')
    app_stream_id: Optional[int] = pydantic.Field(None, alias='app_stream')
    source_type: Optional[
        Literal['drilling', 'drillout', 'frac', 'wireline']
    ] = pydantic.Field(None, description='Source Data Type')
    company_id: Optional[int] = pydantic.Field(None, alias='company')
    provider: Optional[str] = pydantic.Field(
        None, description='Company name identifier'
    )
    asset_name: Optional[str] = None
    asset_type: Optional[Literal['Well']] = None
    timezone: Optional[str] = pydantic.Field(None, description='Time zone of the asset')
    log_type: Optional[Literal['time', 'depth']] = pydantic.Field(
        None, description='Source Log Type'
    )
    log_identifier: Optional[str] = pydantic.Field(
        None,
        description='Unique Log Identifier, only available for depth based streams',
    )
    day_shift_start: Optional[str] = pydantic.Field(
        None, description='Day shift start time'
    )

    @staticmethod
    def from_raw_event(event: str, **kwargs) -> List[ScheduledEvent]:
        events = pydantic.parse_raw_as(List[List[ScheduledEvent]], event)

        # raw event from queue comes in from of 2d array of ScheduledEvent
        # flatten parsed event into 1d array of ScheduledEvent, which is an expected return type
        events = list(itertools.chain(*events))

        return events


class ScheduledContext(BaseContext[ScheduledEvent]):
    pass
