from __future__ import annotations

import itertools
from typing import List, Literal, Optional

import pydantic

from corva.models.base import BaseContext, BaseEvent


class ScheduledEvent(BaseEvent):
    asset_id: int
    interval: int = pydantic.Field(
        ..., description='Time in seconds between two schedule triggers'
    )
    schedule_id: int = pydantic.Field(..., alias='schedule')
    schedule_start: int = pydantic.Field(
        ..., description='Unix timestamp, when the schedule was triggered'
    )
    app_connection_id: int = pydantic.Field(..., alias='app_connection')
    app_stream_id: int = pydantic.Field(..., alias='app_stream')

    # optional fields
    schedule_end: Optional[int] = pydantic.Field(
        None, description='Schedule end timestamp'
    )
    cron_string: Optional[str] = pydantic.Field(
        None, description='Cron expression representing the schedule'
    )
    environment: Optional[
        Literal['testing', 'localhost', 'qa', 'staging', 'production']
    ] = None
    app_id: Optional[int] = pydantic.Field(None, alias='app')
    app_key: Optional[str] = pydantic.Field(None, description='Unique app identifier')
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
        description='Unique Log Identifier, only available for depth-based streams',
    )
    day_shift_start: Optional[str] = pydantic.Field(
        None, description='Day shift start time'
    )

    @pydantic.validator('schedule_start', 'schedule_end')
    def set_timestamp(cls, v: int) -> int:
        """Casts Unix timestamp from milliseconds to seconds.

        Casts Unix timestamp from millisecond to seconds, if provided timestamp is in
          milliseconds.
        """

        # 1 January 10000 00:00:00 - first date to not fit into the datetime instance
        if v >= 253402300800:
            v //= 1000

        return v

    @staticmethod
    def from_raw_event(event: List[List[dict]]) -> List[ScheduledEvent]:
        events = pydantic.parse_obj_as(List[List[ScheduledEvent]], event)

        # raw event from queue comes in from of 2d array of ScheduledEvent
        # flatten parsed event into 1d array of ScheduledEvent, which is an expected return type
        events = list(itertools.chain(*events))

        return events


class ScheduledContext(BaseContext[ScheduledEvent]):
    pass
