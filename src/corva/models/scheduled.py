from __future__ import annotations

import itertools
from typing import List, Optional

import pydantic

from corva.models.base import BaseContext, CorvaBaseModel, RawBaseEvent


class ScheduledEvent(CorvaBaseModel):
    """Scheduled event data.

    Attributes:
        asset_id: asset id.
        time_from: the first not processed time. Unix timestamp.
        time_to: the last not processed time. Unix timestamp.
    """

    asset_id: int
    time_from: int
    time_to: int = pydantic.Field(..., alias='schedule_start')

    class Config:
        allow_population_by_field_name = True


class RawScheduledEvent(RawBaseEvent):
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
    time_from: int = None

    @pydantic.validator('time_from', always=True)
    def set_time_from(cls, v: Optional[int], values) -> int:
        """Calculates time_from field."""

        if 'schedule_start' in values and 'interval' in values:
            return values['schedule_start'] - values['interval'] + 1

        raise ValueError('Cannot set time_from field.')

    @pydantic.validator('schedule_start')
    def set_schedule_start(cls, v: int) -> int:
        """Casts Unix timestamp from milliseconds to seconds.

        Casts Unix timestamp from millisecond to seconds, if provided timestamp is in
          milliseconds.
        """

        # 1 January 10000 00:00:00 - first date to not fit into the datetime instance
        if v >= 253402300800:
            v //= 1000

        return v

    @staticmethod
    def from_raw_event(event: List[List[dict]]) -> List[RawScheduledEvent]:
        # flatten the event into 1d array
        event: List[dict] = list(itertools.chain(*event))

        events = pydantic.parse_obj_as(List[RawScheduledEvent], event)

        return events


class ScheduledContext(BaseContext[RawScheduledEvent]):
    pass
