from __future__ import annotations

import itertools
from typing import List

import pydantic

from corva.models.base import BaseContext, CorvaBaseEvent, RawBaseEvent


class ScheduledEvent(CorvaBaseEvent):
    """Scheduled event data.

    Contains time range data to be processed.
    E.g., if your app is scheduled to be run every 5 minutes.
        It will be called with the following events in order:
                   1st event     2nd event     3rd event
        Time ->    [-----]       [-----]       [-----]
                   ↑     ↑       ↑     ↑       ↑     ↑
                   start end     start end     start end
        The app may then fetch and process the data
        for the time range received in event.

    Attributes:
        asset_id: asset id.
        start_time: left bound of the time range, covered by this event. Use inclusively.
        end_time: right bound of the time range, covered by this event. Use inclusively.
    """

    asset_id: int
    start_time: int
    end_time: int = pydantic.Field(..., alias='schedule_start')

    class Config:
        allow_population_by_field_name = True


class RawScheduledEvent(CorvaBaseEvent, RawBaseEvent):
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
    start_time: int = None

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

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_start_time(cls, values: dict) -> dict:
        """Calculates start_time field."""

        values["start_time"] = int(values["schedule_start"] - values["interval"] + 1)
        return values

    @staticmethod
    def from_raw_event(event: List[List[dict]]) -> List[RawScheduledEvent]:
        # flatten the event into 1d array
        event: List[dict] = list(itertools.chain(*event))

        events = pydantic.parse_obj_as(List[RawScheduledEvent], event)

        return events


class ScheduledContext(BaseContext[RawScheduledEvent]):
    pass
