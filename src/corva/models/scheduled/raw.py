from __future__ import annotations

import itertools
from typing import List, Optional, Union

import pydantic

from corva.api import Api
from corva.models import validators
from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.rerun import RerunDepth, RerunTime
from corva.models.scheduled.scheduler_type import SchedulerType


class RawScheduledEvent(CorvaBaseEvent, RawBaseEvent):
    """Base class for raw scheduled event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        schedule_id: schedule id.
        app_connection_id: app connection id.
        app_stream_id: app stream id.
        scheduler_type: type of scheduled event.
    """

    asset_id: int
    company_id: int = pydantic.Field(..., alias='company')
    schedule_id: int = pydantic.Field(..., alias='schedule')
    app_connection_id: int = pydantic.Field(..., alias='app_connection')
    app_stream_id: int = pydantic.Field(..., alias='app_stream')
    scheduler_type: SchedulerType
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Union[dict, List[List[dict]]]) -> List[RawScheduledEvent]:
        if isinstance(event, dict):
            event = [[event]]

        # flatten the event into 1d array
        flattened_event: List[dict] = list(itertools.chain(*event))

        parsed_raw_events = pydantic.parse_obj_as(
            List[RawScheduledEvent], flattened_event
        )

        events = [
            parsed_raw_event.scheduler_type.raw_event.parse_obj(sub_event)
            for parsed_raw_event, sub_event in zip(parsed_raw_events, flattened_event)
        ]

        return events

    def set_schedule_as_completed(self, api: Api) -> None:
        """Sets schedule as completed."""
        api.post(path=f'scheduler/{self.schedule_id}/completed')


class RawScheduledDataTimeEvent(RawScheduledEvent):
    """Raw data time scheduled event data.

    Attributes:
        schedule_start: Unix timestamp, when the schedule was triggered.
        start_time: left bound of the time range, covered by this event.
            Use inclusively.
        interval: time between two schedule triggers.
        rerun: rerun metadata.
    """

    schedule_start: int
    start_time: int = None  # type: ignore
    interval: float
    rerun: Optional[RerunTime] = None

    # validators
    _set_schedule_start = pydantic.validator('schedule_start', allow_reuse=True)(
        validators.from_ms_to_s
    )

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_start_time(cls, values: dict) -> dict:
        """Calculates start_time field."""

        values["start_time"] = int(values["schedule_start"] - values["interval"] + 1)
        return values


class RawScheduledDepthEvent(RawScheduledEvent):
    """Raw depth scheduled event data.

    Attributes:
        top_depth: start depth in ft., covered by this event. Use exclusively.
        bottom_depth: end depth in ft., covered by this event. Use inclusively.
        log_identifier: app stream log identifier. Used to scope data by stream,
            asset might be connected to multiple depth based logs.
        interval: distance between two schedule triggers.
        rerun: rerun metadata.
    """

    top_depth: float
    bottom_depth: float
    log_identifier: str
    interval: float = pydantic.Field(..., alias='depth_milestone')
    rerun: Optional[RerunDepth] = None


class RawScheduledNaturalTimeEvent(RawScheduledEvent):
    """Raw natural scheduled event data.

    Attributes:
        schedule_start: Unix timestamp, when the schedule was triggered.
        interval: time between two schedule triggers.
        rerun: rerun metadata.
    """

    schedule_start: int
    interval: float
    rerun: Optional[RerunTime] = None

    # validators
    _set_schedule_start = pydantic.validator('schedule_start', allow_reuse=True)(
        validators.from_ms_to_s
    )
