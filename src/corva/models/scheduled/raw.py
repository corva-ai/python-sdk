from __future__ import annotations

import itertools
from typing import List, Optional, Union

from pydantic import Field, TypeAdapter, field_validator, model_validator
from typing_extensions import Self

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
    company_id: int = Field(..., alias='company')
    schedule_id: int = Field(..., alias='schedule')
    app_connection_id: int = Field(..., alias='app_connection')
    app_stream_id: int = Field(..., alias='app_stream')
    scheduler_type: SchedulerType
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Union[dict, List[List[dict]]]) -> List[RawScheduledEvent]:
        if isinstance(event, dict):
            event = [[event]]

        # flatten the event into 1d array
        flattened_event: List[dict] = list(itertools.chain(*event))

        parsed_raw_events = (
            TypeAdapter(List[RawScheduledEvent])
            .validate_python(flattened_event)
        )

        events = [
            parsed_raw_event.scheduler_type.raw_event.model_validate(sub_event)
            for parsed_raw_event, sub_event in zip(parsed_raw_events, flattened_event)
        ]

        return events

    def set_schedule_as_completed(self, api: Api) -> None:
        """Sets schedule as completed."""
        api.post(path=f'scheduler/{self.schedule_id}/completed')


class DataTimeMergeMetadata(CorvaBaseEvent):
    """
    For data time events we may need to store information about event merging
    (if merge_events=True is used in @scheduled)
    """

    start_time: int
    end_time: int
    number_of_merged_events: int


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
    merge_metadata: Optional[DataTimeMergeMetadata] = None

    # validators
    _set_schedule_start = (
        field_validator('schedule_start')
        (validators.from_ms_to_s)
    )

    @model_validator(mode="after")
    def set_start_time(self) -> Self:
        """Calculates start_time field if not provided."""

        if self.start_time is None:
            self.start_time = int(self.schedule_start - self.interval + 1)
        return self

    def rebuild_with_modified_times(
        self, start_time: int, end_time: int
    ) -> RawScheduledDataTimeEvent:
        raw_dict = self.model_dump(exclude_none=True, by_alias=True)
        raw_dict["start_time"] = start_time
        raw_dict["end_time"] = end_time
        raw_dict["schedule_start"] = end_time
        return RawScheduledDataTimeEvent.model_validate(raw_dict)


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
    interval: float = Field(..., alias='depth_milestone')
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
    _set_schedule_start = (
        field_validator('schedule_start')
        (validators.from_ms_to_s)
    )
