from typing import Optional

import pydantic

from corva.models.base import CorvaBaseEvent
from corva.models.rerun import RerunDepth, RerunTime


class ScheduledEvent(CorvaBaseEvent):
    """Base class for scheduled event data."""


class ScheduledDataTimeEvent(ScheduledEvent):
    """Data time scheduled event data.

    Data time scheduled apps are run when there is a time span worth of new data.
    The time span is specified by user (e.g., 1 minute, 5 minutes, etc.). The event
    contains time range (start and end times) that there is a new data for. The app
    may then fetch and use that data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        start_time: left bound of the time range, covered by this event.
            Use inclusively.
        end_time: right bound of the time range, covered by this event.
            Use inclusively.
        rerun: rerun metadata.
    """

    asset_id: int
    company_id: int
    start_time: int
    end_time: int = pydantic.Field(..., alias='schedule_start')
    rerun: Optional[RerunTime] = None

    class Config:
        allow_population_by_field_name = True


class ScheduledDepthEvent(ScheduledEvent):
    """Depth scheduled event data.

    Depth scheduled apps are run when there is a distance worth of new data.
    The distance is specified by user (e.g., 5 ft., 10 ft., etc.). The event contains
    depth range (top and bottom depths) that there is a new data for. The app
    may then fetch and use that data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        top_depth: start depth in ft., covered by this event. Use exclusively.
        bottom_depth: end depth in ft., covered by this event. Use inclusively.
        log_identifier: app stream log identifier. Used to scope data by stream,
            asset might be connected to multiple depth based logs.
        interval: distance between two schedule triggers.
        rerun: rerun metadata.
    """

    asset_id: int
    company_id: int
    top_depth: float
    bottom_depth: float
    log_identifier: str
    interval: float
    rerun: Optional[RerunDepth] = None


class ScheduledNaturalTimeEvent(ScheduledEvent):
    """Natural time scheduled event data.

    Natural time scheduled apps are run with time frequency set up by user based on
    the actual time instead of data time (e.g., every 1 minute, every 5 minutes, etc.).

    Attributes:
        asset_id: asset id.
        company_id: company id.
        schedule_start: schedule trigger time.
        interval: time between two schedule triggers.
        rerun: rerun metadata.
    """

    asset_id: int
    company_id: int
    schedule_start: int
    interval: float
    rerun: Optional[RerunTime] = None
