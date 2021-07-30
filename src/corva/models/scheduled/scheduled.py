import pydantic

from corva.models.base import CorvaBaseEvent


class ScheduledEvent(CorvaBaseEvent):
    """Base class for scheduled event data."""


class ScheduledTimeEvent(ScheduledEvent):
    """Time scheduled event data.

    Time scheduled apps are run with time frequency set up by user
    (e.g., every 5 seconds, every 1 minute, etc.). The event contains time range
    (start and end times) that the app should fetch data for.

    Example: if the app is scheduled to run every 3 seconds, it will be called with
        the following events in order:
                 1st event   2nd event   3rd event
        Time ->    - - -       - - -       - - -
                   ↑   ↑       ↑   ↑       ↑   ↑
                   0   2       3   5       6   8
                 start end   start end   start end

    Attributes:
        asset_id: asset id.
        company_id: company id.
        start_time: left bound of the time range, covered by this event.
            Use inclusively.
        end_time: right bound of the time range, covered by this event.
            Use inclusively.
    """

    asset_id: int
    company_id: int
    start_time: int
    end_time: int = pydantic.Field(..., alias='schedule_start')

    class Config:
        allow_population_by_field_name = True


class ScheduledDepthEvent(ScheduledEvent):
    """Depth scheduled event data.

    Depth scheduled apps are run with depth frequency set up by user
    (e.g., every 5 ft., every 10 ft., etc.). The event contains depth range
    (top and bottom depths) that the app should fetch data for.

    Example: if the app is scheduled to run every 3 ft., it will be called with
        the following events in order:
        Depth ↓  1st event
              0 ft.  _     top depth
                     |
                     |
              3 ft.  |     bottom depth

                 2nd event
              3 ft.  |     top depth
                     |
              6 ft.  |     bottom depth

    Attributes:
        asset_id: asset id.
        company_id: company id.
        top_depth: start depth in ft., covered by this event. Use exclusively.
        bottom_depth: end depth in ft., covered by this event. Use inclusively.
        log_identifier: TODO
        interval: distance between two schedule triggers.
    """

    asset_id: int
    company_id: int
    top_depth: float
    bottom_depth: float
    log_identifier: str
    interval: float


class ScheduledNaturalEvent(ScheduledEvent):
    """Natural scheduled event data.

    TODO

    Attributes:
        asset_id: asset id.
        company_id: company id.
        schedule_start: time of the event creation.
        interval: time between two schedule triggers.
    """

    asset_id: int
    company_id: int
    schedule_start: int
    interval: float
