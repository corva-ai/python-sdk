import pydantic

from corva.models import base, validators


class RerunDepthRange(base.CorvaBaseEvent):
    """Rerun depth range metadata.

    Attributes:
        start: start depth.
        end: end depth.
    """

    start: float
    end: float


class RerunTimeRange(base.CorvaBaseEvent):
    """Rerun time range metadata.

    Attributes:
        start: start time.
        end: end time.
    """

    start: int
    end: int

    # validators
    _set_start = pydantic.validator('start', allow_reuse=True)(validators.from_ms_to_s)
    _set_end = pydantic.validator('end', allow_reuse=True)(validators.from_ms_to_s)


class RerunTime(base.CorvaBaseEvent):
    """Rerun metadata for time event.

    Attributes:
        range: rerun time range.
        invoke: invoke counter.
        total: total invoke count for the rerun.
    """

    range: RerunTimeRange
    invoke: int
    total: int


class RerunDepth(base.CorvaBaseEvent):
    """Rerun metadata for depth event.

    Attributes:
        range: rerun depth range.
        invoke: invoke counter.
        total: total invoke count for the rerun.
    """

    range: RerunDepthRange
    invoke: int
    total: int
