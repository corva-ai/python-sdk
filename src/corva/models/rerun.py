from typing import Optional

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

    @pydantic.field_validator("start", mode="before")
    @classmethod
    def _set_start(cls, v):
        return validators.from_ms_to_s(v)

    @pydantic.field_validator("end", mode="before")
    @classmethod
    def _set_end(cls, v):
        return validators.from_ms_to_s(v)


class RerunTime(base.CorvaBaseEvent):
    """Rerun metadata for time event.

    Attributes:
        id: rerun id.
        range: rerun time range.
        invoke: invoke counter.
        total: total invoke count for the rerun.
    """

    id: Optional[int] = None
    range: RerunTimeRange
    invoke: int
    total: int


class RerunDepth(base.CorvaBaseEvent):
    """Rerun metadata for depth event.

    Attributes:
        id: rerun id.
        range: rerun depth range.
        invoke: invoke counter.
        total: total invoke count for the rerun.
    """

    id: Optional[int] = None
    range: RerunDepthRange
    invoke: int
    total: int
