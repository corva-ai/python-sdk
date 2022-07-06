from typing import TYPE_CHECKING, Optional, Sequence

import pydantic

from corva.models.base import CorvaBaseEvent
from corva.models.rerun import RerunDepth, RerunTime


class StreamTimeRecord(CorvaBaseEvent):
    """Stream time record data.

    Attributes:
        timestamp: Unix timestamp.
        data: record data.
        metadata: record metadata.
    """

    timestamp: int
    data: dict = {}
    metadata: dict = {}


class StreamDepthRecord(CorvaBaseEvent):
    """Stream depth record data.

    Attributes:
        measured_depth: measured depth (ft).
        data: record data.
        metadata: record metadata.
    """

    measured_depth: float
    data: dict = {}
    metadata: dict = {}


if TYPE_CHECKING:
    RecordsTime = Sequence[StreamTimeRecord]
    RecordsDepth = Sequence[StreamDepthRecord]
else:
    RecordsTime = pydantic.conlist(StreamTimeRecord, min_items=1)
    RecordsDepth = pydantic.conlist(StreamDepthRecord, min_items=1)


class StreamEvent(CorvaBaseEvent):
    """Base stream event data."""


class StreamTimeEvent(StreamEvent):
    """Stream time event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        records: data records.
        rerun: rerun metadata.
    """

    asset_id: int
    company_id: int
    records: RecordsTime
    rerun: Optional[RerunTime] = None


class StreamDepthEvent(StreamEvent):
    """Stream depth event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        records: data records.
        rerun: rerun metadata.
    """

    asset_id: int
    company_id: int
    records: RecordsDepth
    rerun: Optional[RerunDepth] = None
