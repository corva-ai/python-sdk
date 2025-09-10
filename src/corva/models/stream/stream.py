from typing import List, TYPE_CHECKING, Optional, Sequence

from corva.models.base import CorvaBaseEvent
from corva.models.rerun import RerunDepth, RerunTime
from pydantic import Field
from typing_extensions import Annotated


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
    RecordsTime = Annotated[List[StreamTimeRecord], Field(min_items=1)]
    RecordsDepth = Annotated[List[StreamDepthRecord], Field(min_items=1)]


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
    # TODO: remove `Optional` in v2 as it was added for backward compatibility.
    log_identifier: Optional[str] = None
    rerun: Optional[RerunDepth] = None
