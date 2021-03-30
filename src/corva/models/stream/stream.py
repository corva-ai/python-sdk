from typing import Generic, List, TypeVar

from corva.models.base import CorvaBaseEvent, CorvaBaseGenericEvent


class StreamBaseRecord(CorvaBaseEvent):
    """Stream base record data."""


class StreamTimeRecord(StreamBaseRecord):
    """Stream time record data.

    Attributes:
        timestamp: Unix timestamp.
        data: record data.
        metadata: record metadata.
    """

    timestamp: int
    data: dict = {}
    metadata: dict = {}


class StreamDepthRecord(StreamBaseRecord):
    """Stream depth record data.

    Attributes:
        measured_depth: measured depth (ft).
        data: record data.
        metadata: record metadata.
    """

    measured_depth: float
    data: dict = {}
    metadata: dict = {}


StreamBaseRecordTV = TypeVar('StreamBaseRecordTV', bound=StreamBaseRecord)


class StreamEvent(CorvaBaseGenericEvent, Generic[StreamBaseRecordTV]):
    """Stream event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        records: data records.
    """

    asset_id: int
    company_id: int
    records: List[StreamBaseRecordTV]


class StreamTimeEvent(StreamEvent[StreamTimeRecord]):
    """Stream time event data."""


class StreamDepthEvent(StreamEvent[StreamDepthRecord]):
    """Stream depth event data."""
