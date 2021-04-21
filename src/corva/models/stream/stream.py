import pydantic

from corva.models.base import CorvaBaseEvent


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


class StreamEvent(CorvaBaseEvent):
    """Stream event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        records: data records.
    """

    asset_id: int
    company_id: int
    records: pydantic.conlist(StreamBaseRecord, min_items=1)


class StreamTimeEvent(StreamEvent):
    """Stream time event data."""

    records: pydantic.conlist(StreamTimeRecord, min_items=1)


class StreamDepthEvent(StreamEvent):
    """Stream depth event data."""

    records: pydantic.conlist(StreamDepthRecord, min_items=1)
