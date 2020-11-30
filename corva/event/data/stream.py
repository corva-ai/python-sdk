from typing import Any, Dict, List, Optional

from corva.event.data.base import BaseEventData


class Record(BaseEventData):
    timestamp: Optional[int] = None
    asset_id: int
    company_id: int
    version: int
    data: Dict[str, Any]
    measured_depth: Optional[float] = None
    collection: Optional[str] = None


class StreamEventData(BaseEventData):
    records: List[Record]
    metadata: Dict[str, Any]
    asset_id: int
    app_connection_id: int
    app_stream_id: int
    is_completed: bool
