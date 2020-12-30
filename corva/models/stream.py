from typing import Dict, List, Optional

from corva.models.base import BaseContext, BaseEventData, ListEvent
from corva.state.redis_state import RedisState


class StreamContext(BaseContext):
    state: RedisState


class RecordData(BaseEventData):
    hole_depth: Optional[float] = None
    weight_on_bit: Optional[int] = None
    state: Optional[str] = None


class Record(BaseEventData):
    timestamp: Optional[int] = None
    asset_id: int
    company_id: int
    version: int
    measured_depth: Optional[float] = None
    collection: str
    data: RecordData


class AppMetadata(BaseEventData):
    app_connection_id: int


class StreamEventMetadata(BaseEventData):
    app_stream_id: int
    apps: Dict[str, AppMetadata]


class StreamEventData(BaseEventData):
    app_key: Optional[str] = None
    records: List[Record]
    metadata: StreamEventMetadata

    @property
    def asset_id(self) -> int:
        return self.records[0].asset_id

    @property
    def app_connection_id(self) -> int:
        return self.metadata.apps[self.app_key].app_connection_id

    @property
    def app_stream_id(self) -> int:
        return self.metadata.app_stream_id

    @property
    def is_completed(self) -> bool:
        if len(self.records):
            return self.records[-1].collection == 'wits.completed'

        return False


class StreamEvent(ListEvent[StreamEventData]):
    pass
