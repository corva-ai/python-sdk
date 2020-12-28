from typing import Dict, List, Optional

from corva.models.base import BaseContext, BaseEventData, ListEvent, BaseStateData


class StreamEventData(BaseEventData):
    class Record(BaseEventData):
        class Data(BaseEventData):
            hole_depth: Optional[float] = None
            weight_on_bit: Optional[int] = None
            state: Optional[str] = None

        timestamp: Optional[int] = None
        asset_id: int
        company_id: int
        version: int
        measured_depth: Optional[float] = None
        collection: str
        data: Data

    class Metadata(BaseEventData):
        class AppData(BaseEventData):
            app_connection_id: int

        app_stream_id: int
        apps: Dict[str, AppData]

    app_key: Optional[str] = None
    records: List[Record]
    metadata: Metadata

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
        return self.records[-1].collection == 'wits.completed'


class StreamEvent(ListEvent[StreamEventData]):
    pass


class StreamStateData(BaseStateData):
    last_processed_timestamp: int = -1
    last_processed_depth: float = -1


class StreamContext(BaseContext[StreamEvent, StreamStateData]):
    pass
