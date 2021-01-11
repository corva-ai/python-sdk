from __future__ import annotations

from functools import cached_property
from typing import Dict, List, Optional, Type

from pydantic import parse_raw_as

from corva.models.base import BaseContext, BaseData, BaseEvent


class RecordData(BaseData):
    hole_depth: Optional[float] = None
    weight_on_bit: Optional[int] = None
    state: Optional[str] = None


class Record(BaseData):
    timestamp: Optional[int] = None
    asset_id: int
    company_id: int
    version: int
    measured_depth: Optional[float] = None
    collection: str
    data: RecordData


class AppMetadata(BaseData):
    app_connection_id: int


class StreamEventMetadata(BaseData):
    app_stream_id: int
    source_type: Optional[str] = None
    apps: Dict[str, AppMetadata]


class StreamEventData(BaseData):
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
        if self.records:
            return self.records[-1].collection == 'wits.completed'

        return False


class StreamEvent(BaseEvent, StreamEventData):
    @staticmethod
    def from_raw_event(event: str) -> List[StreamEvent]:
        events = parse_raw_as(List[StreamEvent], event)

        return events


class StreamStateData(BaseData):
    last_processed_timestamp: int = -1
    last_processed_depth: float = -1


class StreamContext(BaseContext[StreamEvent, StreamStateData]):
    state_data_cls: Type[StreamStateData] = StreamStateData
    filter_by_timestamp: bool = False
    filter_by_depth: bool = False

    @cached_property
    def event(self) -> StreamEvent:
        from corva.utils import FilterStreamEvent

        event = super().event  # type: StreamEvent

        event.app_key = self.settings.APP_KEY

        event = FilterStreamEvent.run(
            event=event,
            by_timestamp=self.filter_by_timestamp,
            by_depth=self.filter_by_depth,
            last_processed_timestamp=self.state_data.last_processed_timestamp,
            last_processed_depth=self.state_data.last_processed_depth
        )

        return event
