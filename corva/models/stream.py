from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import parse_raw_as

from corva.models.base import BaseContext, BaseData, ListEvent
from corva.utils import FilterStreamEvent


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
        if len(self.records):
            return self.records[-1].collection == 'wits.completed'

        return False


class StreamEvent(ListEvent[StreamEventData]):
    @staticmethod
    def from_raw_event(event: str, **kwargs) -> StreamEvent:
        app_key: str = kwargs['app_key']

        parsed = parse_raw_as(List[StreamEventData], event)  # type: List[StreamEventData]

        for data in parsed:
            data.app_key = app_key

        return StreamEvent(parsed)


class StreamStateData(BaseData):
    last_processed_timestamp: int = -1
    last_processed_depth: float = -1


class StreamContext(BaseContext[StreamEvent, StreamStateData]):
    filter_by_timestamp: bool = False
    filter_by_depth: bool = False

    @property
    def event(self) -> StreamEvent:
        if self._event is None:
            event = super().event

            self._event = FilterStreamEvent.run(
                event=event,
                by_timestamp=self.filter_by_timestamp,
                by_depth=self.filter_by_depth,
                last_processed_timestamp=self.state_data.last_processed_timestamp,
                last_processed_depth=self.state_data.last_processed_depth
            )

        return self._event
