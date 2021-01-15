from __future__ import annotations

from typing import Dict, List, Optional, Type

import pydantic

from corva.models.base import BaseContext, BaseEventData
from corva.state.redis_state import RedisState

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
    app_version: Optional[int] = None


class StreamEventMetadata(BaseData):
    app_stream_id: int
    source_type: Optional[str] = None
    apps: Dict[str, AppMetadata]


class StreamEventData(BaseData):
    app_key: Optional[str] = None
    records: List[Record]
    metadata: StreamEventMetadata
    asset_id: int = None

    @validator('asset_id', pre=True, always=True)
    def set_asset_id(cls, v, values):
        """dynamically sets value for asset_id

        asset_id could've been defined as property like below.

        @property
        def asset_id(self) -> Optional[int]:
            return self.records[0].asset_id if self.records else None

        The issue with the above method is:
         after filtering, we may end up with empty records. Which leads to asset_id becoming None.
         Using this validator we are able to dynamically set and store value of asset_id,
         no matter what happens to records.
        """

        records = values['records']  # type: List[Record]
        if records:
            return records[0].asset_id

        raise ValueError('Can\'t determine asset_id as records are empty (which should not happen).')

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
    def from_raw_event(event: str, **kwargs) -> List[StreamEvent]:
        app_key = kwargs['app_key']

        events = pydantic.parse_raw_as(List[StreamEvent], event)  # type: List[StreamEvent]

        for event in events:
            event.app_key = app_key

        return events

    @staticmethod
    def filter(
         event: StreamEvent, by_timestamp: bool, by_depth: bool, last_timestamp: int, last_depth: float
    ) -> StreamEvent:
        records = event.records

        if event.is_completed:
            records = records[:-1]  # remove "completed" record

        new_records = []
        for record in records:
            if by_timestamp and record.timestamp <= last_timestamp:
                continue
            if by_depth and record.measured_depth <= last_depth:
                continue

            new_records.append(record)

        return event.copy(update={'records': new_records}, deep=True)


class StreamStateData(BaseData):
    last_processed_timestamp: int = -1
    last_processed_depth: float = -1


class StreamContext(BaseContext[StreamEvent, StreamStateData]):
    cache_data_cls: Type[StreamStateData] = StreamStateData
    filter_by_timestamp: bool = False
    filter_by_depth: bool = False

    @root_validator(pre=True)
    def check_one_active_filter_at_most(cls, values):
        if values['filter_by_timestamp'] and values['filter_by_depth']:
            raise ValueError('filter_by_timestamp and filter_by_depth can\'t be set to True together.')

        return values
