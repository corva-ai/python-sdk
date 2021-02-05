from __future__ import annotations

from typing import Dict, List, Optional, Union

import pydantic

from corva.models.base import BaseContext, BaseEvent, CorvaBaseModel


class RecordData(CorvaBaseModel):
    hole_depth: Optional[float] = None
    weight_on_bit: Optional[int] = None
    state: Optional[str] = None


class Record(CorvaBaseModel):
    timestamp: Optional[int] = None
    asset_id: int
    company_id: int
    version: int
    measured_depth: Optional[float] = None
    collection: str
    data: RecordData


class AppMetadata(CorvaBaseModel):
    app_connection_id: int


class StreamEventMetadata(CorvaBaseModel):
    app_stream_id: int
    source_type: Optional[str] = None
    apps: Dict[str, AppMetadata]


class StreamEventData(CorvaBaseModel):
    app_key: Optional[str] = None
    records: List[Record]
    metadata: StreamEventMetadata
    asset_id: int = None  # type hint reason: https://pydantic-docs.helpmanual.io/usage/validators/#validate-always

    @pydantic.validator('asset_id', pre=True, always=True)
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

        records = values.get('records', [])  # type: List[Record]

        if len(records) == 0:
            raise ValueError('Can\'t set asset_id as records are empty (which should not happen).')

        return records[0].asset_id

    @property
    def app_connection_id(self) -> int:
        return self.metadata.apps[self.app_key].app_connection_id

    @property
    def app_stream_id(self) -> int:
        return self.metadata.app_stream_id

    @property
    def is_completed(self) -> bool:
        """there can only be 1 completed record, always located at the end of the list"""

        if self.records:
            return self.records[-1].collection == 'wits.completed'

        return False


class StreamEvent(BaseEvent, StreamEventData):
    @staticmethod
    def from_raw_event(event: Union[str, List], **kwargs) -> List[StreamEvent]:
        app_key = kwargs['app_key']

        parse = pydantic.parse_obj_as
        if isinstance(event, str):
            parse = pydantic.parse_raw_as

        events = parse(List[StreamEvent], event)  # type: List[StreamEvent]

        for event in events:
            event.app_key = app_key

        return events

    @classmethod
    def filter(
         cls, event: StreamEvent, by_timestamp: bool, by_depth: bool, last_timestamp: int, last_depth: float
    ) -> StreamEvent:
        records = event.records

        if event.is_completed:
            # there can only be 1 completed record, always located at the end of the list
            records = records[:-1]  # remove "completed" record

        new_records = list(
            cls._filter_records(
                records=records,
                by_timestamp=by_timestamp,
                by_depth=by_depth,
                last_timestamp=last_timestamp,
                last_depth=last_depth
            )
        )

        return event.copy(update={'records': new_records}, deep=True)

    @staticmethod
    def _filter_records(
         records: List[Record], by_timestamp: bool, by_depth: bool, last_timestamp: int, last_depth: float
    ):
        for record in records:
            if by_timestamp and record.timestamp <= last_timestamp:
                continue

            if by_depth and record.measured_depth <= last_depth:
                continue

            yield record


class StreamStateData(CorvaBaseModel):
    last_processed_timestamp: Optional[int] = None
    last_processed_depth: Optional[float] = None


class StreamContext(BaseContext[StreamEvent]):
    filter_by_timestamp: bool = False
    filter_by_depth: bool = False

    @property
    def cache_data(self) -> StreamStateData:
        state_data_dict = self.cache.load_all()
        return StreamStateData(**state_data_dict)

    def store_cache_data(self, cache_data: StreamStateData) -> int:
        if cache_data := cache_data.dict(exclude_defaults=True, exclude_none=True):
            return self.cache.store(mapping=cache_data)

        return 0

    @pydantic.root_validator(pre=True)
    def check_one_active_filter_at_most(cls, values):
        if values['filter_by_timestamp'] and values['filter_by_depth']:
            raise ValueError('filter_by_timestamp and filter_by_depth can\'t be set to True together.')

        return values
