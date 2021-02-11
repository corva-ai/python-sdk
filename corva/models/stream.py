from __future__ import annotations

import copy
import enum
from typing import Dict, List, Literal, Optional, Union

import pydantic

from corva.models.base import BaseContext, BaseEvent, CorvaBaseModel


class FilterMode(enum.Enum):
    timestamp = 'timestamp'
    depth = 'depth'


class Record(CorvaBaseModel):
    asset_id: int

    data: Optional[dict] = None
    metadata: Optional[dict] = None

    timestamp: Optional[int] = pydantic.Field(
        None,
        description='Timestamp (Unix epoch time). Only present for time based streams',
    )
    measured_depth: Optional[float] = pydantic.Field(
        None,
        description='Measured depth (ft). Only present for depth based streams',
    )

    version: Optional[int] = pydantic.Field(None, description='Version of the record')
    app: Optional[str] = pydantic.Field(
        None, description='App which generated the data'
    )
    company_id: Optional[int] = None
    provider: Optional[str] = None
    collection: Optional[str] = None

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def require_timestamp_or_measured_depth(cls, values):
        # at least one of timestamp or measured_depth is required
        if values.get('timestamp') is None and values.get('measured_depth') is None:
            raise ValueError('At least one of timestamp or measured_depth is required')

        return values


class AppMetadata(CorvaBaseModel):
    app_connection_id: int


class StreamEventMetadata(CorvaBaseModel):
    app_stream_id: int
    apps: Dict[str, AppMetadata]

    source_type: Optional[
        Literal['drilling', 'drillout', 'frac', 'wireline']
    ] = pydantic.Field(None, description='Source Data Type')
    log_type: Optional[Literal['time', 'depth']] = pydantic.Field(
        None, description='Source Log Type'
    )
    log_identifier: Optional[str] = pydantic.Field(
        None, description='Unique log identifier, only available on depth based streams'
    )


class StreamEvent(BaseEvent):
    app_key: str
    records: pydantic.conlist(Record, min_items=1)
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
        """there can only be 1 completed record, always located at the end of the list"""

        return self.records[-1].collection == 'wits.completed'

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def require_app_key_in_metadata_apps(cls, values):
        if values['app_key'] not in values['metadata'].apps:
            raise ValueError('metadata.apps dict must contain an app key.')

        return values

    @staticmethod
    def from_raw_event(event: List[dict], **kwargs) -> List[StreamEvent]:
        app_key = kwargs['app_key']

        result = []

        for event_dict in event:
            if 'app_key' in event_dict:
                raise ValueError(
                    "app_key can't be set manually, it is extracted from env and set automatically."
                )

            event_dict = copy.deepcopy(event_dict)  # copy the dict before modifying
            event_dict['app_key'] = app_key  # add app_key to each event
            result.append(pydantic.parse_obj_as(StreamEvent, event_dict))

        return result

    @classmethod
    def filter_records(
        cls,
        event: StreamEvent,
        filter_mode: Optional[FilterMode],
        last_value: Optional[Union[float, int]],
    ) -> List[Record]:
        new_records = copy.deepcopy(event.records)

        if event.is_completed:
            # there can only be 1 completed record, always located at the end of the list
            new_records = new_records[:-1]  # remove "completed" record

        if filter_mode is not None and last_value is not None:
            if filter_mode == FilterMode.timestamp:
                record_attr = 'timestamp'

            if filter_mode == FilterMode.depth:
                record_attr = 'measured_depth'

            new_records = [
                record
                for record in new_records
                if getattr(record, record_attr) > last_value
            ]

        return new_records


class StreamStateData(CorvaBaseModel):
    last_processed_timestamp: Optional[int] = None
    last_processed_depth: Optional[float] = None


class StreamContext(BaseContext[StreamEvent]):
    filter_mode: Optional[FilterMode] = None

    @property
    def cache_data(self) -> StreamStateData:
        state_data_dict = self.cache.load_all()
        return StreamStateData(**state_data_dict)

    @property
    def last_processed_value(self) -> Optional[Union[int, float]]:
        if self.filter_mode == FilterMode.timestamp:
            return self.cache_data.last_processed_timestamp

        if self.filter_mode == FilterMode.depth:
            return self.cache_data.last_processed_depth

        return None

    def store_cache_data(self, cache_data: StreamStateData) -> int:
        if cache_data := cache_data.dict(exclude_defaults=True, exclude_none=True):
            return self.cache.store(mapping=cache_data)

        return 0
