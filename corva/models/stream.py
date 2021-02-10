from __future__ import annotations

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

    @pydantic.root_validator(pre=True)
    def require_timestamp_or_measured_depth(cls, values):
        # exactly one of timestamp or measured_depth should be provided
        if len({'timestamp', 'measured_depth'} - set(values)) == 1:
            return values

        raise ValueError('Either timestamp or measured_depth is required')


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
    asset_id: int
    app_key: str
    records: List[Record]
    metadata: StreamEventMetadata

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

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def require_app_key_in_metadata_apps(cls, values):
        if values['app_key'] not in values['metadata'].apps:
            raise ValueError('metadata.apps dict must contain an app key.')

        return values

    @pydantic.root_validator(pre=True)
    def set_asset_id(cls, values):
        """Dynamically sets value for asset_id.

        asset_id could've been defined as property like below.

        @property
        def asset_id(self) -> Optional[int]:
            return self.records[0].asset_id if self.records else None

        The issue with the above method is:
          after filtering, we may end up with empty records. Which leads to asset_id becoming None.
          Using this validator we are able to dynamically set and store value of asset_id,
          no matter what happens to records.
        """

        if 'asset_id' in values:
            raise ValueError(
                "asset_id can't be set manually, it is extracted from records automatically."
            )

        records = pydantic.parse_obj_as(
            List[Record], values.get('records', [])
        )  # type: List[Record]

        if not len(records):
            raise ValueError(
                "Can't set asset_id as records are empty (which should not happen)."
            )

        values['asset_id'] = records[0].asset_id

        return values

    @staticmethod
    def from_raw_event(event: Union[str, List], **kwargs) -> List[StreamEvent]:
        app_key = kwargs['app_key']

        parse = pydantic.parse_obj_as
        if isinstance(event, str):
            parse = pydantic.parse_raw_as

        event_dicts = parse(List[dict], event)  # type: List[dict]

        for event_dict in event_dicts:
            if 'app_key' in event_dict:
                raise ValueError(
                    "app_key can't be set manually, it is extracted from env and set automatically."
                )

            event_dict['app_key'] = app_key  # add app_key to each event

        events = pydantic.parse_obj_as(List[StreamEvent], event_dicts)

        return events

    @classmethod
    def filter(
        cls,
        event: StreamEvent,
        filter_mode: Optional[FilterMode],
        last_value: Optional[Union[float, int]],
    ) -> StreamEvent:
        records = event.records

        if event.is_completed:
            # there can only be 1 completed record, always located at the end of the list
            records = records[:-1]  # remove "completed" record

        new_records = records

        if filter_mode is not None and last_value is not None:
            new_records = list(
                cls._filter_records(
                    records=records, filter_mode=filter_mode, last_value=last_value
                )
            )

        return event.copy(update={'records': new_records}, deep=True)

    @staticmethod
    def _filter_records(
        records: List[Record], filter_mode: FilterMode, last_value: Union[float, int]
    ):
        for record in records:
            if filter_mode == FilterMode.timestamp and record.timestamp <= last_value:
                continue

            if filter_mode == FilterMode.depth and record.measured_depth <= last_value:
                continue

            yield record


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
