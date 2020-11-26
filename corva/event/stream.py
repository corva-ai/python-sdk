from __future__ import annotations

from typing import List, Optional

from corva.event.base import BaseEvent
from corva.event.data.stream import StreamEventData


class StreamEvent(BaseEvent):
    @classmethod
    def load(cls, event: str, **kwargs) -> StreamEvent:
        if 'app_key' not in kwargs:
            raise ValueError('Missing app_key in kwargs.')
        app_key: str = kwargs['app_key']
        last_processed_timestamp: Optional[int] = kwargs.get('last_processed_timestamp')
        last_processed_depth: Optional[float] = kwargs.get('last_processed_depth')

        event: List[dict] = super()._load(event=event)

        data = []
        for subdata in event:
            records = subdata['records']

            asset_id = cls.GetAssetId.from_dict(data=subdata)
            app_connection_id = cls._get_app_connection_id(subdata=subdata, app_key=app_key)
            app_stream_id = subdata['metadata']['app_stream_id']
            is_completed = cls._get_is_completed(records=records)

            subdata['records'] = cls._filter_records(
                records=records,
                last_processed_timestamp=last_processed_timestamp,
                last_processed_depth=last_processed_depth
            )

            data.append(StreamEventData(
                **subdata,
                asset_id=asset_id,
                app_connection_id=app_connection_id,
                app_stream_id=app_stream_id,
                is_completed=is_completed
            ))

        return StreamEvent(data=data)

    @staticmethod
    def _get_is_completed(records: List[dict]):
        try:
            return records[-1].get('collection') == 'wits.completed'
        except IndexError as exc:
            raise ValueError(f'Records are empty: {records}') from exc

    @classmethod
    def _filter_records(
         cls,
         records: List[dict],
         last_processed_timestamp: Optional[int] = None,
         last_processed_depth: Optional[float] = None
    ):
        if cls._get_is_completed(records=records):
            records = records[:-1]  # remove "completed" record

        result = []
        for record in records:
            if last_processed_timestamp is not None and record['timestamp'] <= last_processed_timestamp:
                continue
            if last_processed_depth is not None and record['measured_depth'] <= last_processed_depth:
                continue
            result.append(record)
        return result

    @staticmethod
    def _get_app_connection_id(subdata: dict, app_key: str):
        try:
            return subdata['metadata']['apps'][app_key]['app_connection_id']
        except KeyError as exc:
            raise ValueError(f'Can\'t get {app_key} from metadata.apps.') from exc

    class GetAssetId:
        @classmethod
        def from_event(cls, event: str):
            event: List[dict] = BaseEvent._load(event=event)

            try:
                data = event[0]
            except IndexError as exc:
                raise ValueError(f'Could not find asset id in event: {event}.') from exc

            return cls.from_dict(data=data)

        @staticmethod
        def from_dict(data: dict):
            try:
                return data['records'][0]['asset_id']
            except (IndexError, KeyError) as exc:
                raise ValueError(f'Could not find an asset id in data: {data}.') from exc
