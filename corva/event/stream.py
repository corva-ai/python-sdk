from __future__ import annotations

from typing import List

from corva.event.base import BaseEvent
from corva.event.data.stream import StreamEventData


class StreamEvent(BaseEvent):
    @classmethod
    def load(cls, event: str, app_key: str, **kwargs) -> StreamEvent:
        event: List[dict] = super()._load(event=event)

        data = []
        for subdata in event:
            asset_id = cls.get_asset_id(data=subdata)
            app_connection_id = cls._get_app_connection_id(subdata=subdata, app_key=app_key)
            app_stream_id = subdata['metadata']['app_stream_id']
            is_completed = cls._get_is_completed(records=subdata['records'])

            data.append(StreamEventData(
                asset_id=asset_id,
                app_connection_id=app_connection_id,
                app_stream_id=app_stream_id,
                is_completed=is_completed,
                **subdata
            ))

        return StreamEvent(data=data)

    @staticmethod
    def _get_is_completed(records: List[dict]):
        try:
            return records[-1].get('collection') == 'wits.completed'
        except IndexError as exc:
            raise ValueError(f'Records are empty: {records}') from exc

    @staticmethod
    def _get_app_connection_id(subdata: dict, app_key: str):
        try:
            return subdata['metadata']['apps'][app_key]['app_connection_id']
        except KeyError as exc:
            raise ValueError(f'Can\'t get {app_key} from metadata.apps.') from exc

    @staticmethod
    def get_asset_id(data: dict):
        try:
            return data['records'][0]['asset_id']
        except (IndexError, KeyError) as exc:
            raise ValueError(f'Could not find an asset id in data: {data}.') from exc
