from __future__ import annotations

from typing import List

from corva.event import Event
from corva.models.stream import StreamEventData
from corva.loader.base import BaseLoader


class StreamLoader(BaseLoader):
    def __init__(self, app_key: str):
        self.app_key = app_key

    def load(self, event: str) -> Event:
        event: List[dict] = super()._load_json(event=event)

        data = []
        for subdata in event:
            asset_id = self.get_asset_id(data=subdata)
            app_connection_id = self._get_app_connection_id(subdata=subdata, app_key=self.app_key)
            app_stream_id = subdata['metadata']['app_stream_id']
            is_completed = self._get_is_completed(records=subdata['records'])

            data.append(StreamEventData(
                asset_id=asset_id,
                app_connection_id=app_connection_id,
                app_stream_id=app_stream_id,
                is_completed=is_completed,
                **subdata
            ))

        return Event(data)

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
