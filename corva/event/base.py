from __future__ import annotations

import json
from abc import abstractmethod, ABC
from collections import UserList
from typing import List

from corva.constants import EVENT_TYPE
from corva.event.data.base import BaseEventData
from corva.settings import APP_KEY
from corva.utils import get_provider


class BaseEvent(ABC, UserList):
    def __init__(self, data: List[BaseEventData]):
        super().__init__(data)

    @classmethod
    @abstractmethod
    def load(cls, event: str, **kwargs) -> BaseEvent:
        pass

    @classmethod
    @abstractmethod
    def get_asset_id(cls, event: EVENT_TYPE) -> int:
        pass

    @classmethod
    @abstractmethod
    def get_app_stream_id(cls, event: EVENT_TYPE) -> int:
        pass

    @classmethod
    @abstractmethod
    def get_app_connection_id(cls, event: EVENT_TYPE) -> int:
        pass

    @classmethod
    def _load(cls, event: str) -> EVENT_TYPE:
        event = cls._load_json(event=event)
        return event

    @staticmethod
    def _load_json(event: str) -> EVENT_TYPE:
        if not event:
            raise ValueError('Empty event')

        if not isinstance(event, str):
            raise TypeError(f'Unknown event type {type(event)}')

        try:
            event = json.loads(event)
        except ValueError as exc:
            raise ValueError('Invalid event JSON') from exc

        return event

    @classmethod
    def get_state_key(cls, event: str, app_key=APP_KEY):
        event = cls._load(event=event)
        provider = get_provider(app_key=app_key)
        asset_id = cls.get_asset_id(event=event)
        app_stream_id = cls.get_app_stream_id(event=event)
        app_connection_id = cls.get_app_connection_id(event=event)
        state_key = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
        return state_key

    def __eq__(self, other):
        return (
             super().__eq__(other)
             and
             all(
                 type(self_value) == type(other_value)
                 for self_value, other_value in zip(self, other)
             )
        )
