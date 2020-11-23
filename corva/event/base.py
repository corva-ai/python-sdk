from __future__ import annotations

import json
from abc import abstractmethod, ABC
from collections import UserList
from typing import List

from corva.constants import RAW_EVENT_TYPE, EVENT_TYPE
from corva.event.data.base import BaseEventData


class BaseEvent(ABC, UserList):
    def __init__(self, data: List[BaseEventData]):
        super().__init__(data)

    @classmethod
    @abstractmethod
    def load(cls, event: RAW_EVENT_TYPE, **kwargs) -> BaseEvent:
        ...

    @classmethod
    def _load(cls, event: RAW_EVENT_TYPE) -> EVENT_TYPE:
        event = cls._load_json(event=event)
        return event

    @staticmethod
    def _load_json(event: RAW_EVENT_TYPE) -> EVENT_TYPE:
        if not event:
            raise ValueError('Empty event')

        if isinstance(event, (str, bytes, bytearray)):
            try:
                event = json.loads(event)
            except json.JSONDecodeError as error:
                raise ValueError('Invalid event JSON') from error
        else:
            raise TypeError(f'Unknown event type {type(event)}')

        return event

    def __eq__(self, other):
        return (
                super().__eq__(other)
                and
                all(type(s) == type(o) for s, o in zip(self, other))
        )
