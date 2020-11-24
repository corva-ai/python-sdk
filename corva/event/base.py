from __future__ import annotations

import json
from abc import abstractmethod, ABC
from collections import UserList
from typing import List

from corva.constants import EVENT_TYPE
from corva.event.data.base import BaseEventData


class BaseEvent(ABC, UserList):
    def __init__(self, data: List[BaseEventData]):
        super().__init__(data)

    @classmethod
    @abstractmethod
    def load(cls, event: str, **kwargs) -> BaseEvent:
        ...

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

    def __eq__(self, other):
        return (
             super().__eq__(other)
             and
             all(
                 type(self_value) == type(other_value)
                 for self_value, other_value in zip(self, other)
             )
        )
