from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, TypeVar

from pydantic import BaseModel, Extra

from corva.network.api import Api
from corva.state.redis_state import RedisState


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str, **kwargs) -> BaseEvent:
        pass


class BaseContext(BaseModel):
    """Stores common data for running a Corva app."""

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    raw_event: str
    app_key: str

    event: Optional[BaseEvent] = None
    api: Optional[Api] = None
    state: Optional[RedisState] = None
    user_result: Any = None


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


BaseEventDataTV = TypeVar('BaseEventDataTV', bound=BaseEventData)


class ListEvent(BaseEvent, List[BaseEventDataTV]):
    """Base class for list events (events that consist of more than one event data)."""

    pass
