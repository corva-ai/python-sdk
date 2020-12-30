from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel

from corva.network.api import Api
from corva.state.redis_state import RedisState


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str, **kwargs) -> BaseEvent:
        pass


class BaseStateData(BaseModel):
    class Config:
        validate_assignment = True


BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)
BaseStateDataTV = TypeVar('BaseStateDataTV', bound=BaseStateData)


class BaseContext(GenericModel, Generic[BaseEventTV, BaseStateDataTV]):
    """Stores common data for running a Corva app."""

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow
        validate_assignment = True

    raw_event: str
    app_key: str

    event: Optional[BaseEventTV] = None
    api: Optional[Api] = None
    state: Optional[RedisState] = None
    state_data: Optional[BaseStateDataTV] = None
    user_result: Any = None


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


BaseEventDataTV = TypeVar('BaseEventDataTV', bound=BaseEventData)


class ListEvent(BaseEvent, List[BaseEventDataTV]):
    """Base class for list events (events that consist of more than one event data)."""

    pass
