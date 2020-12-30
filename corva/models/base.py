from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel

from corva.network.api import Api
from corva.state.redis_state import RedisState


class BaseConfig:
    allow_population_by_field_name = True
    arbitrary_types_allowed = True
    extra = Extra.allow
    validate_assignment = True


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str, **kwargs) -> BaseEvent:
        pass


class BaseData(BaseModel):
    class Config(BaseConfig):
        pass


BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)
BaseDataTV = TypeVar('BaseDataTV', bound=BaseData)


class BaseContext(GenericModel, Generic[BaseEventTV, BaseDataTV]):
    """Stores common data for running a Corva app."""

    class Config(BaseConfig):
        pass

    raw_event: str
    app_key: str

    event: Optional[BaseEventTV] = None
    api: Optional[Api] = None
    state: Optional[RedisState] = None
    state_data: Optional[BaseDataTV] = None
    user_result: Any = None


class ListEvent(BaseEvent, List[BaseDataTV]):
    """Base class for list events (events that consist of more than one event data)."""

    pass
