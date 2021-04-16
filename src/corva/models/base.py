from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar, Union

import pydantic.generics

from corva.api import Api
from corva.configuration import Settings
from corva.state.redis_state import RedisState, get_cache


class EventConfig:
    extra = pydantic.Extra.allow
    allow_mutation = False


class CorvaBaseEvent(pydantic.BaseModel):
    class Config(EventConfig):
        pass


class CorvaBaseGenericEvent(pydantic.generics.GenericModel):
    class Config(EventConfig):
        pass


class RawBaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: Any) -> Union[List[RawBaseEvent], RawBaseEvent]:
        pass


RawBaseEventTV = TypeVar('RawBaseEventTV', bound=RawBaseEvent)


class BaseContext(pydantic.generics.GenericModel, Generic[RawBaseEventTV]):
    """Stores common data for running a Corva app."""

    class Config:
        arbitrary_types_allowed = True
        extra = pydantic.Extra.ignore
        validate_assignment = True

    event: RawBaseEventTV
    settings: Settings
    api: Api
    aws_request_id: str
    cache_: Optional[RedisState] = None
    cache_settings: dict = {}

    @property
    def cache(self) -> RedisState:
        if self.cache_:
            return self.cache_

        self.cache_ = get_cache(
            asset_id=self.event.asset_id,
            app_stream_id=self.event.app_stream_id,
            app_connection_id=self.event.app_connection_id,
            settings=self.settings,
            cache_settings=self.cache_settings,
        )

        return self.cache_
