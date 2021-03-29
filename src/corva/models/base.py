from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar, Union

import pydantic.generics

from corva.api import Api
from corva.configuration import Settings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class EventConfig:
    extra = pydantic.Extra.ignore
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
    cache_: Optional[RedisState] = None
    cache_settings: dict = {}

    @property
    def cache_key(self) -> str:
        return (
            f'{self.settings.PROVIDER}/well/{self.event.asset_id}/stream/{self.event.app_stream_id}/'
            f'{self.settings.APP_KEY}/{self.event.app_connection_id}'
        )

    @property
    def cache(self) -> RedisState:
        if self.cache_:
            return self.cache_

        redis_adapter = RedisAdapter(
            default_name=self.cache_key,
            cache_url=self.settings.CACHE_URL,
            **self.cache_settings,
        )

        self.cache_ = RedisState(redis=redis_adapter)

        return self.cache_
