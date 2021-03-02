from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar, Union

import pydantic.generics

from corva.api import Api
from corva.configuration import Settings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class CorvaModelConfig:
    arbitrary_types_allowed = True
    extra = pydantic.Extra.allow
    validate_assignment = True


class CorvaBaseModel(pydantic.BaseModel):
    Config = CorvaModelConfig


class BaseEvent(CorvaBaseModel, ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: Any) -> Union[List[BaseEvent], BaseEvent]:
        pass


BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)


class BaseContext(pydantic.generics.GenericModel, Generic[BaseEventTV]):
    """Stores common data for running a Corva app."""

    Config = CorvaModelConfig

    event: BaseEventTV
    settings: Settings
    api: Api
    _cache: Optional[RedisState] = None

    user_result: Any = None

    cache_settings: dict = {}

    @property
    def cache_key(self) -> str:
        return (
            f'{self.settings.PROVIDER}/well/{self.event.asset_id}/stream/{self.event.app_stream_id}/'
            f'{self.settings.APP_KEY}/{self.event.app_connection_id}'
        )

    @property
    def cache(self) -> RedisState:
        if self._cache:
            return self._cache

        redis_adapter = RedisAdapter(
            default_name=self.cache_key,
            cache_url=self.settings.CACHE_URL,
            **self.cache_settings,
        )

        self._cache = RedisState(redis=redis_adapter)

        return self._cache
