from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, Type, TypeVar, Union

import pydantic
from pydantic.generics import GenericModel

from corva.network.api import Api
from corva.settings import CorvaSettings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str, **kwargs) -> Union[List[BaseEvent], BaseEvent]:
        pass


class CorvaModelConfig:
    allow_population_by_field_name = True
    arbitrary_types_allowed = True
    extra = pydantic.Extra.allow
    validate_assignment = True


class CorvaBaseModel(pydantic.BaseModel):
    Config = CorvaModelConfig


class CorvaGenericModel(GenericModel):
    Config = CorvaModelConfig


BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)
CorvaBaseModelTV = TypeVar('CorvaBaseModelTV', bound=CorvaBaseModel)


class BaseContext(CorvaGenericModel, Generic[BaseEventTV, CorvaBaseModelTV]):
    """Stores common data for running a Corva app."""

    event: BaseEventTV
    settings: CorvaSettings
    api: Api
    _cache: Optional[RedisState] = None

    user_result: Any = None

    # cache params
    cache_kwargs: dict = {}
    cache_data_cls: Optional[Type[CorvaBaseModelTV]] = None

    @property
    def cache_key(self) -> str:
        return (
            f'{self.settings.PROVIDER}/well/{self.event.asset_id}/stream/{self.event.app_stream_id}/'
            f'{self.settings.APP_KEY}/{self.event.app_connection_id}'
        )

    @property
    def cache(self) -> RedisState:
        if self._cache is not None:
            return self._cache

        redis_adapter = RedisAdapter(
            name=self.cache_key,
            cache_url=self.settings.CACHE_URL,
            **self.cache_kwargs
        )

        self._cache = RedisState(redis=redis_adapter)

        return self._cache

    @property
    def cache_data(self) -> CorvaBaseModelTV:
        state_data_dict = self.cache.load_all()
        return self.cache_data_cls(**state_data_dict)

    def store_cache_data(self, cache_data: CorvaBaseModelTV) -> int:
        if cache_data := cache_data.dict(exclude_defaults=True, exclude_none=True):
            return self.cache.store(mapping=cache_data)

        return 0
