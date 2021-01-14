from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel

from corva.network.api import Api
from corva.settings import Settings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class BaseConfig:
    allow_population_by_field_name = True
    arbitrary_types_allowed = True
    extra = Extra.allow
    validate_assignment = True


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str, **kwargs) -> Union[List[BaseEvent], BaseEvent]:
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

    event: BaseEventTV
    settings: Settings
    api: Api
    _cache: Optional[RedisState] = None

    user_result: Any = None

    # cache params
    cache_kwargs: Optional[dict] = None
    cache_data_cls: Optional[Type[BaseDataTV]] = None

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

        adapter_params = {
            'default_name': self.cache_key,
            'cache_url': self.settings.CACHE_URL,
            **(self.cache_kwargs or {})
        }

        self._cache = RedisState(redis=RedisAdapter(**adapter_params))

        return self._cache

    @property
    def cache_data(self) -> BaseDataTV:
        state_data_dict = self.cache.load_all()
        return self.cache_data_cls(**state_data_dict)

    def store_cache_data(self, cache_data: BaseDataTV) -> int:
        cache_data = cache_data.dict(exclude_defaults=True, exclude_none=True)
        if cache_data:
            return self.cache.store(mapping=cache_data)

        return 0
