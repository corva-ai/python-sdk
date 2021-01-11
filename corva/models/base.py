from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
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
    keep_untouched = (cached_property,)


class BaseEvent(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_event(event: str) -> Union[List[BaseEvent], BaseEvent]:
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

    _event: BaseEventTV
    settings: Settings
    state_data_cls: Optional[Type[BaseDataTV]] = None

    user_result: Any = None

    # api params
    api_timeout: Optional[int] = None
    api_max_retries: Optional[int] = None

    # cache params
    cache_kwargs: Optional[dict] = None

    @property
    def cache_key(self) -> str:
        return (
            f'{self.settings.PROVIDER}/well/{self.event.asset_id}/stream/{self.event.app_stream_id}/'
            f'{self.settings.APP_KEY}/{self.event.app_connection_id}'
        )

    @cached_property
    def event(self) -> BaseEventTV:
        return self._event

    @cached_property
    def api(self) -> Api:
        kwargs = {
            'api_url': self.settings.API_ROOT_URL,
            'data_api_url': self.settings.DATA_API_ROOT_URL,
            'api_key': self.settings.API_KEY,
            'app_name': self.settings.APP_NAME
        }

        if self.api_timeout is not None:
            kwargs['timeout'] = self.api_timeout
        if self.api_max_retries is not None:
            kwargs['max_retries'] = self.api_max_retries

        return Api(**kwargs)

    @cached_property
    def state(self) -> RedisState:
        adapter_params = {
            'default_name': self.cache_key,
            'cache_url': self.settings.CACHE_URL,
            **(self.cache_kwargs or {})
        }

        return RedisState(redis=RedisAdapter(**adapter_params))

    @cached_property
    def state_data(self) -> BaseDataTV:
        state_data_dict = self.state.load_all()
        return self.state_data_cls(**state_data_dict)

    def store_state_data(self) -> int:
        store_data = self.state_data.dict(exclude_defaults=True, exclude_none=True)
        if store_data:
            return self.state.store(mapping=store_data)

        return 0
