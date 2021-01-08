from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel

from corva.network.api import Api
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
    event_cls: Type[BaseEventTV]
    state_data_cls: Optional[Type[BaseDataTV]] = None
    app_key: str

    user_result: Any = None

    # api params
    api_url: str
    api_data_url: str
    api_key: str
    api_app_name: str
    api_timeout: Optional[int] = None
    api_max_retries: Optional[int] = None

    # cache params
    cache_url: Optional[str] = None
    cache_kwargs: Optional[dict] = None

    @property
    def provider(self) -> str:
        return self.app_key.split('.')[0]

    @property
    def cache_key(self) -> str:
        event = self.event

        if isinstance(event, list):
            event = event[0]

        return (
            f'{self.provider}/well/{event.asset_id}/stream/{event.app_stream_id}/'
            f'{self.app_key}/{event.app_connection_id}'
        )

    @cached_property
    def event(self) -> BaseEventTV:
        return self.event_cls.from_raw_event(self.raw_event, app_key=self.app_key)

    @cached_property
    def api(self) -> Api:
        kwargs = dict(
            api_url=self.api_url,
            data_api_url=self.api_data_url,
            api_key=self.api_key,
            api_name=self.api_app_name
        )

        if self.api_timeout is not None:
            kwargs['timeout'] = self.api_timeout
        if self.api_max_retries is not None:
            kwargs['max_retries'] = self.api_max_retries

        return Api(**kwargs)

    @cached_property
    def state(self) -> RedisState:
        adapter_params = {
            'default_name': self.cache_key,
            'cache_url': self.cache_url,
            **(self.cache_kwargs or {})
        }

        return RedisState(redis=RedisAdapter(**adapter_params))

    @cached_property
    def state_data(self) -> BaseDataTV:
        state_data_dict = self.state.load_all()
        return self.state_data_cls(**state_data_dict)

    def store_state_data(self) -> int:
        return self.state.store(mapping=self.state_data.dict(exclude_defaults=True, exclude_none=True))


class ListEvent(BaseEvent, List[BaseDataTV]):
    """Base class for list events (events that consist of more than one event data)."""

    pass
