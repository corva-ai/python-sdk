from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar

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

    _event: Optional[BaseEventTV] = None
    _api: Optional[Api] = None
    _state: Optional[RedisState] = None
    _state_data: Optional[BaseDataTV] = None
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

    @property
    def event(self) -> BaseEventTV:
        if self._event is None:
            self._event = BaseEventTV.from_raw_event(self.raw_event, app_key=self.app_key)

        return self._event

    @property
    def api(self) -> Api:
        if self._api is None:
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

            self._api = Api(**kwargs)

        return self._api

    @property
    def state(self) -> RedisState:
        if self._state is None:
            adapter_params = dict(
                default_name=self.cache_key,
                cache_url=self.cache_url,
                **(self.cache_kwargs or {})
            )

            self._state = RedisState(redis=RedisAdapter(**adapter_params))

        return self._state

    @property
    def state_data(self) -> BaseDataTV:
        if self._state_data:
            state_data_dict = self.state.load_all()
            self._state_data = BaseDataTV(**state_data_dict)

        return self._state_data

    @state_data.setter
    def state_data(self, value):
        self._state_data = value


class ListEvent(BaseEvent, List[BaseDataTV]):
    """Base class for list events (events that consist of more than one event data)."""

    pass
