from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, LoggerAdapter
from typing import Optional, Type, Union, Any, Dict

from corva.event.base import BaseEvent
from corva.logger import LOGGER
from corva.network.api import Api
from corva.settings import APP_KEY
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


@dataclass(frozen=True)
class ProcessResult:
    event: BaseEvent
    next_process_kwargs: Dict[str, Any] = field(default_factory=dict)


class BaseApp(ABC):
    def __init__(
         self,
         app_key: str = APP_KEY,
         api: Optional[Api] = None,
         state: Optional[RedisState] = None,
         event: Optional[str] = None,
         logger: Union[Logger, LoggerAdapter] = LOGGER
    ):
        self.app_key = app_key
        self.api = api or Api()
        self.state = state or RedisState(
            redis=RedisAdapter(
                default_name=self.event_cls.get_state_key(event=event, app_key=app_key)
            ),
            logger=logger
        )
        self.logger = logger

    @property
    @abstractmethod
    def event_cls(self) -> Type[BaseEvent]:
        pass

    def run(
         self,
         event: str,
         pre_process_kwargs: Optional[dict] = None,
         process_kwargs: Optional[dict] = None,
         post_process_kwargs: Optional[dict] = None,
         on_fail_before_post_process_kwargs: Optional[dict] = None
    ):
        pre_process_kwargs = pre_process_kwargs or {}
        process_kwargs = process_kwargs or {}
        post_process_kwargs = post_process_kwargs or {}
        on_fail_before_post_process_kwargs = on_fail_before_post_process_kwargs or {}

        try:
            pre_result = self.pre_process(event=event, **pre_process_kwargs)
            event = pre_result.event
            process_result = self.process(
                event=event, **{**pre_result.next_process_kwargs, **process_kwargs}
            )
        except Exception:
            self.logger.error('An error occurred in pre_process or process.')
            self.on_fail_before_post_process(event=event, **on_fail_before_post_process_kwargs)
            raise

        return self.post_process(
            event=process_result.event,
            **{
                **process_result.next_process_kwargs,
                **post_process_kwargs
            }
        )

    def pre_process(self, event: str, load_kwargs: Optional[dict] = None, **kwargs) -> ProcessResult:
        load_kwargs = load_kwargs or {}
        event = self.event_cls.load(event=event, **load_kwargs)
        return ProcessResult(event=event)

    def process(self, event: BaseEvent, **kwargs) -> ProcessResult:
        return ProcessResult(event=event)

    def post_process(self, event: BaseEvent, **kwargs) -> ProcessResult:
        return ProcessResult(event=event)

    def on_fail_before_post_process(self, event: Union[str, BaseEvent], **kwargs) -> None:
        pass

    def fetch_asset_settings(self, asset_id: int):
        pass

    def fetch_app_settings(self, asset_id: int, app_connection_id: int):
        pass
