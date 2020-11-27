from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import groupby
from logging import Logger, LoggerAdapter
from typing import Any, Dict, List, Optional, Type, Union

from corva.event.base import BaseEvent
from corva.logger import LOGGER
from corva.network.api import Api
from corva.settings import APP_KEY
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState
from corva.utils import get_state_key


@dataclass(frozen=True)
class ProcessResult:
    event: BaseEvent
    next_process_kwargs: Dict[str, Any] = field(default_factory=dict)


class BaseApp(ABC):
    def __init__(
         self,
         app_key: str = APP_KEY,
         api: Optional[Api] = None,
         logger: Union[Logger, LoggerAdapter] = LOGGER
    ):
        self.app_key = app_key
        self.api = api or Api()
        self.logger = logger

    @property
    @abstractmethod
    def event_cls(self) -> Type[BaseEvent]:
        pass

    def run(
         self,
         event: str,
         load_kwargs: Optional[dict] = None,
         pre_process_kwargs: Optional[dict] = None,
         process_kwargs: Optional[dict] = None,
         post_process_kwargs: Optional[dict] = None,
         on_fail_before_post_process_kwargs: Optional[dict] = None
    ):
        load_kwargs = load_kwargs or {}
        try:
            event: BaseEvent = self.event_cls.load(event=event, **load_kwargs)
            events: List[BaseEvent] = self._group_event(event=event)
            states = self._get_states(events=events)
        except Exception:
            self.logger.error('Could not prepare events and states for run.')
            raise

        for event, state in zip(events, states):
            self._run(
                event=event,
                state=state,
                pre_process_kwargs=pre_process_kwargs,
                process_kwargs=process_kwargs,
                post_process_kwargs=post_process_kwargs,
                on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
            )

    def _run(
         self,
         event: BaseEvent,
         state: RedisState,
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
            pre_result = self.pre_process(event=event, state=state, **pre_process_kwargs)
            event = pre_result.event
            process_result = self.process(
                event=event, state=state, **{**pre_result.next_process_kwargs, **process_kwargs}
            )
        except Exception:
            self.logger.error('An error occurred in pre_process or process.')
            self.on_fail_before_post_process(event=event, state=state, **on_fail_before_post_process_kwargs)
            raise

        try:
            self.post_process(
                event=process_result.event,
                state=state,
                **{
                    **process_result.next_process_kwargs,
                    **post_process_kwargs
                }
            )
        except Exception:
            self.logger.error('An error occurred in post_process.')
            raise

    def pre_process(self, event: BaseEvent, state: RedisState, **kwargs) -> ProcessResult:
        return ProcessResult(event=event)

    def process(self, event: BaseEvent, state: RedisState, **kwargs) -> ProcessResult:
        return ProcessResult(event=event)

    def post_process(self, event: BaseEvent, state: RedisState, **kwargs) -> ProcessResult:
        return ProcessResult(event=event)

    def on_fail_before_post_process(self, event: BaseEvent, state: RedisState, **kwargs) -> None:
        pass

    def fetch_asset_settings(self, asset_id: int):
        pass

    def fetch_app_settings(self, asset_id: int, app_connection_id: int):
        pass

    def _group_event(self, event: BaseEvent) -> List[BaseEvent]:
        events = [
            self.event_cls(data=list(group))
            for key, group in groupby(event, key=lambda data: data.app_connection_id)
        ]
        return events

    def _get_states(self, events: List[BaseEvent]) -> List[RedisState]:
        states = []
        for event in events:
            states.append(RedisState(
                redis=RedisAdapter(
                    default_name=get_state_key(
                        asset_id=event[0].asset_id,
                        app_stream_id=event[0].app_stream_id,
                        app_key=self.app_key,
                        app_connection_id=event[0].app_connection_id
                    ),
                    logger=self.logger
                )
            ))
        return states
