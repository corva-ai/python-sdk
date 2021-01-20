from abc import ABC, abstractmethod
from itertools import groupby
from logging import Logger, LoggerAdapter
from typing import List, Optional, Union

from corva.event import Event
from corva.logger import DEFAULT_LOGGER
from corva.models.base import BaseContext
from corva.network.api import Api
from corva.configuration import SETTINGS


class BaseApp(ABC):
    def __init__(
         self,
         app_key: str = SETTINGS.APP_KEY,
         cache_url: str = SETTINGS.CACHE_URL,
         api: Optional[Api] = None,
         logger: Union[Logger, LoggerAdapter] = DEFAULT_LOGGER
    ):
        self.app_key = app_key
        self.cache_url = cache_url
        self.api = api
        self.logger = logger

    @property
    @abstractmethod
    def event_loader(self):
        pass

    @property
    @abstractmethod
    def group_by_field(self) -> str:
        pass

    @abstractmethod
    def get_context(self, event: Event) -> BaseContext:
        pass

    def run(self, event: str) -> None:
        try:
            event = self.event_loader.load(event=event)
            events = self._group_event(event=event)
        except Exception:
            self.logger.error('Could not prepare events for run.')
            raise

        for event in events:
            self._run(event=event)

    def _run(self, event: Event) -> None:
        try:
            context = self.get_context(event=event)
        except Exception:
            self.logger.error('Could not get context.')
            raise

        try:
            self.pre_process(context=context)
            self.process(context=context)
            self.post_process(context=context)
        except Exception as exc:
            self.logger.error('An error occurred in process pipeline.')
            self.on_fail(context=context, exception=exc)
            raise

    def pre_process(self, context: BaseContext) -> None:
        pass

    def process(self, context: BaseContext) -> None:
        pass

    def post_process(self, context: BaseContext) -> None:
        pass

    def on_fail(self, context: BaseContext, exception: Exception) -> None:
        pass

    def fetch_asset_settings(self, asset_id: int):
        pass

    def fetch_app_settings(self, asset_id: int, app_connection_id: int):
        pass

    def _group_event(self, event: Event) -> List[Event]:
        events = [
            Event(list(group))
            for key, group in groupby(event, key=lambda data: getattr(data, self.group_by_field))
        ]
        return events
