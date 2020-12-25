from typing import Generic, TypeVar

from pydantic import parse_raw_as

from corva.models.base import BaseEvent
from corva.types import SCHEDULED_EVENT_TYPE, STREAM_EVENT_TYPE, TASK_EVENT_TYPE

BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)
EventTypeTV = TypeVar('EventTypeTV', SCHEDULED_EVENT_TYPE, STREAM_EVENT_TYPE, TASK_EVENT_TYPE)


class BaseLoader(Generic[BaseEventTV, EventTypeTV]):
    def load(self, event: str) -> BaseEventTV:
        return parse_raw_as(EventTypeTV, event)
