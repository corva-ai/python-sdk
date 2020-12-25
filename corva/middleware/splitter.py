from itertools import groupby
from typing import Any, Callable, List, Union

from corva.models.base import BaseContext
from corva.models.scheduled import ScheduledContext, ScheduledEvent
from corva.models.stream import StreamContext, StreamEvent


class SplitterMiddleware:
    def __init__(self, call: Callable[[BaseContext], Any], split_by_field: str):
        self.call = call
        self.split_by_field = split_by_field

    def __call__(self, context: Union[StreamContext, ScheduledContext]) -> Any:
        events = self.split_event(event=context.event, split_by_field=self.split_by_field)

        results = [
            self.call(
                context.copy(update={'event': event}, deep=True)
            )
            for event in events
        ]

        return results

    @staticmethod
    def split_event(
         event: Union[StreamEvent, ScheduledEvent],
         split_by_field: str
    ) -> List[Union[StreamEvent, ScheduledEvent]]:
        events = [
            type(event)(list(group))
            for key, group in groupby(event, key=lambda data: getattr(data, split_by_field))
        ]
        return events
