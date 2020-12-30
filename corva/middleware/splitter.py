from itertools import groupby
from typing import Callable, List, Union

from corva.models.scheduled import ScheduledEvent, ScheduledContext
from corva.models.stream import StreamEvent, StreamContext


def _split_event(
     event: Union[StreamEvent, ScheduledEvent],
     split_by_field: str
) -> List[Union[StreamEvent, ScheduledEvent]]:
    events = [
        type(event)(list(group))
        for key, group in groupby(event, key=lambda data: getattr(data, split_by_field))
    ]
    return events


def splitter_factory(split_by_field: str) -> Callable:
    def splitter(
         context: Union[ScheduledContext, StreamContext], call_next: Callable
    ) -> List[Union[ScheduledContext, StreamContext]]:
        events = _split_event(event=context.event, split_by_field=split_by_field)

        contexts = [
            call_next(
                context.copy(update={'event': event}, deep=True)
            )
            for event in events
        ]

        return contexts

    return splitter