from typing import Callable

from corva.models.scheduled import ScheduledContext


def scheduled(context: ScheduledContext, call_next: Callable) -> ScheduledContext:
    context = call_next(context)  # type: ScheduledContext

    context.api.post(path=f'scheduler/{context.event.schedule}/completed')

    return context
