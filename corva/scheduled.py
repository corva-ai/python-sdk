from typing import Any, Callable

from corva.models.scheduled import ScheduledContext


def scheduled_runner(fn: Callable, context: ScheduledContext) -> Any:
    result = fn(context.event, context.api, context.cache)

    context.api.post(path=f'scheduler/{context.event.schedule}/completed')

    return result
