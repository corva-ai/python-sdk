from typing import Any, Callable

from corva.models.scheduled import ScheduledContext


def scheduled_runner(fn: Callable, context: ScheduledContext) -> Any:
    result = fn(context.event, context.api, context.cache)

    # should never raise
    context.api._set_schedule_as_completed(schedule_id=context.event.schedule_id)

    return result
