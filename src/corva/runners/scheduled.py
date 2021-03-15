from typing import Any, Callable

from corva.api import Api
from corva.models.scheduled import ScheduledContext


def set_schedule_as_completed(api: Api, schedule_id: int) -> None:
    """Sets schedule as completed. Should never raise."""

    api.post(path=f'scheduler/{schedule_id}/completed')


def scheduled_runner(fn: Callable, context: ScheduledContext) -> Any:
    result = fn(context.event, context.api, context.cache)

    # should never raise
    set_schedule_as_completed(api=context.api, schedule_id=context.event.schedule_id)

    return result
