from typing import Any, Callable

from corva.api import Api
from corva.logger import setup_logging
from corva.models.scheduled import ScheduledContext, ScheduledEvent


def set_schedule_as_completed(api: Api, schedule_id: int) -> None:
    """Sets schedule as completed. Should never raise."""

    api.post(path=f'scheduler/{schedule_id}/completed')


def scheduled_runner(fn: Callable, context: ScheduledContext) -> Any:
    event = ScheduledEvent.parse_obj(context.event)

    with setup_logging(
        aws_request_id=context.aws_request_id,
        asset_id=context.event.asset_id,
        app_connection_id=context.event.app_connection_id,
    ):
        result = fn(event, context.api, context.cache)

    # should never raise
    set_schedule_as_completed(api=context.api, schedule_id=context.event.schedule_id)

    return result
