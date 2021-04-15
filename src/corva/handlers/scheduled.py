import contextlib
import functools

from corva.api import Api
from corva.handlers.base import base_handler
from corva.models.scheduled import RawScheduledEvent
from corva.state.redis_state import RedisState


def scheduled(func):
    @functools.wraps(func)
    @base_handler
    def wrapper(event: RawScheduledEvent, api: Api, cache: RedisState):
        result = fn(ScheduledEvent.parse_obj(event), api, cache)

        with contextlib.suppress(Exception):
            # lambda should not fail if we were not able to set completed status
            event.set_schedule_as_completed(api=api)

        return result

    return wrapper
