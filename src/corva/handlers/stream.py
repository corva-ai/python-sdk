import functools
import contextlib
from corva.api import Api
from corva.handlers.base import base_handler
from corva.models.stream.raw import RawStreamEvent
from corva.state.redis_state import RedisState


def stream(func):
    @functools.wraps(func)
    @base_handler
    def wrapper(event: RawStreamEvent, api: Api, cache: RedisState):
        records = event.filter_records(
            old_max_record_value=event.get_cached_max_record_value(cache=cache)
        )

        if not records:
            # we've got the duplicate data if there are no records left after filtering
            return

        result = func(
            event.metadata.log_type.event.parse_obj(
                event.copy(update={'records': records}, deep=True)
            ),
            api,
            cache,
        )

        with contextlib.suppress(Exception):
            # lambda should not fail if we were not able to cache the value
            event.set_cached_max_record_value(cache=cache)

        return result

    return wrapper
