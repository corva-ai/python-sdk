from logging import Logger
from typing import Callable, Optional, Union

from corva.models.scheduled import ScheduledContext
from corva.models.stream import StreamContext
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


def init_state_factory(
     *,
     default_name: str,
     cache_url: str,
     cache_kwargs: Optional[dict] = None,
     logger: Optional[Logger] = None
) -> Callable:
    def init_state(
         context: Union[StreamContext, ScheduledContext], call_next: Callable
    ) -> Union[StreamContext, ScheduledContext]:
        adapter_params = dict(
            default_name=default_name,
            cache_url=cache_url,
            **cache_kwargs
        )
        state_params = {}

        if logger is not None:
            adapter_params['logger'] = logger
            state_params['logger'] = logger

        context.state = RedisState(redis=RedisAdapter(**adapter_params), **state_params)

        context = call_next(context)

        return context

    return init_state
