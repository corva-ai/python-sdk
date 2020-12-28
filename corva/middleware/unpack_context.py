import inspect
from typing import Callable, Optional, Tuple

from pydantic.utils import lenient_issubclass

from corva.models.base import BaseContext, BaseEvent
from corva.network.api import Api
from corva.state.redis_state import RedisState


def _parse_call(call: Callable) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    event_param_name = None
    api_param_name = None
    state_param_name = None
    context_param_name = None

    for param in inspect.signature(call).parameters.values():
        name = param.name
        annotation = param.annotation

        if lenient_issubclass(annotation, BaseEvent):
            event_param_name = name
        elif lenient_issubclass(annotation, Api):
            api_param_name = name
        elif lenient_issubclass(annotation, RedisState):
            state_param_name = name
        elif lenient_issubclass(annotation, BaseContext):
            context_param_name = name

    return event_param_name, api_param_name, state_param_name, context_param_name


def unpack_context(context: BaseContext, call_next: Callable) -> BaseContext:
    event_param_name, api_param_name, state_param_name, context_param_name = _parse_call(call=call_next)

    kwargs = context.user_kwargs.copy()

    if event_param_name:
        kwargs[event_param_name] = context.event
    if state_param_name:
        kwargs[state_param_name] = context.state
    if api_param_name:
        kwargs[api_param_name] = context.api
    if context_param_name:
        kwargs[context_param_name] = context

    context.user_result = call_next(**kwargs)

    return context
