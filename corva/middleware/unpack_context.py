import inspect
from typing import Callable, Optional, Tuple

from pydantic.utils import lenient_issubclass

from corva.models.base import BaseContext, BaseEvent
from corva.network.api import Api
from corva.state.redis_state import RedisState


def _parse_call(call: Callable) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Helper function, that looks for arguments with special type annotations
    and returns names of those arguments

    Example, when 'Event' and 'Api' are special type annotations:

        def foo(event: 'Event', api: 'Api'): pass

        _parse_call(foo)  # returns ('event', 'api')
    """

    event_param_name = None
    api_param_name = None
    state_param_name = None
    context_param_name = None

    # iterate over each parameter in signature
    for param in inspect.signature(call).parameters.values():
        name = param.name
        annotation = param.annotation

        # look for annotations with selected types and store argument names
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
    """
    Looks for arguments with special type annotations in function signature,
    then injects values from context into those arguments and calls the function.

    Example, when 'Event' and 'Api' are special type annotations:

        def foo(event: 'Event', api: 'Api'): pass

        @dataclass
        class Context:
            event: Event = None
            api: Api = = None

        context = Context()

        # will `unpack` the context and call foo like this: foo(event=context.event, api=context.api)
        unpack_context(context, foo)
    """

    event_param_name, api_param_name, state_param_name, context_param_name = _parse_call(call=call_next)

    kwargs = context.user_kwargs.copy()

    # populate kwargs with found argument names and values from context
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
