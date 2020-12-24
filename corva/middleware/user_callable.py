import inspect
from typing import Callable, Any, Tuple, Optional

from pydantic.utils import lenient_issubclass

from corva.models.base import BaseContext
from corva.models.base import BaseEvent
from corva.network.api import Api
from corva.state.redis_state import RedisState


class UserCallableMiddleware:
    def __init__(self, call: Callable):
        self.call = call
        self.event_param_name, self.state_param_name, self.api_param_name = self.parse_callable(call=self.call)

    def __call__(self, context: BaseContext) -> Any:
        kwargs = context.user_kwargs.copy()

        if self.event_param_name:
            kwargs[self.event_param_name] = context.event
        if self.state_param_name:
            kwargs[self.state_param_name] = context.state
        if self.api_param_name:
            kwargs[self.api_param_name] = context.api

        result = self.call(**kwargs)

        context.user_result = result

        return result

    @staticmethod
    def parse_callable(call: Callable) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        event_param_name = None
        api_param_name = None
        state_param_name = None

        for param in inspect.signature(call).parameters.values():
            name = param.name
            annotation = param.annotation

            if lenient_issubclass(annotation, BaseEvent):
                event_param_name = name
            elif lenient_issubclass(annotation, Api):
                api_param_name = name
            elif lenient_issubclass(annotation, RedisState):
                state_param_name = name

        return event_param_name, api_param_name, state_param_name
