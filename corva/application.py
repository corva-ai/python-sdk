from typing import Callable, List, Optional

from corva.middleware.user_callable import UserCallableMiddleware
from corva.models.base import BaseContext
from corva.types import MIDDLEWARE_CALL_TYPE, MIDDLEWARE_TYPE


def wrap_call_in_middleware(
     call: Callable,
     middleware: Optional[List[MIDDLEWARE_TYPE]] = None
) -> MIDDLEWARE_CALL_TYPE:
    def wrapper_factory(mw, call):
        def wrapper(ctx):
            return mw(ctx, call)

        return wrapper

    middleware = middleware or []

    for mw in reversed(middleware):
        call = wrapper_factory(mw, call)

    return call


class Corva:
    def __init__(
         self,
         *,
         middleware: Optional[List[MIDDLEWARE_TYPE[BaseContext]]] = None
    ):
        self.user_middleware = middleware or []

    def get_middleware_stack(
         self,
         middleware: Optional[List[MIDDLEWARE_TYPE[BaseContext]]] = None
    ) -> List[MIDDLEWARE_TYPE[BaseContext]]:
        middleware = middleware or []

        middleware_stack = (
             middleware
             + self.user_middleware
             + [UserCallableMiddleware]
        )

        return middleware_stack

    def add_middleware(self, func: MIDDLEWARE_TYPE[BaseContext]) -> None:
        self.user_middleware.append(func)

    def middleware(self, func: MIDDLEWARE_TYPE[BaseContext]) -> None:
        return self.add_middleware(func=func)
