from typing import Callable, List, Optional

from corva.middleware.unpack_context import unpack_context


def wrap_call_in_middleware(
     call: Callable,
     middleware: Optional[List[Callable]] = None
) -> Callable:
    def wrapper_factory(mw, call):
        def wrapper(ctx):
            return mw(ctx, call)

        return wrapper

    middleware = middleware or []

    for mw in reversed(middleware):
        call = wrapper_factory(mw, call)

    return call


class Corva:
    def __init__(self, middleware: Optional[List[Callable]] = None):
        self.user_middleware = middleware or []

    def get_middleware_stack(
         self,
         middleware: Optional[List[Callable]] = None
    ) -> List[Callable]:
        middleware = middleware or []
        default_middleware = [unpack_context]  # default middleware, should be called last

        middleware_stack = (
             middleware
             + self.user_middleware
             + default_middleware
        )

        return middleware_stack

    def add_middleware(self, func: Callable) -> None:
        self.user_middleware.append(func)

    def middleware(self, func: Callable) -> None:
        return self.add_middleware(func=func)
