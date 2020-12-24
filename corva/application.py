from typing import Any, Callable, List, Optional

from corva.middleware.user import UserMiddleware
from corva.middleware.user_callable import UserCallableMiddleware
from corva.middleware.wrapper import Middleware
from corva.models.base import BaseContext
from corva.types import DISPATCH_TYPE


class Corva:
    def __init__(
         self,
         *,
         middleware: Optional[List[Middleware]] = None
    ):
        self.user_middleware = middleware or []

    def build_middlware_stack(
         self,
         *,
         call: Callable,
         middleware: Optional[List[Middleware]] = None
    ) -> Callable[[BaseContext], Any]:
        middleware = (
             [Middleware(UserCallableMiddleware)]
             + self.user_middleware
             + middleware
        )  # latest called first

        for cls, options in middleware:
            call = cls(call, **options)
        return call

    def add_middleware(self, func: DISPATCH_TYPE) -> None:
        self.user_middleware.insert(0, Middleware(UserMiddleware, dispatch=func))

    def middleware(self, func: DISPATCH_TYPE) -> None:
        return self.add_middleware(func=func)
