from typing import Any, Callable

from corva.models.base import BaseContext
from corva.types import DISPATCH_TYPE


class UserMiddleware:
    """Wraps user's middleware function"""

    def __init__(
         self,
         call: Callable[[BaseContext], Any],
         dispatch: DISPATCH_TYPE
    ):
        self.call = call
        self.dispatch = dispatch

    def __call__(self, context: BaseContext) -> Any:
        return self.dispatch(context, self.call)
