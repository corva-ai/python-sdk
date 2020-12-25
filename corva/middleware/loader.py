from typing import Any, Callable

from corva.loader.base import BaseLoader
from corva.models.base import BaseContext


class LoaderMiddleware:
    def __init__(self, call: Callable[[BaseContext], Any], loader: BaseLoader):
        self.call = call
        self.loader = loader

    def __call__(self, context: BaseContext) -> Any:
        context.event = self.loader.load(event=context.raw_event)
        return self.call(context)
