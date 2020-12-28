from typing import Callable

from corva.loader.base import BaseLoader
from corva.models.base import BaseContext


def loader_factory(*, loader: BaseLoader) -> Callable:
    def loader_(context: BaseContext, call_next: Callable) -> BaseContext:
        context.event = loader.load(event=context.raw_event)
        context = call_next(context)
        return context

    return loader_
