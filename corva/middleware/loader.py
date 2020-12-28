from typing import Callable

from corva.loader.base import BaseLoader
from corva.models.base import BaseContext


def loader(context: BaseContext, call_next: Callable, *, loader: BaseLoader) -> BaseContext:
    context.event = loader.load(event=context.raw_event)
    context = call_next(context)
    return context
