from typing import Callable, Optional

from corva.models.base import BaseContext


def get_loader_fn(loader: Callable, loader_kwargs: Optional[dict] = None) -> Callable:
    def loader_(context: BaseContext, call_next: Callable) -> BaseContext:
        context.event = loader(context.raw_event, **(loader_kwargs or {}))

        context = call_next(context)

        return context

    return loader_
