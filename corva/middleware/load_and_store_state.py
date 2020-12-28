from typing import Callable

from corva.models.base import BaseContext


def load_and_store_state(context: BaseContext, call_next: Callable) -> BaseContext:
    state_data_dict = context.state.load_all()
    context.state_data = context.__fields__['state_data'].type_(**state_data_dict)

    context = call_next(context)

    if context.state_data:
        context.state.store(mapping=context.state_data.dict(exclude_defaults=True))

    return context
