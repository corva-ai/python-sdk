from typing import Callable

from corva.models.base import BaseContext


def unpack_context_factory(include_state=False):
    def unpack_context(context: BaseContext, call_next: Callable) -> BaseContext:
        """
        Calls user function with 'unpacked' arguments from context.

        Corva app passes some arguments to user's function by default (e.g event, api),
        this middleware 'unpacks' arguments from context and calls user's function with them.
        """

        args = [context.event, context.api]

        if include_state:
            args.append(context.state)

        context.user_result = call_next(*args)

        return context

    return unpack_context
