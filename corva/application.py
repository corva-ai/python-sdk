from functools import partial, wraps
from typing import Any, Callable, List, Optional

from corva.middleware.stream import stream
from corva.middleware.unpack_context import unpack_context_factory
from corva.models.stream import StreamContext, StreamEvent
from corva.settings import Settings, SETTINGS


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

    def add_middleware(self, func: Callable) -> None:
        self.user_middleware.append(func)

    def stream(
         self,
         func=None,
         *,
         filter_by_timestamp: bool = False,
         filter_by_depth: bool = False,

         settings: Optional[Settings] = None,

         # api params
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,

         # cache params
         cache_kwargs: Optional[dict] = None
    ) -> Callable:
        """Decorates a function to be a stream one

        Can be used both with and without arguments.
        https://github.com/dabeaz/python-cookbook/blob/master/src/9/defining_a_decorator_that_takes_an_optional_argument/example.py
        """

        if func is None:
            return partial(
                self.stream,
                filter_by_timestamp=filter_by_timestamp,
                filter_by_depth=filter_by_depth,
                settings=settings,
                api_timeout=api_timeout,
                api_max_retries=api_max_retries,
                cache_kwargs=cache_kwargs
            )

        @wraps(func)
        def wrapper(event) -> List[Any]:
            settings_ = settings or SETTINGS.copy()

            middleware = [stream] + self.user_middleware + [unpack_context_factory(include_state=True)]

            call = wrap_call_in_middleware(call=func, middleware=middleware)

            events = StreamEvent.from_raw_event(event=event)

            results = []

            for event in events:
                ctx = StreamContext(
                    _event=event,
                    settings=settings_,
                    api_timeout=api_timeout,
                    api_max_retries=api_max_retries,
                    cache_kwargs=cache_kwargs,
                    filter_by_timestamp=filter_by_timestamp,
                    filter_by_depth=filter_by_depth
                )
                ctx = call(ctx)  # type: StreamContext
                results.append(ctx.user_result)

            return results

        return wrapper
