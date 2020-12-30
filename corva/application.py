from typing import Any, Callable, List, Optional

from corva.middleware.init_api import init_api_factory
from corva.middleware.init_state import init_state_factory
from corva.middleware.load_and_store_state import load_and_store_state
from corva.middleware.loader import loader_factory
from corva.middleware.splitter import splitter_factory
from corva.middleware.stream import stream
from corva.middleware.stream_filter import stream_filter_factory
from corva.middleware.unpack_context import unpack_context_factory
from corva.models.stream import StreamContext
from corva.models.stream import StreamEvent


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
         middleware: Optional[List[Callable]] = None,
         tail_middleware: Optional[List[Callable]] = None
    ) -> List[Callable]:
        middleware = middleware or []
        tail_middleware = tail_middleware or []

        middleware_stack = middleware + self.user_middleware + tail_middleware

        return middleware_stack

    def add_middleware(self, func: Callable) -> None:
        self.user_middleware.append(func)

    def stream(
         self,
         func=None,
         *,
         app_key: str,

         api_url: str,
         api_data_url: str,
         api_key: str,
         api_name: str,
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,

         cache_url: str,
         cache_kwargs: Optional[dict],

         filter_by_timestamp=False,
         filter_by_depth=False,

         include_context=False
    ) -> Callable:
        def decorator(func) -> Callable:
            def wrapper(event, **kwargs) -> Any:
                middleware = [
                    loader_factory(loader=StreamEvent.from_raw_event, loader_kwargs={'app_key': app_key}),
                    init_api_factory(
                        api_url=api_url,
                        data_api_url=api_data_url,
                        api_key=api_key,
                        api_name=api_name,
                        timeout=api_timeout,
                        max_retries=api_max_retries
                    ),
                    splitter_factory(split_by_field='app_connection_id'),
                    init_state_factory(cache_url=cache_url, cache_kwargs=cache_kwargs),
                    load_and_store_state,
                    stream_filter_factory(by_timestamp=filter_by_timestamp, by_depth=filter_by_depth),
                    stream
                ]
                tail_middleware = [
                    unpack_context_factory(include_state=True, include_context=include_context)
                ]

                middleware_stack = self.get_middleware_stack(
                    middleware=middleware,
                    tail_middleware=tail_middleware
                )

                call = wrap_call_in_middleware(call=func, middleware=middleware_stack)

                ctx = StreamContext(raw_event=event, user_kwargs=kwargs, app_key=app_key)
                ctxs = call(ctx)  # type: List[StreamContext]

                return [ctx.user_result for ctx in ctxs]

            return wrapper

        if func is None:
            return decorator
        else:
            return decorator(func)