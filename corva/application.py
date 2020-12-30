from typing import Any, Callable, List, Optional

from corva import settings
from corva.middleware.splitter import splitter_factory
from corva.middleware.stream import stream
from corva.middleware.unpack_context import unpack_context_factory
from corva.models.stream import StreamContext


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

    def Corva(
         self,
         func=None,
         *,
         filter_by_timestamp=False,
         filter_by_depth=False,

         # misc params
         app_key: str = settings.APP_KEY,

         # api params
         api_url: str = settings.API_ROOT_URL,
         api_data_url: str = settings.DATA_API_ROOT_URL,
         api_key: str = settings.API_KEY,
         api_app_name: str = settings.APP_NAME,
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,

         # cache params
         cache_url: str = settings.CACHE_URL,
         cache_kwargs: Optional[dict] = None,
    ) -> Callable:
        def decorator(func) -> Callable:
            def wrapper(event, **kwargs) -> Any:
                middleware = [
                    splitter_factory(split_by_field='app_connection_id'),
                    stream
                ]
                tail_middleware = [
                    unpack_context_factory(include_state=True)
                ]

                middleware_stack = self.get_middleware_stack(
                    middleware=middleware,
                    tail_middleware=tail_middleware
                )

                call = wrap_call_in_middleware(call=func, middleware=middleware_stack)

                ctx = StreamContext(
                    raw_event=event,
                    app_key=app_key,
                    api_url=api_url,
                    api_data_url=api_data_url,
                    api_key=api_key,
                    api_app_name=api_app_name,
                    api_timeout=api_timeout,
                    api_max_retries=api_max_retries,
                    cache_url=cache_url,
                    cache_kwargs=cache_kwargs,
                    filter_by_timestamp=filter_by_timestamp,
                    filter_by_depth=filter_by_depth
                )
                ctxs = call(ctx)  # type: List[StreamContext]

                return [ctx.user_result for ctx in ctxs]

            return wrapper

        if func is None:
            return decorator
        else:
            return decorator(func)
