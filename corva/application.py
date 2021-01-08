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

    def stream(
         self,
         func=None,
         *,
         filter_by_timestamp=False,
         filter_by_depth=False,

         # misc params
         app_key: Optional[str] = None,

         # api params
         api_url: Optional[str] = None,
         api_data_url: Optional[str] = None,
         api_key: Optional[str] = None,
         api_app_name: Optional[str] = None,
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,

         # cache params
         cache_url: Optional[str] = None,
         cache_kwargs: Optional[dict] = None,
    ) -> Callable:
        def wrapper_factory(func) -> Callable:
            def wrapper(event) -> Any:
                nonlocal app_key, api_url, api_data_url, api_key, api_app_name, cache_url

                app_key = app_key or settings.APP_KEY
                api_url = api_url or settings.API_ROOT_URL
                api_data_url = api_data_url or settings.DATA_API_ROOT_URL
                api_key = api_key or settings.API_KEY
                api_app_name = api_app_name or settings.APP_NAME
                cache_url = cache_url or settings.CACHE_URL

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
            return wrapper_factory

        return wrapper_factory(func)
