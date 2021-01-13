from functools import partial, wraps
from typing import Any, Callable, List, Optional, Type

from corva.middleware.scheduled import scheduled
from corva.middleware.stream import stream
from corva.middleware.unpack_context import unpack_context_factory
from corva.models.base import BaseContext, BaseEvent
from corva.models.scheduled import ScheduledEvent, ScheduledContext
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


def app_wrapper(
     event,
     *,
     func: Callable,
     event_cls: Type[BaseEvent],
     context_cls: Type[BaseContext],
     head_middleware: Optional[List[Callable]] = None,
     user_middleware: Optional[List[Callable]] = None,
     tail_middleware: Optional[List[Callable]] = None,
     context_kwargs: Optional[dict] = None,
     settings: Optional[Settings] = None,
     api_timeout: Optional[int] = None,
     api_max_retries: Optional[int] = None,
     cache_kwargs: Optional[dict] = None
) -> List[Any]:
    head_middleware = head_middleware or []
    user_middleware = user_middleware or []
    tail_middleware = tail_middleware or []
    context_kwargs = context_kwargs or {}
    settings = settings or SETTINGS.copy()

    middleware = head_middleware + user_middleware + tail_middleware

    call = wrap_call_in_middleware(call=func, middleware=middleware)

    events = event_cls.from_raw_event(event=event, app_key=settings.APP_KEY)

    results = []

    for event in events:
        ctx = context_cls(
            event=event,
            settings=settings,
            api_timeout=api_timeout,
            api_max_retries=api_max_retries,
            cache_kwargs=cache_kwargs,
            **context_kwargs
        )
        ctx = call(ctx)  # type: BaseContext
        results.append(ctx.user_result)

    return results


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

        wrapper = partial(
            app_wrapper,
            func=func,
            head_middleware=[stream],
            user_middleware=self.user_middleware,
            tail_middleware=[unpack_context_factory(include_state=True)],
            event_cls=StreamEvent,
            context_cls=StreamContext,
            settings=settings,
            api_timeout=api_timeout,
            api_max_retries=api_max_retries,
            cache_kwargs=cache_kwargs,
            context_kwargs={
                'filter_by_timestamp': filter_by_timestamp,
                'filter_by_depth': filter_by_depth
            }
        )

        return wraps(func)(wrapper)

    def scheduled(
         self,
         func=None,
         *,
         settings: Optional[Settings] = None,

         # api params
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,

         # cache params
         cache_kwargs: Optional[dict] = None
    ):
        if func is None:
            return partial(
                self.scheduled,
                settings=settings,
                api_timeout=api_timeout,
                api_max_retries=api_max_retries,
                cache_kwargs=cache_kwargs
            )

        wrapper = partial(
            app_wrapper,
            func=func,
            head_middleware=[scheduled],
            user_middleware=self.user_middleware,
            tail_middleware=[unpack_context_factory(include_state=True)],
            event_cls=ScheduledEvent,
            context_cls=ScheduledContext,
            settings=settings,
            api_timeout=api_timeout,
            api_max_retries=api_max_retries,
            cache_kwargs=cache_kwargs,
            context_kwargs={}
        )

        return wraps(func)(wrapper)
