from logging import Logger
from typing import Any, Callable, Union

from corva.logger import DEFAULT_LOGGER
from corva.models.base import BaseContext
from corva.models.scheduled import ScheduledContext
from corva.models.stream import StreamContext
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class InitStateMiddleware:
    def __init__(
         self,
         call: Callable[[BaseContext], Any],
         *,
         default_name: str,
         cache_url: str,
         cache_kwargs: dict = None,
         logger: Logger = DEFAULT_LOGGER
    ):
        self.call = call
        self.default_name = default_name
        self.cache_url = cache_url
        self.cache_kwargs = cache_kwargs or {}
        self.logger = logger

    def __call__(self, context: Union[StreamContext, ScheduledContext]) -> Any:
        adapter = RedisAdapter(
            default_name=self.default_name,
            cache_url=self.cache_url,
            logger=self.logger,
            **self.cache_kwargs
        )
        context.state = RedisState(redis=adapter, logger=self.logger)

        return self.call(context)
