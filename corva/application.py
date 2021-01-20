from typing import Any, Callable, List, Optional

from corva.configuration import SETTINGS
from corva.models.scheduled import ScheduledContext, ScheduledEvent
from corva.models.stream import StreamContext, StreamEvent
from corva.network.api import Api
from corva.runners.scheduled import scheduled_runner
from corva.runners.stream import stream_runner


class Corva:
    def __init__(
         self,
         timeout: Optional[int] = None,
         max_retries: Optional[int] = None,
         cache_settings: Optional[dict] = None
    ):
        """
        params:
         timeout: api request timeout, set None to use default value
         max_retries: number or api retries for failed request, set to None to use default value
         cache_settings: additional cache settings
        """

        self.cache_settings = cache_settings or {}

        self.api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=SETTINGS.API_KEY,
            app_name=SETTINGS.APP_NAME,
            timeout=timeout,
            max_retries=max_retries
        )

    def stream(
         self,
         fn: Callable,
         event: str,
         *,
         filter_by_timestamp: bool = False,
         filter_by_depth: bool = False
    ) -> List[Any]:
        events = StreamEvent.from_raw_event(event=event, app_key=SETTINGS.APP_KEY)

        results = []

        for event in events:
            ctx = StreamContext(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_kwargs=self.cache_settings,
                filter_by_timestamp=filter_by_timestamp,
                filter_by_depth=filter_by_depth
            )

            results.append(stream_runner(fn=fn, context=ctx))

        return results

    def scheduled(self, fn: Callable, event: str):
        events = ScheduledEvent.from_raw_event(event=event)

        results = []

        for event in events:
            ctx = ScheduledContext(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_kwargs=self.cache_settings
            )

            results.append(scheduled_runner(fn=fn, context=ctx))

        return results
