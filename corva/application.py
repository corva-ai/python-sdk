from typing import Any, Callable, List, Literal, Optional

from corva.api import Api
from corva.configuration import SETTINGS
from corva.models.scheduled import ScheduledContext, ScheduledEvent
from corva.models.stream import StreamContext, StreamEvent
from corva.runners.scheduled import scheduled_runner
from corva.runners.stream import stream_runner


class Corva:
    def __init__(
        self,
        context: Any,
        *,
        timeout: Optional[int] = None,
        cache_settings: Optional[dict] = None
    ):
        """
        params:
         context: AWS Lambda context object
         timeout: api request timeout, set None to use default value
         cache_settings: additional cache settings
        """

        try:
            api_key = context.client_context.env["API_KEY"]
        except (AttributeError, KeyError):
            raise Exception('No API Key found.')

        self.cache_settings = cache_settings or {}

        self.api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_name=SETTINGS.APP_NAME,
            timeout=timeout,
        )

    def stream(
        self,
        fn: Callable,
        event: List[dict],
        *,
        filter_mode: Optional[Literal['timestamp', 'depth']] = None
    ) -> List[Any]:
        """Runs stream app

        params:
         fn: stream app function to run
         event: raw stream event
         filter_mode: remove records with previously processed `timestamp` or `measured_depth`
           from the event
        returns: list of returned values from fn
        """

        events = StreamEvent.from_raw_event(event=event)

        results = []

        for event in events:
            ctx = StreamContext(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_settings=self.cache_settings,
                filter_mode=filter_mode,
            )

            results.append(stream_runner(fn=fn, context=ctx))

        return results

    def scheduled(self, fn: Callable, event: List[List[dict]]) -> List[Any]:
        """Runs scheduled app

        params:
         fn: scheduled app function to run
         event: raw scheduled event
        returns: list of returned values from fn
        """

        events = ScheduledEvent.from_raw_event(event=event)

        results = []

        for event in events:
            ctx = ScheduledContext(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_settings=self.cache_settings,
            )

            results.append(scheduled_runner(fn=fn, context=ctx))

        return results
