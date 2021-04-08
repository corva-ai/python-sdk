from typing import Any, Callable, List, Optional

from corva.api import get_api
from corva.configuration import SETTINGS
from corva.models.scheduled import RawScheduledEvent, ScheduledContext
from corva.models.stream.raw import RawStreamEvent
from corva.models.task import RawTaskEvent, TaskContext
from corva.runners.scheduled import scheduled_runner
from corva.runners.stream import stream_runner
from corva.runners.task import task_runner


class Corva:
    """Provides functionality to run apps.

    Attributes:
        cache_settings: custom cache params.
        api: Api instance.
        aws_request_id: aws request id from the lambda context
    """

    def __init__(
        self,
        context: Any,
        *,
        timeout: Optional[int] = None,
        cache_settings: Optional[dict] = None
    ):
        """

        Args:
            context: AWS Lambda context object.
            timeout: api request timeout, set None to use default value.
            cache_settings: additional cache settings.
        """

        self.api = get_api(context=context, settings=SETTINGS, timeout=timeout)
        self.aws_request_id = context.aws_request_id
        self.cache_settings = cache_settings or {}

    def stream(
        self,
        fn: Callable,
        event: List[dict],
    ) -> List[Any]:
        """Runs stream app.

        params:
         fn: stream app function to run
         event: raw stream event
        returns: list of returned values from fn
        """

        events = RawStreamEvent.from_raw_event(event=event)

        results = []

        for event in events:
            ctx = event.metadata.log_type.context(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_settings=self.cache_settings,
                aws_request_id=self.aws_request_id,
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

        events = RawScheduledEvent.from_raw_event(event=event)

        results = []

        for event in events:
            ctx = ScheduledContext(
                event=event,
                settings=SETTINGS.copy(),
                api=self.api,
                cache_settings=self.cache_settings,
                aws_request_id=self.aws_request_id,
            )

            results.append(scheduled_runner(fn=fn, context=ctx))

        return results

    def task(self, fn: Callable, event: dict) -> Any:
        """Runs task app

        params:
         fn: task app function to run
         event: raw task event
        returns: returned value from fn
        """

        event = RawTaskEvent.from_raw_event(event=event)

        ctx = TaskContext(
            event=event,
            settings=SETTINGS.copy(),
            api=self.api,
            cache_settings=self.cache_settings,
            aws_request_id=self.aws_request_id,
        )

        result = task_runner(fn=fn, context=ctx)

        return result
