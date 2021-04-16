import functools
from typing import Any, Callable, List, Type

from corva.api import Api
from corva.configuration import SETTINGS
from corva.models.base import RawBaseEvent
from corva.models.context import CorvaContext


def base_handler(raw_event_type: Type[RawBaseEvent]) -> Callable:
    def decorator(func: Callable[[RawBaseEvent, Api, str], Any]) -> Callable:
        @functools.wraps(func)
        def wrapper(aws_event: Any, aws_context: Any) -> List[Any]:
            context = CorvaContext.parse_obj(aws_context)

            api = Api(
                api_url=SETTINGS.API_ROOT_URL,
                data_api_url=SETTINGS.DATA_API_ROOT_URL,
                api_key=context.api_key,
                app_name=SETTINGS.APP_NAME,
                timeout=None,
            )

            raw_events = raw_event_type.from_raw_event(event=aws_event)

            results = [
                func(raw_event, api, context.aws_request_id) for raw_event in raw_events
            ]

            return results

        return wrapper

    return decorator
