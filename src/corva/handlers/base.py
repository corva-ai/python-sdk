import functools
from typing import Any, List, Union

from corva.api import Api
from corva.configuration import SETTINGS
from corva.logger import setup_logging
from corva.models.context import CorvaContext
from corva.models.scheduled import RawScheduledEvent
from corva.models.stream.raw import RawStreamEvent
from corva.models.task import RawTaskEvent
from corva.state.redis_state import get_cache


def base_handler(func):
    @functools.wraps(func)
    def wrapper(
        event: Union[dict, List[dict], List[List[dict]]], context: Any
    ) -> List[Any]:
        context = CorvaContext.parse_obj(context)

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=context.api_key,
            app_name=SETTINGS.APP_NAME,
            timeout=None,
        )

        if isinstance(event, dict):
            return [
                func(
                    event=RawTaskEvent.from_raw_event(event),
                    api=api,
                    aws_request_id=context.aws_request_id,
                )
            ]

        if any(isinstance(item, dict) for item in event):
            events = RawStreamEvent.from_raw_event(event)
        else:
            events = RawScheduledEvent.from_raw_event(event)

        results = []
        for event in events:
            cache = get_cache(
                asset_id=event.asset_id,
                app_stream_id=event.app_stream_id,
                app_connection_id=event.app_connection_id,
                provider=SETTINGS.PROVIDER,
                app_key=SETTINGS.APP_KEY,
                cache_url=SETTINGS.CACHE_URL,
                cache_settings=None,
            )

            with setup_logging(
                aws_request_id=context.aws_request_id,
                asset_id=event.asset_id,
                app_connection_id=event.app_connection_id,
            ):
                results.append(
                    func(
                        event=event,
                        api=api,
                        cache=cache,
                    )
                )

        return results

    return wrapper
