import contextlib
import functools
from typing import Any, Callable, List, Type

from corva.api import Api
from corva.configuration import SETTINGS
from corva.logger import setup_logging
from corva.models.base import RawBaseEvent
from corva.models.context import CorvaContext
from corva.models.scheduled import RawScheduledEvent, ScheduledEvent
from corva.models.stream.raw import RawStreamEvent
from corva.models.stream.stream import StreamEvent
from corva.models.task import RawTaskEvent, TaskEvent, TaskStatus
from corva.state.redis_state import RedisState, get_cache


def base_handler(raw_event_type: Type[RawBaseEvent]) -> Callable:
    def decorator(func: Callable[[RawBaseEvent, Api, str], Any]) -> Callable:
        @functools.wraps(func)
        def wrapper(aws_event: Any, aws_context: Any) -> List[Any]:
            context = CorvaContext.parse_obj(aws_context)

            api = Api(
                api_url=SETTINGS.API_ROOT_URL,
                data_api_url=SETTINGS.DATA_API_ROOT_URL,
                api_key=context.api_key,
                app_key=SETTINGS.APP_KEY,
                timeout=None,
            )

            raw_events = raw_event_type.from_raw_event(event=aws_event)

            results = [
                func(raw_event, api, context.aws_request_id) for raw_event in raw_events
            ]

            return results

        return wrapper

    return decorator


def stream(func: Callable[[StreamEvent, Api, RedisState], Any]) -> Callable:
    """Runs stream app."""

    @functools.wraps(func)
    @base_handler(raw_event_type=RawStreamEvent)
    def wrapper(event: RawStreamEvent, api: Api, aws_request_id: str) -> Any:
        cache = get_cache(
            asset_id=event.asset_id,
            app_stream_id=event.app_stream_id,
            app_connection_id=event.app_connection_id,
            provider=SETTINGS.PROVIDER,
            app_key=SETTINGS.APP_KEY,
            cache_url=SETTINGS.CACHE_URL,
            cache_settings=None,
        )

        records = event.filter_records(
            old_max_record_value=event.get_cached_max_record_value(cache=cache)
        )

        if not records:
            # we've got the duplicate data if there are no records left after filtering
            return

        app_event = event.metadata.log_type.event.parse_obj(
            event.copy(update={'records': records}, deep=True)
        )

        with setup_logging(
            aws_request_id=aws_request_id,
            asset_id=event.asset_id,
            app_connection_id=event.app_connection_id,
        ):
            result = func(app_event, api, cache)

        with contextlib.suppress(Exception):
            # lambda succeeds if we're unable to cache the value
            event.set_cached_max_record_value(cache=cache)

        return result

    return wrapper


def scheduled(func: Callable[[ScheduledEvent, Api, RedisState], Any]) -> Callable:
    """Runs scheduled app."""

    @functools.wraps(func)
    @base_handler(raw_event_type=RawScheduledEvent)
    def wrapper(event: RawScheduledEvent, api: Api, aws_request_id: str) -> Any:
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
            aws_request_id=aws_request_id,
            asset_id=event.asset_id,
            app_connection_id=event.app_connection_id,
        ):
            result = func(ScheduledEvent.parse_obj(event), api, cache)

        with contextlib.suppress(Exception):
            # lambda succeeds if we're unable to set completed status
            event.set_schedule_as_completed(api=api)

        return result

    return wrapper


def task(func: Callable[[TaskEvent, Api], Any]) -> Callable:
    """Runs task app."""

    @base_handler(raw_event_type=RawTaskEvent)
    @functools.wraps(func)
    def wrapper(event: RawTaskEvent, api: Api, aws_request_id: str) -> Any:
        try:
            app_event = event.get_task_event(api=api)

            with setup_logging(
                aws_request_id=aws_request_id,
                asset_id=app_event.asset_id,
                app_connection_id=None,
            ):
                result = func(app_event, api)

            status = TaskStatus.success
            data = {'payload': result}

            return result

        except Exception as exc:
            status = TaskStatus.fail
            data = {'fail_reason': str(exc)}

        finally:
            with contextlib.suppress(Exception):
                # lambda succeeds if we're unable to update task data
                event.update_task_data(
                    api=api,
                    status=status,
                    data=data,
                )

    return wrapper
