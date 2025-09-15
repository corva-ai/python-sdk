import contextlib
import functools
import itertools
import logging
import sys
import warnings
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import pydantic
import redis

from corva.api import Api
from corva.configuration import SETTINGS
from corva.logger import CORVA_LOGGER, CorvaLoggerHandler, LoggingContext
from corva.models import validators
from corva.models.base import RawBaseEvent
from corva.models.context import CorvaContext
from corva.models.merge.merge import PartialRerunMergeEvent
from corva.models.merge.raw import RawPartialRerunMergeEvent
from corva.models.scheduled.raw import RawScheduledDataTimeEvent, RawScheduledEvent
from corva.models.scheduled.scheduled import ScheduledEvent, ScheduledNaturalTimeEvent
from corva.models.scheduled.scheduler_type import SchedulerType
from corva.models.stream.raw import RawStreamEvent
from corva.models.stream.stream import StreamEvent
from corva.models.task import RawTaskEvent, TaskEvent, TaskStatus
from corva.service import service
from corva.service.api_sdk import CachingApiSdk, CorvaApiSdk
from corva.service.cache_sdk import FakeInternalCacheSdk, InternalRedisSdk, UserRedisSdk
from corva.validate_app_init import validate_app_type_context

StreamEventT = TypeVar("StreamEventT", bound=StreamEvent)
ScheduledEventT = TypeVar("ScheduledEventT", bound=ScheduledEvent)
HANDLERS: Dict[Type[RawBaseEvent], Callable] = {}
GENERIC_APP_EVENT_TYPES = (
    RawStreamEvent,
    RawScheduledEvent,
    RawTaskEvent,
)


def get_cache_key(
    provider: str,
    asset_id: int,
    app_stream_id: int,
    app_key: str,
    app_connection_id: int,
) -> str:
    return (
        f"{provider}/well/{asset_id}/stream/{app_stream_id}/"
        f"{app_key}/{app_connection_id}"
    )


def base_handler(
    func: Callable,
    raw_event_type: Type[RawBaseEvent],
    handler: Optional[logging.Handler],
    merge_events: bool = False,
) -> Callable[[Any, Any], List[Any]]:
    @functools.wraps(func)
    def wrapper(aws_event: Any, aws_context: Any) -> List[Any]:
        with LoggingContext(
            aws_request_id=aws_context.aws_request_id,
            asset_id=None,
            app_connection_id=None,
            handler=logging.StreamHandler(stream=sys.stdout),
            user_handler=handler,
            logger=CORVA_LOGGER,
        ) as logging_ctx:
            # Verify either current call from app_decorator or not
            # for instance from partial rerun merge
            (
                raw_custom_event_type,
                custom_handler,
            ) = _get_custom_event_type_by_raw_aws_event(aws_event)
            is_direct_app_call: bool = not custom_handler
            data_transformation_type = raw_custom_event_type or raw_event_type
            if merge_events:
                aws_event = _merge_events(aws_event, data_transformation_type)

            if (
                is_direct_app_call
                and data_transformation_type not in GENERIC_APP_EVENT_TYPES
            ):
                CORVA_LOGGER.warning(
                    f"Handler for {data_transformation_type.__name__!r} "
                    f"event not found. Skipping..."
                )
                return []

            if is_direct_app_call:
                # Means current app call is not RawPartialRerunMergeEvent or similar
                validate_app_type_context(aws_event, raw_event_type)

            try:
                context = CorvaContext.from_aws(
                    aws_event=aws_event, aws_context=aws_context
                )

                redis_client = redis.Redis.from_url(
                    url=SETTINGS.CACHE_URL, decode_responses=True, max_connections=1
                )
                raw_events = data_transformation_type.from_raw_event(event=aws_event)
                specific_callable = custom_handler or func

                results = [
                    specific_callable(
                        raw_event,
                        context.api_key,
                        context.aws_request_id,
                        logging_ctx,
                        redis_client,
                    )
                    for raw_event in raw_events
                ]

                return results

            except Exception:
                CORVA_LOGGER.exception("The app failed to execute.")
                raise

    return wrapper


def stream(
    func: Optional[Callable[[StreamEventT, Api, UserRedisSdk], Any]] = None,
    *,
    handler: Optional[logging.Handler] = None,
    merge_events: bool = False,
) -> Callable:
    """Runs stream app.

    Arguments:
        handler: logging handler to include in Corva logger.
        merge_events: if True - merge all incoming events into one before
          passing them to func
    """

    if func is None:
        return functools.partial(stream, handler=handler, merge_events=merge_events)

    @functools.wraps(func)
    @functools.partial(
        base_handler,
        raw_event_type=RawStreamEvent,
        handler=handler,
        merge_events=merge_events,
    )
    def wrapper(
        event: RawStreamEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
        redis_client: redis.Redis,
    ) -> Any:
        logging_ctx.asset_id = event.asset_id
        logging_ctx.app_connection_id = event.app_connection_id

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.asset_id,
            app_stream_id=event.app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        user_cache_sdk = UserRedisSdk(
            hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL, redis_client=redis_client
        )
        internal_cache_sdk = InternalRedisSdk(
            hash_name=hash_name, redis_client=redis_client
        )

        records = event.filter_records(
            old_max_record_value=event.get_cached_max_record_value(cache=user_cache_sdk)
        )

        if not records:
            # we've got the duplicate data if there are no records left after filtering
            return

        app_event = event.metadata.log_type.event.parse_obj(
            event.copy(update={"records": records}, deep=True)
        )
        with LoggingContext(
            aws_request_id=aws_request_id,
            asset_id=event.asset_id,
            app_connection_id=event.app_connection_id,
            handler=CorvaLoggerHandler(
                max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
                max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
                logger=CORVA_LOGGER,
                placeholder=" ...",
            ),
            user_handler=handler,
            logger=CORVA_LOGGER,
        ):
            result = service.run_app(
                has_secrets=event.has_secrets,
                app_key=SETTINGS.APP_KEY,
                api_sdk=CachingApiSdk(
                    api_sdk=CorvaApiSdk(api_adapter=api),
                    ttl=SETTINGS.SECRETS_CACHE_TTL,
                ),
                cache_sdk=internal_cache_sdk,
                app=functools.partial(
                    cast(Callable[[StreamEvent, Api, UserRedisSdk], Any], func),
                    app_event,
                    api,
                    user_cache_sdk,
                ),
            )

        try:
            event.set_cached_max_record_value(cache=user_cache_sdk)
        except Exception as e:
            # lambda succeeds if we're unable to cache the value
            CORVA_LOGGER.warning(f"Could not save data to cache. Details: {str(e)}.")

        return result

    return wrapper


def scheduled(
    func: Optional[Callable[[ScheduledEventT, Api, UserRedisSdk], Any]] = None,
    *,
    handler: Optional[logging.Handler] = None,
    merge_events: bool = False,
) -> Callable:
    """Runs scheduled app.

    Arguments:
        handler: logging handler to include in Corva logger.
        merge_events: if True - merge all incoming events into one before
          passing them to func
    """

    if func is None:
        return functools.partial(scheduled, handler=handler, merge_events=merge_events)

    @functools.wraps(func)
    @functools.partial(
        base_handler,
        raw_event_type=RawScheduledEvent,
        handler=handler,
        merge_events=merge_events,
    )
    def wrapper(
        event: RawScheduledEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
        redis_client: redis.Redis,
    ) -> Any:
        logging_ctx.asset_id = event.asset_id
        logging_ctx.app_connection_id = event.app_connection_id

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.asset_id,
            app_stream_id=event.app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        user_cache_sdk = UserRedisSdk(
            hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL, redis_client=redis_client
        )
        internal_cache_sdk = InternalRedisSdk(
            hash_name=hash_name, redis_client=redis_client
        )

        if isinstance(event, RawScheduledDataTimeEvent) and event.merge_metadata:
            event = event.rebuild_with_modified_times(
                event.merge_metadata.start_time, event.merge_metadata.end_time
            )

        app_event = event.scheduler_type.event.parse_obj(event)

        with LoggingContext(
            aws_request_id=aws_request_id,
            asset_id=event.asset_id,
            app_connection_id=event.app_connection_id,
            handler=CorvaLoggerHandler(
                max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
                max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
                logger=CORVA_LOGGER,
                placeholder=" ...",
            ),
            user_handler=handler,
            logger=CORVA_LOGGER,
        ):
            try:
                result = service.run_app(
                    has_secrets=event.has_secrets,
                    app_key=SETTINGS.APP_KEY,
                    api_sdk=CachingApiSdk(
                        api_sdk=CorvaApiSdk(api_adapter=api),
                        ttl=SETTINGS.SECRETS_CACHE_TTL,
                    ),
                    cache_sdk=internal_cache_sdk,
                    app=functools.partial(
                        cast(Callable[[ScheduledEvent, Api, UserRedisSdk], Any], func),
                        app_event,
                        api,
                        user_cache_sdk,
                    ),
                )
            except Exception:
                if isinstance(app_event, ScheduledNaturalTimeEvent):
                    set_schedule_as_completed(event=event, api=api)
                raise
            else:
                set_schedule_as_completed(event=event, api=api)

        return result

    return wrapper


def set_schedule_as_completed(event: RawScheduledEvent, api: Api) -> None:
    try:
        event.set_schedule_as_completed(api=api)
    except Exception as e:
        # lambda succeeds if we're unable to set completed status
        CORVA_LOGGER.warning(f"Could not set schedule as completed. Details: {str(e)}.")


def task(
    func: Optional[Callable[[TaskEvent, Api], Any]] = None,
    *,
    handler: Optional[logging.Handler] = None,
) -> Callable:
    """Runs task app.

    Arguments:
        handler: logging handler to include in Corva logger.
    """

    if func is None:
        return functools.partial(task, handler=handler)

    @functools.wraps(func)
    @functools.partial(
        base_handler,
        raw_event_type=RawTaskEvent,
        handler=handler,
    )
    def wrapper(
        event: RawTaskEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
        redis_client: redis.Redis,
    ) -> Any:
        status = TaskStatus.fail
        data: Dict[str, Union[dict, str]] = {"payload": {}}

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=None,
        )

        try:
            app_event = event.get_task_event(api=api)

            logging_ctx.asset_id = app_event.asset_id

            with LoggingContext(
                aws_request_id=aws_request_id,
                asset_id=app_event.asset_id,
                app_connection_id=None,
                handler=CorvaLoggerHandler(
                    max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
                    max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
                    logger=CORVA_LOGGER,
                    placeholder=" ...",
                ),
                user_handler=handler,
                logger=CORVA_LOGGER,
            ):
                result = service.run_app(
                    has_secrets=event.has_secrets,
                    app_key=SETTINGS.APP_KEY,
                    api_sdk=CachingApiSdk(
                        api_sdk=CorvaApiSdk(api_adapter=api),
                        ttl=SETTINGS.SECRETS_CACHE_TTL,
                    ),
                    cache_sdk=FakeInternalCacheSdk(),
                    app=functools.partial(
                        cast(Callable[[TaskEvent, Api], Any], func), app_event, api
                    ),
                )

            if isinstance(result, dict):
                warnings.warn(
                    "Returning dict result from task app to get it stored in task "
                    "payload is deprecated and will be removed from corva in the next "
                    "major version. Update the payload explicitly in your app.",
                    FutureWarning,
                )

                data["payload"] = result

            status = TaskStatus.success

            return result

        except Exception as exc:
            CORVA_LOGGER.exception("Task app failed to execute.")
            data = {"fail_reason": str(exc)}
            raise

        finally:
            try:
                event.update_task_data(
                    api=api,
                    status=status,
                    data=data,
                ).raise_for_status()
            except Exception as e:
                # lambda succeeds if we're unable to update task data
                CORVA_LOGGER.warning(f"Could not update task data. Details: {str(e)}.")

    return wrapper


def partial_rerun_merge(
    func: Optional[
        Callable[[PartialRerunMergeEvent, Api, UserRedisSdk, UserRedisSdk], Any]
    ] = None,
    *,
    handler: Optional[logging.Handler] = None,
) -> Callable:
    """Runs partial merge app.

    Arguments:
        handler: logging handler to include in Corva logger.
    """

    if func is None:
        return functools.partial(partial_rerun_merge, handler=handler)

    @functools.wraps(func)
    def wrapper(
        event: RawPartialRerunMergeEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
        redis_client: redis.Redis,
    ) -> Any:
        logging_ctx.asset_id = event.data.asset_id
        logging_ctx.app_connection_id = event.data.app_connection_id

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
        )

        asset_cache_hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.data.asset_id,
            app_stream_id=event.data.app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.data.app_connection_id,
        )
        rerun_asset_cache_hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.data.rerun_asset_id,
            app_stream_id=event.data.rerun_app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.data.rerun_app_connection_id,
        )
        internal_cache_hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.data.rerun_asset_id,
            app_stream_id=event.data.rerun_app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.data.app_connection_id,
        )

        asset_cache = UserRedisSdk(
            hash_name=asset_cache_hash_name,
            redis_dsn=SETTINGS.CACHE_URL,
            redis_client=redis_client,
        )
        rerun_asset_cache = UserRedisSdk(
            hash_name=rerun_asset_cache_hash_name,
            redis_dsn=SETTINGS.CACHE_URL,
            redis_client=redis_client,
        )
        app_event = PartialRerunMergeEvent(
            **event.data.dict(), event_type=event.event_type
        )

        with LoggingContext(
            aws_request_id=aws_request_id,
            asset_id=event.data.asset_id,
            app_connection_id=event.data.app_connection_id,
            handler=CorvaLoggerHandler(
                max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
                max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
                logger=CORVA_LOGGER,
                placeholder=" ...",
            ),
            user_handler=handler,
            logger=CORVA_LOGGER,
        ):
            result = service.run_app(
                has_secrets=event.has_secrets,
                app_key=SETTINGS.APP_KEY,
                api_sdk=CachingApiSdk(
                    api_sdk=CorvaApiSdk(api_adapter=api),
                    ttl=SETTINGS.SECRETS_CACHE_TTL,
                ),
                cache_sdk=InternalRedisSdk(
                    hash_name=internal_cache_hash_name, redis_client=redis_client
                ),
                app=functools.partial(
                    cast(
                        Callable[
                            [PartialRerunMergeEvent, Api, UserRedisSdk, UserRedisSdk],
                            Any,
                        ],
                        func,
                    ),
                    app_event,
                    api,
                    asset_cache,
                    rerun_asset_cache,
                ),
            )

        return result

    HANDLERS[RawPartialRerunMergeEvent] = wrapper
    return wrapper


def _get_custom_event_type_by_raw_aws_event(
    aws_event: Any,
) -> Union[Tuple[Type[RawBaseEvent], Callable], Tuple[None, None]]:
    events = None
    # Here we do not know what schema will future custom events have,
    # so trying to parse all registered custom types.
    for event_type, handler in HANDLERS.items():
        with contextlib.suppress(pydantic.ValidationError):
            events = event_type.from_raw_event(aws_event)
        if events:
            return event_type, handler
    return None, None


def _merge_events(
    aws_event: Any,
    data_transformation_type: Type[RawBaseEvent],
) -> Any:
    """
    Merges incoming aws_events into one.
    Merge happens differently, depending on app type.
    Only "scheduled"(data and depth) and "stream" type of apps can be processed here.
    Scheduled Natural events don't need a merge since they will never receive multiple
    events in batch.
    If somehow any other type is passed - raise an exception
    """
    if not isinstance(
        aws_event, list
    ):  # if aws_event is not a list - there is nothing to merge, so do nothing.
        return aws_event

    if data_transformation_type is RawScheduledEvent:
        # scheduled event
        if not isinstance(aws_event[0], dict):
            aws_event = list(itertools.chain(*aws_event))
        scheduler_type = aws_event[0]["scheduler_type"]
        if isinstance(scheduler_type, SchedulerType):
            scheduler_type = scheduler_type.value
        is_depth = scheduler_type == SchedulerType.data_depth_milestone.value
        event_start, event_end = (
            ("top_depth", "bottom_depth")
            if is_depth
            else ("schedule_start", "schedule_end")
        )
        min_event_start: int = min(e[event_start] for e in aws_event)
        max_event_end = max(
            (e[event_end] for e in aws_event if e.get(event_end) is not None),
            default=None,
        )
        if not is_depth:
            # we're working with ScheduledDataTimeEvent
            max_event_start: int = max(e[event_start] for e in aws_event)
            interval = aws_event[0]["interval"]
            # cast from ms to s if needed
            min_value = validators.from_ms_to_s(min_event_start)
            max_value = validators.from_ms_to_s(max_event_start)
            aws_event[0]["merge_metadata"] = {
                "start_time": int(min_value - interval + 1),
                "end_time": int(max_value),
                "number_of_merged_events": len(aws_event),
            }

        aws_event[0][event_start] = min_event_start
        if max_event_end:
            aws_event[0][event_end] = max_event_end
        aws_event = aws_event[0]

    elif data_transformation_type is RawStreamEvent:
        # stream event
        for event in aws_event[1:]:
            aws_event[0]["records"].extend(event["records"])
        aws_event = [aws_event[0]]

    else:
        CORVA_LOGGER.warning(
            f"{data_transformation_type.__name__} does not support `merge event` "
            "parameter."
        )

    return aws_event
