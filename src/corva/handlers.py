import functools
import logging
import sys
import warnings
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, cast

from corva.api import Api
from corva.configuration import SETTINGS
from corva.logger import CORVA_LOGGER, CorvaLoggerHandler, LoggingContext
from corva.models.base import RawBaseEvent
from corva.models.context import CorvaContext
from corva.models.scheduled.raw import RawScheduledEvent
from corva.models.scheduled.scheduled import ScheduledEvent, ScheduledNaturalTimeEvent
from corva.models.stream.raw import RawStreamEvent
from corva.models.stream.stream import StreamEvent
from corva.models.task import RawTaskEvent, TaskEvent, TaskStatus
from corva.service import service
from corva.service.api_sdk import CachingApiSdk, CorvaApiSdk
from corva.service.cache_sdk import FakeInternalCacheSdk, InternalRedisSdk, UserRedisSdk

StreamEventT = TypeVar('StreamEventT', bound=StreamEvent)
ScheduledEventT = TypeVar('ScheduledEventT', bound=ScheduledEvent)


def get_cache_key(
    provider: str,
    asset_id: int,
    app_stream_id: int,
    app_key: str,
    app_connection_id: int,
) -> str:
    return (
        f'{provider}/well/{asset_id}/stream/{app_stream_id}/'
        f'{app_key}/{app_connection_id}'
    )


def base_handler(
    func: Callable,
    raw_event_type: Type[RawBaseEvent],
    handler: Optional[logging.Handler],
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
            try:
                context = CorvaContext.from_aws(
                    aws_event=aws_event, aws_context=aws_context
                )

                raw_events = raw_event_type.from_raw_event(event=aws_event)

                results = [
                    func(
                        raw_event, context.api_key, context.aws_request_id, logging_ctx
                    )
                    for raw_event in raw_events
                ]

                return results

            except Exception:
                CORVA_LOGGER.exception('The app failed to execute.')
                raise

    return wrapper


def stream(
    func: Optional[Callable[[StreamEventT, Api, UserRedisSdk], Any]] = None,
    *,
    handler: Optional[logging.Handler] = None,
) -> Callable:
    """Runs stream app.

    Arguments:
        handler: logging handler to include in Corva logger.
    """

    if func is None:
        return functools.partial(stream, handler=handler)

    @functools.wraps(func)
    @functools.partial(base_handler, raw_event_type=RawStreamEvent, handler=handler)
    def wrapper(
        event: RawStreamEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
    ) -> Any:
        logging_ctx.asset_id = event.asset_id
        logging_ctx.app_connection_id = event.app_connection_id

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            timeout=None,
            app_connection_id=event.app_connection_id,
        )

        hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.asset_id,
            app_stream_id=event.app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        user_cache_sdk = UserRedisSdk(hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL)
        internal_cache_sdk = InternalRedisSdk(
            hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL
        )

        records = event.filter_records(
            old_max_record_value=event.get_cached_max_record_value(cache=user_cache_sdk)
        )

        if not records:
            # we've got the duplicate data if there are no records left after filtering
            return

        app_event = event.metadata.log_type.event.parse_obj(
            event.copy(update={'records': records}, deep=True)
        )
        with LoggingContext(
            aws_request_id=aws_request_id,
            asset_id=event.asset_id,
            app_connection_id=event.app_connection_id,
            handler=CorvaLoggerHandler(
                max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
                max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
                logger=CORVA_LOGGER,
                placeholder=' ...',
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
            CORVA_LOGGER.warning(f'Could not save data to cache. Details: {str(e)}.')

        return result

    return wrapper


def scheduled(
    func: Optional[Callable[[ScheduledEventT, Api, UserRedisSdk], Any]] = None,
    *,
    handler: Optional[logging.Handler] = None,
) -> Callable:
    """Runs scheduled app.

    Arguments:
        handler: logging handler to include in Corva logger.
    """

    if func is None:
        return functools.partial(scheduled, handler=handler)

    @functools.wraps(func)
    @functools.partial(base_handler, raw_event_type=RawScheduledEvent, handler=handler)
    def wrapper(
        event: RawScheduledEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
    ) -> Any:
        logging_ctx.asset_id = event.asset_id
        logging_ctx.app_connection_id = event.app_connection_id

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            timeout=None,
            app_connection_id=event.app_connection_id,
        )

        hash_name = get_cache_key(
            provider=SETTINGS.PROVIDER,
            asset_id=event.asset_id,
            app_stream_id=event.app_stream_id,
            app_key=SETTINGS.APP_KEY,
            app_connection_id=event.app_connection_id,
        )

        user_cache_sdk = UserRedisSdk(hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL)
        internal_cache_sdk = InternalRedisSdk(
            hash_name=hash_name, redis_dsn=SETTINGS.CACHE_URL
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
                placeholder=' ...',
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
        CORVA_LOGGER.warning(f'Could not set schedule as completed. Details: {str(e)}.')


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
    @functools.partial(base_handler, raw_event_type=RawTaskEvent, handler=handler)
    def wrapper(
        event: RawTaskEvent,
        api_key: str,
        aws_request_id: str,
        logging_ctx: LoggingContext,
    ) -> Any:
        status = TaskStatus.fail
        data: Dict[str, Union[dict, str]] = {"payload": {}}

        api = Api(
            api_url=SETTINGS.API_ROOT_URL,
            data_api_url=SETTINGS.DATA_API_ROOT_URL,
            api_key=api_key,
            app_key=SETTINGS.APP_KEY,
            timeout=None,
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
                    placeholder=' ...',
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

                data['payload'] = result

            status = TaskStatus.success

            return result

        except Exception as exc:
            CORVA_LOGGER.exception('Task app failed to execute.')
            data = {'fail_reason': str(exc)}

        finally:
            try:
                event.update_task_data(
                    api=api,
                    status=status,
                    data=data,
                ).raise_for_status()
            except Exception as e:
                # lambda succeeds if we're unable to update task data
                CORVA_LOGGER.warning(f'Could not update task data. Details: {str(e)}.')

    return wrapper
