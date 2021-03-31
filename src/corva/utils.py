from typing import Any, Optional

from corva import Api
from corva.configuration import Settings
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


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


def get_cache(
    asset_id: int,
    app_stream_id: int,
    app_connection_id: int,
    settings: Settings,
    cache_settings: Optional[dict] = None,
) -> RedisState:
    cache_settings = cache_settings or {}

    redis_adapter = RedisAdapter(
        default_name=get_cache_key(
            provider=settings.PROVIDER,
            asset_id=asset_id,
            app_stream_id=app_stream_id,
            app_key=settings.APP_KEY,
            app_connection_id=app_connection_id,
        ),
        cache_url=settings.CACHE_URL,
        **cache_settings,
    )

    return RedisState(redis=redis_adapter)


def get_api(context: Any, settings: Settings, timeout: Optional[int] = None) -> Api:
    """Returns Api instance.

    Args:
        context: AWS Lambda context.
        settings:
        timeout: custom api timeout.

    Raises:
          Exception: if cound not find an api key in the context.
    """

    try:
        api_key = context.client_context.env["API_KEY"]
    except (AttributeError, KeyError):
        raise Exception('No API Key found.')

    return Api(
        api_url=settings.API_ROOT_URL,
        data_api_url=settings.DATA_API_ROOT_URL,
        api_key=api_key,
        app_name=settings.APP_NAME,
        timeout=timeout,
    )
