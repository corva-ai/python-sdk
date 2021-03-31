from typing import Optional

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
