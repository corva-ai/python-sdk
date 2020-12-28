from typing import Callable, Optional, Union

from corva.models.scheduled import ScheduledContext
from corva.models.stream import StreamContext
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


class GetStateKey:
    @classmethod
    def get_key(cls, asset_id: int, app_stream_id: int, app_key: str, app_connection_id: int):
        provider = cls.get_provider(app_key=app_key)
        state_key = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
        return state_key

    @staticmethod
    def get_provider(app_key: str) -> str:
        return app_key.split('.')[0]


def init_state_factory(
     *,
     cache_url: str,
     cache_kwargs: Optional[dict] = None
) -> Callable:
    def init_state(
         context: Union[StreamContext, ScheduledContext], call_next: Callable
    ) -> Union[StreamContext, ScheduledContext]:
        default_name = GetStateKey.get_key(
            asset_id=context.event[0].asset_id,
            app_stream_id=context.event[0].app_stream_id,
            app_key=context.app_key,
            app_connection_id=context.event[0].app_connection_id
        )

        adapter_params = dict(
            default_name=default_name,
            cache_url=cache_url,
            **(cache_kwargs or {})
        )

        context.state = RedisState(redis=RedisAdapter(**adapter_params))

        context = call_next(context)

        return context

    return init_state
