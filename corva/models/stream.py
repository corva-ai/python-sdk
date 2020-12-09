from corva.models.base import BaseContext
from corva.state.redis_state import RedisState


class StreamContext(BaseContext):
    state: RedisState
