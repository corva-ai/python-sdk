from corva.models.base import BaseContext
from corva.state.redis_state import RedisState


class ScheduledContext(BaseContext):
    state: RedisState
