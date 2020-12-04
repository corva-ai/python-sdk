from pydantic import BaseModel

from corva.app.utils.task_model import TaskData
from corva.event.event import Event
from corva.state.redis_state import RedisState


class BaseContext(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    event: Event


class ScheduledContext(BaseContext):
    state: RedisState


class StreamContext(BaseContext):
    state: RedisState


class TaskContext(BaseContext):
    task: TaskData
    save_data: dict = {}
