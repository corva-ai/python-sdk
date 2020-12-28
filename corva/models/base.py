from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel

from corva.network.api import Api
from corva.state.redis_state import RedisState


class BaseEvent:
    pass


class BaseStateData(BaseModel):
    class Config:
        validate_assignment = True


BaseEventTV = TypeVar('BaseEventTV', bound=BaseEvent)
BaseStateDataTV = TypeVar('BaseStateDataTV', bound=BaseStateData)


class BaseContext(GenericModel, Generic[BaseEventTV, BaseStateDataTV]):
    """Used to pass different parameter sets to steps predefined in BaseApp.run function.

    Child classes of BaseApp may need:
      1 unique sets of parameters passed to each step (e.g.
        TaskApp.process(event, task_data) vs StreamApp.process(event, state))
      2 save data in some step, that will be used in the other one

    Instead of bloating BaseApp's steps with obsolete parameters (e.g. BaseApp.process(event, task_data, state),
      see above that `task_data` in used only in TaskApp and `state` - in StreamApp), context instances are used
      to contain all necessary parameters for app to run.
    """

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True

    raw_event: str
    user_kwargs: Dict[str, Any]
    app_key: str

    event: Optional[BaseEventTV] = None
    api: Optional[Api] = None
    state: Optional[RedisState] = None
    state_data: Optional[BaseStateDataTV] = None
    user_result: Any = None


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


BaseEventDataTV = TypeVar('BaseEventDataTV', bound=BaseEventData)


class ListEvent(BaseEvent, List[BaseEventDataTV]):
    pass
