from typing import List, TypeVar

from pydantic import BaseModel, Extra


class BaseEvent:
    pass


class BaseContext(BaseModel):
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

    event: BaseEvent


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow


BaseEventDataTV = TypeVar('BaseEventDataTV', bound=BaseEventData)


class ListEvent(BaseEvent, List[BaseEventDataTV]):
    pass
