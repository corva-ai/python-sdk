from __future__ import annotations

import abc
from enum import Enum
from typing import Any, Sequence

import pydantic


class AppType(str, Enum):
    STREAM = "stream"
    TASK = "task"
    SCHEDULED = "scheduled"


class CorvaBaseEvent(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.allow
        allow_mutation = False


class RawBaseEvent(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def from_raw_event(event: Any) -> Sequence[RawBaseEvent]:
        pass

    @classmethod
    def get_app_type(cls) -> AppType:
        ...
