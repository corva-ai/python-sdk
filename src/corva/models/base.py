from __future__ import annotations

import abc
from enum import Enum
from typing import Any, Sequence

import pydantic
from pydantic import ConfigDict


class AppType(str, Enum):
    STREAM = "stream"
    TASK = "task"
    SCHEDULER = "scheduler"


class CorvaBaseEvent(pydantic.BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)


class RawBaseEvent(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def from_raw_event(event: Any) -> Sequence[RawBaseEvent]:
        pass
