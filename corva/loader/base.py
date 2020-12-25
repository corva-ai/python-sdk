from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import parse_raw_as

from corva.models.base import BaseEvent


class BaseLoader(ABC):
    parse_as_type: ClassVar[Any]

    @abstractmethod
    def load(self, event: str) -> BaseEvent:
        pass

    @classmethod
    def parse(cls, event: str) -> Any:
        return parse_raw_as(cls.parse_as_type, event)
