from abc import ABC, abstractmethod
from typing import List


class BaseState(ABC):
    @abstractmethod
    def load(self, state_key: str) -> dict:
        ...

    @abstractmethod
    def save(self, state: dict, state_key: str) -> bool:
        ...

    @abstractmethod
    def delete(self, state_keys: List[str]) -> int:
        ...
