from abc import ABC, abstractmethod
from logging import Logger, LoggerAdapter
from typing import List, Union

from corva.logger import LOGGER


class BaseState(ABC):
    def __init__(self, logger: Union[Logger, LoggerAdapter] = LOGGER):
        self.logger = logger

    @abstractmethod
    def load(self, state_key: str) -> dict:
        ...

    @abstractmethod
    def save(self, state: dict, state_key: str) -> bool:
        ...

    @abstractmethod
    def delete(self, state_keys: List[str]) -> int:
        ...
