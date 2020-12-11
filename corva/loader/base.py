import json
from abc import abstractmethod, ABC

from corva.types import EVENT_TYPE
from corva.event import Event


class BaseLoader(ABC):
    @abstractmethod
    def load(self, event: str) -> Event:
        pass

    @staticmethod
    def _load_json(event: str) -> EVENT_TYPE:
        try:
            event = json.loads(event)
        except ValueError as exc:
            raise ValueError('Invalid event JSON') from exc

        return event
