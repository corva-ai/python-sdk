from typing import ClassVar, Generic, Optional

from corva.models.base import BaseContext
from corva.models.stream.raw import (
    RawStreamDepthEvent,
    RawStreamEventTV,
    RawStreamTimeEvent,
)


class BaseStreamContext(BaseContext[RawStreamEventTV], Generic[RawStreamEventTV]):
    last_value_key: ClassVar[str]

    def get_last_value(self) -> Optional[float]:
        result = self.cache.load(key=self.last_value_key)

        if result is None:
            return result

        return float(result)

    def set_last_value(self) -> int:
        return self.cache.store(
            key=self.last_value_key, value=self.event.last_processed_value
        )


class StreamTimeContext(BaseStreamContext[RawStreamTimeEvent]):
    last_value_key: ClassVar[str] = 'last_processed_timestamp'


class StreamDepthContext(BaseStreamContext[RawStreamDepthEvent]):
    last_value_key: ClassVar[str] = 'last_processed_depth'
