from itertools import chain
from typing import List

from corva.loader.base import BaseLoader
from corva.models.scheduled import ScheduledEventData, ScheduledEvent


class ScheduledLoader(BaseLoader):
    parse_as_type = List[List[ScheduledEventData]]

    def load(self, event: str) -> ScheduledEvent:
        parsed = self.parse(event=event)  # type: ScheduledLoader.parse_as_type
        parsed = list(chain(*parsed))

        return ScheduledEvent(parsed)
