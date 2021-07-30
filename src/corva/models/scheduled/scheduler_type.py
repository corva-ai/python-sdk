import enum
from typing import Type

import pydantic


class SchedulerType(enum.Enum):
    natural_time = 1
    data_time = 2
    data_depth_milestone = 4

    @property
    def raw_event(self) -> Type[pydantic.BaseModel]:
        from corva.models.scheduled.raw import (
            RawScheduledDepthEvent,
            RawScheduledNaturalEvent,
            RawScheduledTimeEvent,
        )

        mapping = {
            self.natural_time: RawScheduledNaturalEvent,
            self.data_time: RawScheduledTimeEvent,
            self.data_depth_milestone: RawScheduledDepthEvent,
        }
        return mapping[self]

    @property
    def event(self) -> Type[pydantic.BaseModel]:
        from corva.models.scheduled.scheduled import (
            ScheduledDepthEvent,
            ScheduledNaturalEvent,
            ScheduledTimeEvent,
        )

        mapping = {
            self.natural_time: ScheduledNaturalEvent,
            self.data_time: ScheduledTimeEvent,
            self.data_depth_milestone: ScheduledDepthEvent,
        }
        return mapping[self]
