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
            RawScheduledDataTimeEvent,
            RawScheduledDepthEvent,
            RawScheduledNaturalTimeEvent,
        )

        mapping = {
            self.natural_time: RawScheduledNaturalTimeEvent,
            self.data_time: RawScheduledDataTimeEvent,
            self.data_depth_milestone: RawScheduledDepthEvent,
        }
        return mapping[self]

    @property
    def event(self) -> Type[pydantic.BaseModel]:
        from corva.models.scheduled.scheduled import (
            ScheduledDataTimeEvent,
            ScheduledDepthEvent,
            ScheduledNaturalTimeEvent,
        )

        mapping = {
            self.natural_time: ScheduledNaturalTimeEvent,
            self.data_time: ScheduledDataTimeEvent,
            self.data_depth_milestone: ScheduledDepthEvent,
        }
        return mapping[self]
