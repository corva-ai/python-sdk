import enum
from typing import Type

import pydantic


class SchedulerType(enum.Enum):
    natural_time = 1
    data_time = 2
    data_depth_milestone = 4

    @property
    def raw_event(self):
        from corva.models.scheduled.raw import (
            RawScheduledDataTimeEvent,
            RawScheduledDepthEvent,
            RawScheduledNaturalTimeEvent,
        )

        mapping = {
            type(self).natural_time: RawScheduledNaturalTimeEvent,
            type(self).data_time: RawScheduledDataTimeEvent,
            type(self).data_depth_milestone: RawScheduledDepthEvent,
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
            type(self).natural_time: ScheduledNaturalTimeEvent,
            type(self).data_time: ScheduledDataTimeEvent,
            type(self).data_depth_milestone: ScheduledDepthEvent,
        }
        return mapping[self]
