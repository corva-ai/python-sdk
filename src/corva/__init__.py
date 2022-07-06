from .api import Api
from .handlers import scheduled, stream, task
from .logger import CORVA_LOGGER as Logger
from .models.rerun import RerunDepth, RerunDepthRange, RerunTime, RerunTimeRange
from .models.scheduled.scheduled import (
    ScheduledDataTimeEvent,
    ScheduledDepthEvent,
    ScheduledNaturalTimeEvent,
)
from .models.stream.stream import (
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
)
from .models.task import TaskEvent
from .service.cache_sdk import UserRedisSdk as Cache
from .shared import SECRETS as secrets


def __getattr__(name):
    import warnings

    if name == "ScheduledEvent":
        warnings.warn(
            "The corva.ScheduledEvent class is deprecated "
            "and will be removed from corva in the next major version. "
            "Import corva.ScheduledDataTimeEvent instead.",
            FutureWarning,
            stacklevel=2,
        )

        return ScheduledDataTimeEvent

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
