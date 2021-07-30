from corva.models.base import CorvaBaseEvent
from corva.models.scheduled.scheduler_type import SchedulerType


class InitialScheduledEvent(CorvaBaseEvent):
    """Stores the most essential data, that is parsed first."""

    scheduler_type: SchedulerType
