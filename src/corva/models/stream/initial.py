from corva.models.base import CorvaBaseEvent
from corva.models.stream.log_type import LogType


class InitialMetadata(CorvaBaseEvent):
    log_type: LogType


class InitialStreamEvent(CorvaBaseEvent):
    """Stores the most essential data, that is parsed first."""

    metadata: InitialMetadata
