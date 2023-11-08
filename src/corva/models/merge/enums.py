from enum import Enum


class RerunMode(str, Enum):
    REALTIME = "realtime"
    HISTORICAL = "historical"


class EventType(str, Enum):
    PARTIAL_WELL_RERUN_MERGE = "partial-well-rerun-merge"


class SourceType(str, Enum):
    DRILLING = "drilling"
    COMPLETIONS = "completions"
