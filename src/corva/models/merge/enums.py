from enum import Enum


class RerunModesEnum(str, Enum):
    REALTIME = "realtime"
    HISTORICAL = "historical"


class EventTypesEnum(str, Enum):
    PARTIAL_WELL_RERUN_MERGE = "partial-well-rerun-merge"


class SourceTypesEnum(str, Enum):
    DRILLING = "drilling"
    COMPLETIONS = "completions"
