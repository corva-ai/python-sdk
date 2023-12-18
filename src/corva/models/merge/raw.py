from __future__ import annotations

from typing import Any, Dict, List

import pydantic

from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.merge.enums import EventType, RerunMode, SourceType


class RawPartialMergeEventData(pydantic.BaseModel):
    partial_well_rerun_id: int
    rerun_mode: RerunMode
    start: int
    end: int
    asset_id: int
    rerun_asset_id: int
    app_stream_id: int
    rerun_app_stream_id: int
    version: int = pydantic.Field(
        ..., le=1, ge=1
    )  # Currently handler supports only 1-st version of this event.
    app_id: int
    app_key: str
    app_connection_id: int
    rerun_app_connection_id: int
    source_type: SourceType
    log_type: str
    run_until: int

    class Config:
        extra = "allow"


class RawPartialRerunMergeEvent(CorvaBaseEvent, RawBaseEvent):
    event_type: EventType
    data: RawPartialMergeEventData
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Dict[str, Any]) -> List[RawPartialRerunMergeEvent]:
        return [pydantic.parse_obj_as(RawPartialRerunMergeEvent, event)]
