from __future__ import annotations

from typing import Any, Dict, List, Optional

import pydantic

from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.merge.enums import EventType, RerunMode, SourceType


class RawPartialMergeEventData(pydantic.BaseModel):
    partial_well_rerun_id: Optional[int]
    rerun_mode: Optional[RerunMode]
    start: Optional[int]
    end: Optional[int]
    asset_id: int
    rerun_asset_id: int
    app_stream_id: int
    rerun_app_stream_id: int
    version: int  # Partial re-run version
    app_id: Optional[int]
    app_key: Optional[str]
    app_connection_id: int
    rerun_app_connection_id: int
    source_type: Optional[SourceType]
    log_type: Optional[str]
    run_until: Optional[int]

    class Config:
        extra = "allow"


class RawPartialRerunMergeEvent(CorvaBaseEvent, RawBaseEvent):
    event_type: EventType
    data: RawPartialMergeEventData
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Dict[str, Any]) -> List[RawPartialRerunMergeEvent]:
        return [pydantic.parse_obj_as(RawPartialRerunMergeEvent, event)]
