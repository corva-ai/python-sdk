from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, TypeAdapter

from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.merge.enums import EventType, RerunMode, SourceType


class RawPartialMergeEventData(BaseModel):
    model_config = ConfigDict(extra="allow")

    asset_id: int
    rerun_asset_id: int
    app_stream_id: int
    rerun_app_stream_id: int
    version: int  # Partial re-run version
    app_connection_id: int
    rerun_app_connection_id: int

    source_type: Optional[SourceType] = None
    log_type: Optional[str] = None
    run_until: Optional[int] = None
    partial_well_rerun_id: Optional[int] = None
    rerun_mode: Optional[RerunMode] = None
    start: Optional[int] = None
    end: Optional[int] = None
    app_id: Optional[int] = None
    app_key: Optional[str] = None


class RawPartialRerunMergeEvent(CorvaBaseEvent, RawBaseEvent):
    event_type: EventType
    data: RawPartialMergeEventData
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Dict[str, Any]) -> List[RawPartialRerunMergeEvent]:
        return [TypeAdapter(RawPartialRerunMergeEvent).validate_python(event)]
