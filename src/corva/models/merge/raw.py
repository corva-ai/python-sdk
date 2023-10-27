from __future__ import annotations

from typing import Dict, Any, List

import pydantic

from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.merge.enums import RerunModesEnum, EventTypesEnum, SourceTypesEnum


class RawPartialMergeEventData(pydantic.BaseModel):
    partial_well_rerun_id: int
    partition: int
    rerun_partition: int
    rerun_mode: RerunModesEnum
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
    app_connection_ids: List[int] = pydantic.Field(..., min_items=1)
    rerun_app_connection_ids: List[int] = pydantic.Field(..., min_items=1)
    source_type: SourceTypesEnum
    log_type: str
    run_until: int

    @pydantic.validator("rerun_app_connection_ids", "app_connection_ids", pre=True)
    def sort_connection_ids(cls, values):
        return sorted(values)


class RawPartialMergeEvent(CorvaBaseEvent, RawBaseEvent):
    event_type: EventTypesEnum
    data: RawPartialMergeEventData
    has_secrets: bool = False

    @staticmethod
    def from_raw_event(event: Dict[str, Any]) -> List[RawPartialMergeEvent]:
        return [pydantic.parse_obj_as(RawPartialMergeEvent, event)]
