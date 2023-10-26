from typing import List

from corva.models.base import CorvaBaseEvent
from corva.models.merge.enums import RerunModesEnum


class PartialMergeEvent(CorvaBaseEvent):
    """Partial Merge event data.

    Attributes:
        event_type: event type
        partial_well_rerun_id: partial well rerun id
        partition: partition
        rerun_partition: rerun partition
        rerun_mode: rerun mode
        start: start
        end: end
        asset_id: asset id
        rerun_asset_id: rerun asset id
        app_stream_id: app stream id
        rerun_app_stream_id: rerun app stream id
        version: version
        app_id: app id
        app_key: app key
        app_connection_ids: app connection ids
        rerun_app_connection_ids: rerun app connection ids
        source_type: source type
        log_type: log type
        run_until: run until
    """

    event_type: str
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
    version: int
    app_id: int
    app_key: str
    app_connection_ids: List[int]
    rerun_app_connection_ids: List[int]
    source_type: str
    log_type: str
    run_until: int
