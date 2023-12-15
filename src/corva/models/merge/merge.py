from corva.models.base import CorvaBaseEvent
from corva.models.merge.enums import EventType, RerunMode, SourceType


class PartialRerunMergeEvent(CorvaBaseEvent):
    """Partial Merge event data.

    Attributes:
        event_type: EventType.PARTIAL_WELL_RERUN_MERGE
        partial_well_rerun_id: partial well rerun id
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
        app_connection_id: app connection id
        rerun_app_connection_id: rerun app connection id
        source_type: source type
        log_type: log type
        run_until: run until
    """

    event_type: EventType
    partial_well_rerun_id: int
    rerun_mode: RerunMode
    start: int
    end: int
    asset_id: int
    rerun_asset_id: int
    app_stream_id: int
    rerun_app_stream_id: int
    version: int
    app_id: int
    app_key: str
    app_connection_id: int
    rerun_app_connection_id: int
    source_type: SourceType
    log_type: str
    run_until: int

    class Config:
        extra = "allow"
