from datetime import datetime
from typing import Optional

from pydantic import Field

from corva.models.base import BaseContext, BaseEventData, ListEvent, BaseStateData


class ScheduledEventData(BaseEventData):
    type: Optional[str] = None
    collection: Optional[str] = None
    cron_string: str
    environment: str
    app: int
    app_key: str
    app_version: Optional[int]
    app_connection_id: int = Field(alias='app_connection')
    app_stream_id: int = Field(alias='app_stream')
    source_type: str
    company: int
    provider: str
    api_url: Optional[str] = None
    api_key: Optional[str] = None
    schedule: int
    interval: int
    schedule_start: datetime
    schedule_end: datetime
    asset_id: int
    asset_name: str
    asset_type: str
    timezone: str
    log_type: str
    log_identifier: Optional[str] = None
    day_shift_start: Optional[str] = None


class ScheduledEvent(ListEvent[ScheduledEventData]):
    pass


class ScheduledContext(BaseContext[ScheduledEvent, BaseStateData]):
    pass
