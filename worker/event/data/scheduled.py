from typing import Optional

from worker.event.data.base import BaseEventData


class ScheduledEventData(BaseEventData):
    type: Optional[str] = None
    collection: Optional[str] = None
    cron_string: str
    environment: str
    app: int
    app_key: str
    app_version: Optional[int]
    app_connection: int
    app_stream: int
    source_type: str
    company: int
    provider: str
    api_url: Optional[str] = None
    api_key: Optional[str] = None
    schedule: int
    interval: int
    schedule_start: int
    schedule_end: int
    asset_id: int
    asset_name: str
    asset_type: str
    timezone: str
    log_type: str = None
    log_identifier: Optional[str] = None
    day_shift_start: Optional[str] = None
