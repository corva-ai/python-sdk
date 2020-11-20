from typing import Optional

from worker.event.data.base import BaseEventData


class ScheduledEventData(BaseEventData):
    type: str
    collection: str
    cron_string: str
    environment: str
    app: int
    app_key: str
    app_version: Optional[str]
    app_connection: int
    source_type: str
    company: int
    provider: str
    api_url: str
    api_key: str
    schedule: int
    interval: int
    schedule_start: int
    schedule_end: int
    asset_id: int
    asset_name: str
    asset_type: str
    timezone: str
    log_identifier: Optional[str] = None
    day_shift_start: Optional[str] = None
