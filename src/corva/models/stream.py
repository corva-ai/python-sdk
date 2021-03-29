from __future__ import annotations

import abc
import copy
import enum
from typing import ClassVar, Dict, Generic, List, Optional, TypeVar, Union

import pydantic.generics

from corva.configuration import SETTINGS
from corva.models.base import (
    BaseContext,
    CorvaBaseEvent,
    CorvaBaseGenericEvent,
    RawBaseEvent,
)


def require_at_least_one_record(records: list) -> list:
    """Validates, that there is at least one record provided.

    This function exists, because pydantic.conlist doesnt support generics.
    """

    if not records:
        raise ValueError('At least one record should be provided.')

    return records


class StreamBaseRecord(CorvaBaseEvent):
    pass


class StreamTimeRecord(StreamBaseRecord):
    """Stream time record data.

    Attributes:
        timestamp: Unix timestamp.
        data: record data.
        metadata: record metadata.
    """

    timestamp: int
    data: dict = {}
    metadata: dict = {}


class StreamDepthRecord(StreamBaseRecord):
    """Stream depth record data.

    Attributes:
        measured_depth: measured depth (ft).
        data: record data.
        metadata: record metadata.
    """

    measured_depth: float
    data: dict = {}
    metadata: dict = {}


StreamBaseRecordTV = TypeVar('StreamBaseRecordTV', bound=StreamBaseRecord)


class StreamEvent(CorvaBaseGenericEvent, Generic[StreamBaseRecordTV]):
    """Stream event data.

    Attributes:
        asset_id: asset id.
        company_id: company id.
        records: data records.
    """

    asset_id: int
    company_id: int
    records: List[StreamBaseRecordTV]

    # validators
    _records = pydantic.validator('records', allow_reuse=True)(
        require_at_least_one_record
    )


class StreamTimeEvent(StreamEvent[StreamTimeRecord]):
    pass


class StreamDepthEvent(StreamEvent[StreamDepthRecord]):
    pass


RawRecordMainValueTV = TypeVar('RawRecordMainValueTV', int, float)


class RawBaseRecord(CorvaBaseGenericEvent, Generic[RawRecordMainValueTV], abc.ABC):
    asset_id: int
    company_id: int
    collection: str

    data: dict = {}
    metadata: dict = {}

    @property
    @abc.abstractmethod
    def main_value(self) -> RawRecordMainValueTV:
        pass


class RawTimeRecord(RawBaseRecord[int]):
    timestamp: int
    measured_depth: Optional[float] = None

    @property
    def main_value(self) -> int:
        return self.timestamp


class RawDepthRecord(RawBaseRecord[float]):
    timestamp: Optional[int] = None
    measured_depth: float

    @property
    def main_value(self) -> float:
        return self.measured_depth


class RawAppMetadata(CorvaBaseEvent):
    app_connection_id: int


class LogType(enum.Enum):
    time = 'time'
    depth = 'depth'

    @property
    def raw_event(self):
        mapping = {self.time: RawStreamTimeEvent, self.depth: RawStreamDepthEvent}
        return mapping[self]

    @property
    def context(self):
        mapping = {self.time: StreamTimeContext, self.depth: StreamDepthContext}
        return mapping[self]

    @property
    def event(self):
        mapping = {self.time: StreamTimeEvent, self.depth: StreamDepthEvent}
        return mapping[self]


class RawMetadata(CorvaBaseEvent):
    app_stream_id: int
    apps: Dict[str, RawAppMetadata]
    log_type: LogType


RawBaseRecordTV = TypeVar('RawBaseRecordTV', bound=RawBaseRecord)


class RawStreamEvent(CorvaBaseGenericEvent, Generic[RawBaseRecordTV], RawBaseEvent):
    records: List[RawBaseRecordTV]
    metadata: RawMetadata
    app_key: str = SETTINGS.APP_KEY
    asset_id: int = None
    company_id: int = None

    # validators
    _records = pydantic.validator('records', allow_reuse=True)(
        require_at_least_one_record
    )

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_asset_id(cls, values: dict) -> dict:
        """Calculates asset_id field."""

        records: List[RawRecord] = values['records']

        values["asset_id"] = int(records[0].asset_id)

        return values

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_company_id(cls, values: dict) -> dict:
        """Calculates company_id field."""

        records: List[RawRecord] = values['records']

        values["company_id"] = int(records[0].company_id)

        return values

    @property
    def app_connection_id(self) -> int:
        return self.metadata.apps[self.app_key].app_connection_id

    @property
    def app_stream_id(self) -> int:
        return self.metadata.app_stream_id

    @property
    def is_completed(self) -> bool:
        """There can only be 1 completed record, always located at the end of the list."""

        return self.records[-1].collection == 'wits.completed'

    @property
    def last_processed_value(self) -> Union[int, float]:
        return max(record.main_value for record in self.records)

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def require_app_key_in_metadata_apps(cls, values):
        metadata: RawMetadata = values['metadata']

        if values['app_key'] not in metadata.apps:
            raise ValueError('metadata.apps dict must contain an app key.')

        return values

    @staticmethod
    def from_raw_event(event: List[dict]) -> List[RawStreamEvent]:
        initial_events: List[InitialStreamEvent] = pydantic.parse_obj_as(
            List[InitialStreamEvent], event
        )

        result = [
            initial_event.metadata.log_type.raw_event.parse_obj(sub_event)
            for initial_event, sub_event in zip(initial_events, event)
        ]

        return result

    @staticmethod
    def filter_records(
        records: List[RawBaseRecord],
        last_value: Union[float, int, None],
    ) -> List[RawBaseRecord]:
        if not records:
            return records

        new_records = copy.deepcopy(records)

        if event.is_completed:
            # there can be only 1 completed record, always located at the end
            new_records = new_records[:-1]  # remove "completed" record

        if last_value is None:
            return new_records

        values = [record.main_value for record in new_records]

        new_records = [
            record for record, value in zip(new_records, values) if value > last_value
        ]

        return new_records


class RawStreamTimeEvent(RawStreamEvent[RawTimeRecord]):
    pass


class RawStreamDepthEvent(RawStreamEvent[RawDepthRecord]):
    pass


RawStreamEventTV = TypeVar('RawStreamEventTV', bound=RawStreamEvent)


class BaseStreamContext(BaseContext[RawStreamEventTV], Generic[RawStreamEventTV]):
    last_value_key: ClassVar[str]

    def get_last_value(self) -> Union[None, int, float]:
        return self.cache.load(key=self.last_value_key)

    def set_last_value(self) -> int:
        return self.cache.store(
            key=self.last_value_key, value=self.event.last_processed_value
        )


class StreamTimeContext(BaseStreamContext[RawStreamTimeEvent]):
    last_value_key = 'last_processed_timestamp'


class StreamDepthContext(BaseStreamContext[RawStreamDepthEvent]):
    last_value_key = 'last_processed_depth'


class InitialMetadata(CorvaBaseEvent):
    log_type: LogType


class InitialStreamEvent(CorvaBaseEvent):
    """Stores the most essential data, that is parsed first."""

    metadata: InitialMetadata
