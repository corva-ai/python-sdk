from __future__ import annotations

import abc
import copy
from typing import Dict, Generic, List, Optional, TypeVar, Union

import pydantic.generics

from corva.configuration import SETTINGS
from corva.models.base import CorvaBaseEvent, CorvaBaseGenericEvent, RawBaseEvent
from corva.models.stream import validators
from corva.models.stream.initial import InitialStreamEvent
from corva.models.stream.log_type import LogType


class RawBaseRecord(CorvaBaseEvent, abc.ABC):
    asset_id: int
    company_id: int
    collection: str

    data: dict = {}
    metadata: dict = {}

    @property
    @abc.abstractmethod
    def main_value(self) -> Union[int, float]:
        pass


class RawTimeRecord(RawBaseRecord):
    timestamp: int
    measured_depth: Optional[float] = None

    @property
    def main_value(self) -> int:
        return self.timestamp


class RawDepthRecord(RawBaseRecord):
    timestamp: Optional[int] = None
    measured_depth: float

    @property
    def main_value(self) -> float:
        return self.measured_depth


class RawAppMetadata(CorvaBaseEvent):
    app_connection_id: int


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
        event: RawStreamEvent,
        last_value: Optional[float],
    ) -> List[RawBaseRecord]:
        new_records = copy.deepcopy(event.records)

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

    _require_at_least_one_record = pydantic.root_validator(
        pre=False, skip_on_failure=True, allow_reuse=True
    )(validators.require_at_least_one_record)

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def require_app_key_in_metadata_apps(cls, values):
        metadata: RawMetadata = values['metadata']

        if values['app_key'] not in metadata.apps:
            raise ValueError('metadata.apps dict must contain an app key.')

        return values

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_asset_id(cls, values: dict) -> dict:
        """Calculates asset_id field."""

        records: List[RawBaseRecord] = values['records']

        values["asset_id"] = int(records[0].asset_id)

        return values

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def set_company_id(cls, values: dict) -> dict:
        """Calculates company_id field."""

        records: List[RawBaseRecord] = values['records']

        values["company_id"] = int(records[0].company_id)

        return values


class RawStreamTimeEvent(RawStreamEvent[RawTimeRecord]):
    pass


class RawStreamDepthEvent(RawStreamEvent[RawDepthRecord]):
    pass


RawStreamEventTV = TypeVar('RawStreamEventTV', bound=RawStreamEvent)
