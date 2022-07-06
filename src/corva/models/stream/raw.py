from __future__ import annotations

import abc
import copy
from typing import TYPE_CHECKING, ClassVar, List, Optional, Sequence, Union

import pydantic

from corva.configuration import SETTINGS
from corva.models.base import CorvaBaseEvent, RawBaseEvent
from corva.models.rerun import RerunDepth, RerunTime
from corva.models.stream.initial import InitialStreamEvent
from corva.models.stream.log_type import LogType
from corva.service.cache_sdk import UserCacheSdkProtocol


class RawBaseRecord(CorvaBaseEvent, abc.ABC):
    asset_id: int
    company_id: int
    collection: str

    data: dict = {}
    metadata: dict = {}

    @property
    @abc.abstractmethod
    def record_value(self) -> Union[int, float]:
        pass


class RawTimeRecord(RawBaseRecord):
    timestamp: int
    measured_depth: Optional[float] = None

    @property
    def record_value(self) -> int:
        return self.timestamp


class RawDepthRecord(RawBaseRecord):
    timestamp: Optional[int] = None
    measured_depth: float

    @property
    def record_value(self) -> float:
        return self.measured_depth


class RawAppMetadata(CorvaBaseEvent):
    app_connection_id: int
    has_secrets: bool = False


class RawMetadata(CorvaBaseEvent):
    app_stream_id: int
    apps: pydantic.create_model(  # type: ignore
        "Apps",  # noqa: F821
        **{
            SETTINGS.APP_KEY: (
                RawAppMetadata,
                ...,
            )
        },
    )
    log_type: LogType


if TYPE_CHECKING:
    RecordsBase = Sequence[RawBaseRecord]
    RecordsTime = Sequence[RawTimeRecord]
    RecordsDepth = Sequence[RawDepthRecord]
else:
    RecordsBase = pydantic.conlist(RawBaseRecord, min_items=1)
    RecordsTime = pydantic.conlist(RawTimeRecord, min_items=1)
    RecordsDepth = pydantic.conlist(RawDepthRecord, min_items=1)


class RawStreamEvent(CorvaBaseEvent, RawBaseEvent):
    records: RecordsBase
    metadata: RawMetadata
    asset_id: int = None  # type: ignore
    company_id: int = None  # type: ignore

    # private attributes
    _max_record_value_cache_key: ClassVar[str]

    @property
    def app_connection_id(self) -> int:
        return getattr(self.metadata.apps, SETTINGS.APP_KEY).app_connection_id

    @property
    def has_secrets(self) -> bool:
        return getattr(self.metadata.apps, SETTINGS.APP_KEY).has_secrets

    @property
    def app_stream_id(self) -> int:
        return self.metadata.app_stream_id

    @property
    def is_completed(self) -> bool:
        """Indicates whether there is a completed record.

        There can only be 1 completed record always located at the end of the list.
        """

        return self.records[-1].collection == 'wits.completed'

    @property
    def max_record_value(self) -> Union[int, float]:
        return max(record.record_value for record in self.records)

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

    def get_cached_max_record_value(
        self, cache: UserCacheSdkProtocol
    ) -> Optional[float]:
        result = cache.get(key=self._max_record_value_cache_key)

        if result is None:
            return result

        return float(result)

    def set_cached_max_record_value(self, cache: UserCacheSdkProtocol) -> None:
        cache.set(
            key=self._max_record_value_cache_key, value=str(self.max_record_value)
        )

    def filter_records(
        self,
        old_max_record_value: Optional[float],
    ) -> List[RawBaseRecord]:
        new_records = copy.deepcopy(self.records)

        if self.is_completed:
            new_records = new_records[:-1]  # remove "completed" record

        if old_max_record_value is None:
            return list(new_records)

        values = [record.record_value for record in new_records]

        new_records = [
            record
            for record, value in zip(new_records, values)
            if value > old_max_record_value
        ]

        return new_records

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


class RawStreamTimeEvent(RawStreamEvent):
    records: RecordsTime
    rerun: Optional[RerunTime] = None
    _max_record_value_cache_key: ClassVar[str] = 'last_processed_timestamp'


class RawStreamDepthEvent(RawStreamEvent):
    records: RecordsDepth
    rerun: Optional[RerunDepth] = None
    _max_record_value_cache_key: ClassVar[str] = 'last_processed_depth'
