from itertools import chain
from typing import Optional, List

from corva.app.base import BaseApp
from corva.models.stream import StreamContext, Record, StreamEventData
from corva.event import Event
from corva.loader.stream import StreamLoader
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState
from corva.utils import GetStateKey


class StreamApp(BaseApp):
    DEFAULT_LAST_PROCESSED_VALUE = -1

    group_by_field = 'app_connection_id'

    def __init__(self, filter_by_timestamp: bool = False, filter_by_depth: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filter_by_timestamp = filter_by_timestamp
        self.filter_by_depth = filter_by_depth

    @property
    def event_loader(self) -> StreamLoader:
        return StreamLoader(app_key=self.app_key)

    def get_context(self, event: Event) -> StreamContext:
        return StreamContext(
            event=event,
            state=RedisState(
                redis=RedisAdapter(
                    default_name=GetStateKey.from_event(event=event, app_key=self.app_key),
                    cache_url=self.cache_url,
                    logger=self.logger
                ),
                logger=self.logger
            )
        )

    def pre_process(self, context: StreamContext) -> None:
        last_processed_timestamp = (
            int(context.state.load(key='last_processed_timestamp') or self.DEFAULT_LAST_PROCESSED_VALUE)
            if self.filter_by_timestamp
            else self.DEFAULT_LAST_PROCESSED_VALUE
        )

        last_processed_depth = (
            float(context.state.load(key='last_processed_depth') or self.DEFAULT_LAST_PROCESSED_VALUE)
            if self.filter_by_depth
            else self.DEFAULT_LAST_PROCESSED_VALUE
        )

        event = self._filter_event(
            event=context.event,
            last_processed_timestamp=last_processed_timestamp,
            last_processed_depth=last_processed_depth
        )

        context.event = event

    def post_process(self, context: StreamContext) -> None:
        all_records: List[Record] = list(chain(*[subdata.records for subdata in context.event]))

        last_processed_timestamp = max(
            [
                record.timestamp
                for record in all_records
                if record.timestamp is not None
            ],
            default=None
        )
        last_processed_depth = max(
            [
                record.measured_depth
                for record in all_records
                if record.measured_depth is not None
            ],
            default=None
        )

        mapping = {}
        if last_processed_timestamp is not None:
            mapping['last_processed_timestamp'] = last_processed_timestamp
        if last_processed_depth is not None:
            mapping['last_processed_depth'] = last_processed_depth

        context.state.store(mapping=mapping)

    @classmethod
    def _filter_event(
         cls,
         event: Event,
         last_processed_timestamp: Optional[int],
         last_processed_depth: Optional[float]
    ) -> Event:
        data = []
        for subdata in event:  # type: StreamEventData
            data.append(
                cls._filter_event_data(
                    data=subdata,
                    last_processed_timestamp=last_processed_timestamp,
                    last_processed_depth=last_processed_depth
                )
            )

        return Event(data)

    @staticmethod
    def _filter_event_data(
         data: StreamEventData,
         last_processed_timestamp: Optional[int] = None,
         last_processed_depth: Optional[float] = None
    ) -> StreamEventData:
        records = data.records

        if data.is_completed:
            records = records[:-1]  # remove "completed" record

        new_records = []
        for record in records:
            if last_processed_timestamp is not None and record.timestamp <= last_processed_timestamp:
                continue
            if last_processed_depth is not None and record.measured_depth <= last_processed_depth:
                continue
            new_records.append(record)

        return data.copy(update={'records': new_records}, deep=True)
