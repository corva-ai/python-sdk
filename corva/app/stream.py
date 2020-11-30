from itertools import chain
from typing import Any, Optional, List

from corva.app.base import BaseApp, ProcessResult
from corva.event.base import BaseEvent
from corva.event.data.stream import StreamEventData, Record
from corva.event.stream import StreamEvent
from corva.state.redis_state import RedisState


class StreamApp(BaseApp):
    DEFAULT_LAST_PROCESSED_TIMESTAMP = -1
    DEFAULT_LAST_PROCESSED_DEPTH = -1

    event_cls = StreamEvent

    def __init__(self, filter_by_timestamp: bool = False, filter_by_depth: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filter_by_timestamp = filter_by_timestamp
        self.filter_by_depth = filter_by_depth

    def run(
         self,
         event: str,
         load_kwargs: Optional[dict] = None,
         pre_process_kwargs: Optional[dict] = None,
         process_kwargs: Optional[dict] = None,
         post_process_kwargs: Optional[dict] = None,
         on_fail_before_post_process_kwargs: Optional[dict] = None
    ) -> Any:
        load_kwargs = {'app_key': self.app_key, **(load_kwargs or {})}
        return super(StreamApp, self).run(
            event=event,
            load_kwargs=load_kwargs,
            pre_process_kwargs=pre_process_kwargs,
            process_kwargs=process_kwargs,
            post_process_kwargs=post_process_kwargs,
            on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
        )

    def pre_process(self, event: BaseEvent, state: RedisState, **kwargs) -> ProcessResult:
        event = super().pre_process(event=event, state=state, **kwargs).event

        last_processed_timestamp = (
            int(state.load(key='last_processed_timestamp'))
            if self.filter_by_timestamp
            else self.DEFAULT_LAST_PROCESSED_TIMESTAMP
        )
        last_processed_depth = (
            float(state.load(key='last_processed_depth'))
            if self.filter_by_depth
            else self.DEFAULT_LAST_PROCESSED_DEPTH
        )

        event = self._filter_event(
            event=event,
            last_processed_timestamp=last_processed_timestamp,
            last_processed_depth=last_processed_depth
        )

        return ProcessResult(event=event)

    def post_process(self, event: BaseEvent, state: RedisState, **kwargs) -> ProcessResult:
        event = super(StreamApp, self).post_process(event=event, state=state, **kwargs).event

        all_records: List[Record] = list(chain(*[subdata.records for subdata in event]))
        last_processed_timestamp = max(
            [record.timestamp for record in all_records],
            default=self.DEFAULT_LAST_PROCESSED_TIMESTAMP
        )
        last_processed_depth = max(
            [
                record.measured_depth
                for record in all_records
                if record.measured_depth is not None
            ],
            default=self.DEFAULT_LAST_PROCESSED_DEPTH
        )

        mapping = {'last_processed_timestamp': last_processed_timestamp,
                   'last_processed_depth': last_processed_depth}

        state.store(mapping=mapping)

        return ProcessResult(event=event)

    @classmethod
    def _filter_event(
         cls,
         event: BaseEvent,
         last_processed_timestamp: Optional[int],
         last_processed_depth: Optional[float]
    ) -> StreamEvent:
        data = []
        for subdata in event:  # type: StreamEventData
            data.append(
                cls._filter_event_data(
                    data=subdata,
                    last_processed_timestamp=last_processed_timestamp,
                    last_processed_depth=last_processed_depth
                )
            )
        return StreamEvent(data=data)

    @staticmethod
    def _filter_event_data(
         data: StreamEventData,
         last_processed_timestamp: Optional[int] = None,
         last_processed_depth: Optional[float] = None
    ) -> StreamEventData:
        records = data.records
        if data.is_completed:
            records = records[:-1]  # remove "completed" record

        result = []
        for record in records:
            if last_processed_timestamp is not None and record.timestamp <= last_processed_timestamp:
                continue
            if last_processed_depth is not None and record.measured_depth <= last_processed_depth:
                continue
            result.append(record)
        return data.copy(update={'records': result}, deep=True)
