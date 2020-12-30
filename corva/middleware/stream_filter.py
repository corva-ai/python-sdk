from typing import Callable

from corva.models.stream import StreamContext, StreamEvent, StreamEventData


def stream_filter_factory(*, by_timestamp: bool = False, by_depth: bool = False) -> Callable:
    def stream_filter(context: StreamContext, call_next: Callable) -> StreamContext:
        context.event = _filter_event(
            event=context.event,
            by_timestamp=by_timestamp,
            by_depth=by_depth,
            last_processed_timestamp=context.state_data.last_processed_timestamp,
            last_processed_depth=context.state_data.last_processed_depth
        )

        context = call_next(context)

        return context

    return stream_filter


def _filter_event(
     event: StreamEvent,
     by_timestamp: bool,
     by_depth: bool,
     last_processed_timestamp: int,
     last_processed_depth: float
) -> StreamEvent:
    data = []
    for subdata in event:  # type: StreamEventData
        data.append(
            _filter_event_data(
                data=subdata,
                by_timestamp=by_timestamp,
                by_depth=by_depth,
                last_processed_timestamp=last_processed_timestamp,
                last_processed_depth=last_processed_depth
            )
        )

    return StreamEvent(data)


def _filter_event_data(
     data: StreamEventData,
     by_timestamp: bool,
     by_depth: bool,
     last_processed_timestamp: int,
     last_processed_depth: float
) -> StreamEventData:
    records = data.records

    if data.is_completed:
        records = records[:-1]  # remove "completed" record

    new_records = []
    for record in records:
        if by_timestamp and record.timestamp <= last_processed_timestamp:
            continue
        if by_depth and record.measured_depth <= last_processed_depth:
            continue

        new_records.append(record)

    return data.copy(update={'records': new_records}, deep=True)
