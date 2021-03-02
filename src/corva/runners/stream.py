from typing import Any, Callable

from corva.models.stream import StreamContext, StreamEvent, StreamStateData


def stream_runner(fn: Callable, context: StreamContext) -> Any:
    records = StreamEvent.filter_records(
        event=context.event,
        filter_mode=context.filter_mode,
        last_value=context.last_processed_value,
    )

    if not records:
        # we got a duplicate data if there are no records after filtering
        return

    context.event = context.event.copy(update={'records': records}, deep=True)

    result = fn(context.event, context.api, context.cache)

    last_processed_timestamp = max(
        [
            record.timestamp
            for record in context.event.records
            if record.timestamp is not None
        ],
        default=None,  # old cache value wont be overwritten
    )
    last_processed_depth = max(
        [
            record.measured_depth
            for record in context.event.records
            if record.measured_depth is not None
        ],
        default=None,  # old cache value wont be overwritten
    )

    context.store_cache_data(
        StreamStateData(
            last_processed_timestamp=last_processed_timestamp,
            last_processed_depth=last_processed_depth,
        )
    )

    return result
