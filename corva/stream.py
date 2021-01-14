from typing import Any, Callable

from corva.models.stream import StreamContext, StreamEvent, StreamStateData


def stream(fn: Callable, context: StreamContext) -> Any:
    context.event = StreamEvent.filter(
        event=context.event,
        by_timestamp=context.filter_by_timestamp,
        by_depth=context.filter_by_depth,
        last_timestamp=context.cache_data.last_processed_timestamp,
        last_depth=context.cache_data.last_processed_depth
    )

    result = fn(context.event, context.api, context.cache)

    last_processed_timestamp = max(
        [
            record.timestamp
            for record in context.event.records
            if record.timestamp is not None
        ],
        default=context.cache_data.last_processed_timestamp
    )
    last_processed_depth = max(
        [
            record.measured_depth
            for record in context.event.records
            if record.measured_depth is not None
        ],
        default=context.cache_data.last_processed_depth
    )

    context.store_cache_data(
        StreamStateData(
            last_processed_timestamp=last_processed_timestamp,
            last_processed_depth=last_processed_depth
        )
    )

    return result
