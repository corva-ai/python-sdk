from typing import Callable

from corva.models.stream import StreamContext, StreamEvent, StreamStateData


def stream(context: StreamContext, call_next: Callable) -> StreamContext:
    """Stores needed data in state for future runs."""

    context.event = StreamEvent.filter(
        event=context.event,
        by_timestamp=context.filter_by_timestamp,
        by_depth=context.filter_by_depth,
        last_timestamp=context.cache_data.last_processed_timestamp,
        last_depth=context.cache_data.last_processed_depth
    )

    context = call_next(context)  # type: StreamContext

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
            last_processed_timestamp=last_processed_timestamp, last_processed_depth=last_processed_depth
        )
    )

    return context
