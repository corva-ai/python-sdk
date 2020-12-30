from itertools import chain
from typing import Callable

from corva.models.stream import StreamContext, StreamStateData


def stream(context: StreamContext, call_next: Callable) -> StreamContext:
    context = call_next(context)  # type: StreamContext

    all_records = list(chain(*[subdata.records for subdata in context.event]))

    last_processed_timestamp = max(
        [
            record.timestamp
            for record in all_records
            if record.timestamp is not None
        ],
        default=StreamStateData.__fields__['last_processed_timestamp'].default
    )
    last_processed_depth = max(
        [
            record.measured_depth
            for record in all_records
            if record.measured_depth is not None
        ],
        default=StreamStateData.__fields__['last_processed_depth'].default
    )

    context.state_data = StreamStateData(
        last_processed_timestamp=last_processed_timestamp, last_processed_depth=last_processed_depth
    )

    context.state.store(mapping=context.state_data.dict(exclude_defaults=True, exclude_none=True))

    return context
