from typing import Any, Callable, Union

from corva.models.stream import RawStreamEvent, StreamDepthContext, StreamTimeContext


def stream_runner(
    fn: Callable, context: Union[StreamDepthContext, StreamTimeContext]
) -> Any:
    records = RawStreamEvent.filter_records(
        records=context.event.records, last_value=context.get_last_value()
    )

    if not records:
        # we've got the duplicate data if there are no records left after filtering
        return

    event = context.event.metadata.log_type.event.parse_obj(
        context.event.copy(update={'records': records}, deep=True)
    )

    result = fn(event, context.api, context.cache)

    context.set_last_value()

    return result
