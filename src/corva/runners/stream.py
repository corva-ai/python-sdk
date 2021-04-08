from typing import Any, Callable, Union

from corva.logger import setup_logging
from corva.models.stream.context import StreamDepthContext, StreamTimeContext
from corva.models.stream.raw import RawStreamEvent


def stream_runner(
    fn: Callable, context: Union[StreamDepthContext, StreamTimeContext]
) -> Any:
    records = RawStreamEvent.filter_records(
        event=context.event, last_value=context.get_last_value()
    )

    if not records:
        # we've got the duplicate data if there are no records left after filtering
        return

    event = context.event.metadata.log_type.event.parse_obj(
        context.event.copy(update={'records': records}, deep=True)
    )

    with setup_logging(
        aws_request_id=context.aws_request_id,
        asset_id=context.event.asset_id,
        app_connection_id=context.event.app_connection_id,
    ):
        result = fn(event, context.api, context.cache)

    context.set_last_value()

    return result
