import inspect
from types import SimpleNamespace
from typing import Any, Callable, ClassVar, Union

from corva.api import Api
from corva.configuration import SETTINGS
from corva.models.scheduled.scheduled import ScheduledEvent
from corva.models.stream.stream import StreamEvent
from corva.models.task import TaskEvent
from corva.state.redis_state import get_cache


class TestClient:
    """Interface for testing Corva apps.

    Attributes:
        _context: AWS Lambda context expected by Corva.
        _api: Api instance.
    """

    _context: ClassVar[SimpleNamespace] = SimpleNamespace(
        aws_request_id='qwerty', client_context=SimpleNamespace(env={'API_KEY': '123'})
    )
    _api: ClassVar[Api] = Api(
        api_url=SETTINGS.API_ROOT_URL,
        data_api_url=SETTINGS.DATA_API_ROOT_URL,
        api_key=_context.client_context.env['API_KEY'],
        app_key=SETTINGS.APP_KEY,
    )

    @staticmethod
    def run(
        fn: Callable,
        event: Union[TaskEvent, StreamEvent, ScheduledEvent],
    ) -> Any:

        if isinstance(event, TaskEvent):
            return inspect.unwrap(fn)(event, TestClient._api)

        if isinstance(event, (ScheduledEvent, StreamEvent)):
            return inspect.unwrap(fn)(
                event,
                TestClient._api,
                get_cache(
                    asset_id=event.asset_id,
                    app_stream_id=int(),
                    app_connection_id=int(),
                    provider=SETTINGS.PROVIDER,
                    app_key=SETTINGS.APP_KEY,
                    cache_url=SETTINGS.CACHE_URL,
                ),
            )
