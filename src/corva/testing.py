import types
from typing import Any, Callable, ClassVar, Union
from unittest import mock

from corva.api import Api, get_api
from corva.application import Corva
from corva.configuration import SETTINGS
from corva.models.scheduled import ScheduledEvent
from corva.models.stream.stream import StreamDepthEvent, StreamEvent, StreamTimeEvent
from corva.models.task import TaskEvent
from corva.state.redis_state import get_cache


class TestClient:
    """Interface for testing Corva apps.

    Attributes:
        _context: AWS Lambda context expected by Corva.
        _api: Api instance.
    """

    _context: ClassVar[types.SimpleNamespace] = types.SimpleNamespace(
        aws_request_id='qwerty',
        client_context=types.SimpleNamespace(env={'API_KEY': '123'}),
    )
    _api: ClassVar[Api] = get_api(context=_context, settings=SETTINGS)

    @staticmethod
    def run(
        fn: Callable[[dict, Any], Any],
        event: Union[TaskEvent, StreamEvent, ScheduledEvent],
    ) -> Any:

        if isinstance(event, TaskEvent):
            return TestClient._run_task(fn=fn, event=event)

        if isinstance(event, ScheduledEvent):
            return TestClient._run_scheduled(fn=fn, event=event)

        if isinstance(event, StreamEvent):
            return TestClient._run_stream(fn=fn, event=event)

    @staticmethod
    def _run_task(fn: Callable, event: TaskEvent) -> Any:
        def override_task(self, fn: Callable, event: TaskEvent):
            return fn(event, TestClient._api)

        with mock.patch.object(Corva, 'task', override_task):
            return fn(event, TestClient._context)

    @staticmethod
    def _run_scheduled(fn: Callable, event: ScheduledEvent) -> Any:
        def override_scheduled(self, fn: Callable, event: ScheduledEvent):
            return fn(
                event,
                TestClient._api,
                get_cache(
                    asset_id=event.asset_id,
                    app_stream_id=int(),
                    app_connection_id=int(),
                    settings=SETTINGS,
                ),
            )

        with mock.patch.object(Corva, 'scheduled', override_scheduled):
            return fn(event, TestClient._context)

    @staticmethod
    def _run_stream(
        fn: Callable,
        event: Union[StreamTimeEvent, StreamDepthEvent],
    ) -> Any:
        def override_stream(self, fn: Callable, event: StreamEvent):
            return fn(
                event,
                TestClient._api,
                get_cache(
                    asset_id=event.asset_id,
                    app_stream_id=int(),
                    app_connection_id=int(),
                    settings=SETTINGS,
                ),
            )

        with mock.patch.object(Corva, 'stream', override_stream):
            return fn(event, TestClient._context)
