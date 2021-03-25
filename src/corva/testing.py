import types
from typing import Any, Callable, ClassVar, Union
from unittest import mock

from corva import ScheduledEvent, StreamEvent, TaskEvent
from corva.models.scheduled import RawScheduledEvent
from corva.models.task import RawTaskEvent


class TestClient:
    """Interface for testing Corva apps.

    Attributes:
        _context: AWS Lambda context expected by Corva.
    """

    _context: ClassVar[types.SimpleNamespace] = types.SimpleNamespace(
        client_context=types.SimpleNamespace(env={'API_KEY': '123'})
    )

    @staticmethod
    def run(
        fn: Callable[[dict, Any], Any],
        event: Union[TaskEvent, StreamEvent, ScheduledEvent],
    ) -> Any:
        if isinstance(event, TaskEvent):
            return TestClient._run_task(fn=fn, event=event, context=TestClient._context)

        if isinstance(event, ScheduledEvent):
            return TestClient._run_scheduled(
                fn=fn, event=event, context=TestClient._context
            )

    @staticmethod
    def _run_task(
        fn: Callable, event: TaskEvent, context: types.SimpleNamespace
    ) -> Any:
        patches = [
            mock.patch('corva.runners.task.get_task_event', return_value=event),
            mock.patch('corva.runners.task.update_task_data'),
        ]

        raw_event = RawTaskEvent(task_id=str(), version=2)

        try:
            [patch.start() for patch in patches]
            return fn(raw_event, context)
        finally:
            mock.patch.stopall()

    @staticmethod
    def _run_scheduled(
        fn: Callable, event: ScheduledEvent, context: types.SimpleNamespace
    ) -> Any:
        patches = [
            mock.patch(
                'corva.runners.scheduled.ScheduledEvent.parse_obj', return_value=event
            ),
            mock.patch('corva.runners.scheduled.set_schedule_as_completed'),
        ]

        raw_event = [
            [
                RawScheduledEvent(
                    asset_id=event.asset_id,  # use asset_id from event to build proper cache key
                    interval=int(),
                    schedule=int(),
                    schedule_start=int(),
                    app_connection=int(),
                    app_stream=int(),
                )
            ]
        ]

        try:
            [patch.start() for patch in patches]
            return fn(raw_event, context)[0]
        finally:
            mock.patch.stopall()
