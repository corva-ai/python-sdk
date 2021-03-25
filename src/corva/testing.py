import types
from typing import Any, Callable, ClassVar, Union
from unittest import mock

from corva import ScheduledEvent, StreamEvent, TaskEvent
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

    @classmethod
    def _run_task(
        cls, fn: Callable, event: TaskEvent, context: types.SimpleNamespace
    ) -> Any:
        patches = [
            mock.patch('corva.runners.task.get_task_event', return_value=event),
            mock.patch('corva.runners.task.update_task_data'),
        ]

        raw_event = cls._to_raw_task_event(event=event)

        try:
            [patch.start() for patch in patches]
            return fn(raw_event, context)
        finally:
            mock.patch.stopall()

    @staticmethod
    def _to_raw_task_event(event: TaskEvent):
        return RawTaskEvent(task_id=str(), version=2).dict()
