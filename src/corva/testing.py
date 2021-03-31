import types
from typing import Any, Callable, ClassVar, Union
from unittest import mock

from corva import utils
from corva.api import Api
from corva.application import Corva
from corva.configuration import SETTINGS
from corva.models.scheduled import ScheduledEvent
from corva.models.stream.context import BaseStreamContext
from corva.models.stream.log_type import LogType
from corva.models.stream.raw import (
    RawAppMetadata,
    RawDepthRecord,
    RawMetadata,
    RawStreamDepthEvent,
    RawStreamEvent,
    RawStreamTimeEvent,
    RawTimeRecord,
)
from corva.models.stream.stream import StreamDepthEvent, StreamEvent, StreamTimeEvent
from corva.models.task import TaskEvent


class TestClient:
    """Interface for testing Corva apps.

    Attributes:
        _context: AWS Lambda context expected by Corva.
        _api: Api instance.
    """

    _context: ClassVar[types.SimpleNamespace] = types.SimpleNamespace(
        client_context=types.SimpleNamespace(env={'API_KEY': '123'})
    )
    _api: ClassVar[Api] = utils.get_api(context=_context, settings=SETTINGS)

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

        if isinstance(event, StreamEvent):
            return TestClient._run_stream(
                fn=fn, event=event, context=TestClient._context
            )

    @staticmethod
    def _run_task(
        fn: Callable, event: TaskEvent, context: types.SimpleNamespace
    ) -> Any:
        def override_task(self, fn: Callable, event: TaskEvent):
            return fn(event, TestClient._api)

        with mock.patch.object(Corva, 'task', override_task):
            return fn(event, context)

    @staticmethod
    def _run_scheduled(
        fn: Callable, event: ScheduledEvent, context: types.SimpleNamespace
    ) -> Any:
        def override_scheduled(self, fn: Callable, event: ScheduledEvent):
            return fn(
                event,
                TestClient._api,
                utils.get_cache(
                    asset_id=event.asset_id,
                    app_stream_id=int(),
                    app_connection_id=int(),
                    settings=SETTINGS,
                ),
            )

        with mock.patch.object(Corva, 'scheduled', override_scheduled):
            return fn(event, context)

    @staticmethod
    def _run_stream(
        fn: Callable,
        event: Union[StreamTimeEvent, StreamDepthEvent],
        context: types.SimpleNamespace,
    ) -> Any:
        patches = [
            mock.patch.object(
                RawStreamEvent, 'filter_records', return_value=event.records
            ),
            mock.patch.object(StreamTimeEvent, 'parse_obj', return_value=event),
            mock.patch.object(StreamDepthEvent, 'parse_obj', return_value=event),
            mock.patch.object(
                BaseStreamContext, 'get_last_value'
            ),  # avoid calls to cache instance
            mock.patch.object(
                BaseStreamContext, 'set_last_value'
            ),  # avoid calls to cache instance
        ]

        if isinstance(event, StreamTimeEvent):
            records = [
                RawTimeRecord.construct(
                    asset_id=event.asset_id,  # use asset_id from event to build proper cache key
                    company_id=int(),
                    collection=str(),
                    timestamp=int(),
                )
            ] * len(event.records)

            metadata = RawMetadata.construct(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.time,
            )

            raw_event = [
                RawStreamTimeEvent(
                    records=records,
                    metadata=metadata,
                ).dict()
            ]

        if isinstance(event, StreamDepthEvent):
            records = [
                RawDepthRecord(
                    asset_id=event.asset_id,  # use asset_id from event to build proper cache key
                    company_id=int(),
                    collection=str(),
                    measured_depth=float(),
                )
                for record in event.records
            ]

            metadata = RawMetadata(
                app_stream_id=int(),
                apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
                log_type=LogType.depth,
            )

            raw_event = [
                RawStreamDepthEvent(
                    records=records,
                    metadata=metadata,
                ).dict()
            ]

        try:
            [patch.start() for patch in patches]
            return fn(raw_event, context)[0]
        finally:
            mock.patch.stopall()
