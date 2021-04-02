import contextlib

import pydantic
import pytest

from corva.application import Corva
from corva.models.scheduled import ScheduledEvent
from corva.models.stream.stream import (
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
)
from corva.models.task import TaskEvent


def test_task_app_runner(app_runner):
    """Should not raise."""

    def task_app(event, api):
        return 'Task app result'

    def lambda_handler(event, context):
        return Corva(context).task(fn=task_app, event=event)

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(lambda_handler, event) == 'Task app result'


def test_scheduled_app_runner(app_runner):
    """Should not raise."""

    def scheduled_app(event, api, cache):
        return 'Scheduled app result'

    def lambda_handler(event, context):
        return Corva(context).scheduled(fn=scheduled_app, event=event)

    event = ScheduledEvent(asset_id=int(), start_time=int(), end_time=int())

    assert app_runner(lambda_handler, event) == 'Scheduled app result'


@pytest.mark.parametrize(
    'event',
    (
        StreamTimeEvent(
            asset_id=int(),
            company_id=int(),
            records=[StreamTimeRecord(timestamp=int())],
        ),
        StreamDepthEvent(
            asset_id=int(),
            company_id=int(),
            records=[StreamDepthRecord(measured_depth=float())],
        ),
    ),
)
def test_stream_app_runner(event, app_runner):
    """Should not raise."""

    def stream_app(event, api, cache):
        return 'Stream app result'

    def lambda_handler(event, context):
        return Corva(context).stream(fn=stream_app, event=event)

    assert app_runner(lambda_handler, event) == 'Stream app result'


@pytest.mark.parametrize(
    'event_dict,exc_ctx',
    (
        [
            StreamTimeEvent.construct(
                asset_id=int(),
                company_id=int(),
                records=[],
            ).dict(),
            pytest.raises(
                pydantic.ValidationError,
                match=r'1 validation error [\s\S]* At least one record should be provided\.',
            ),
        ],
        [
            StreamTimeEvent(
                asset_id=int(),
                company_id=int(),
                records=[StreamTimeRecord(timestamp=int())],
            ).dict(),
            contextlib.nullcontext(),
        ],
    ),
    ids=('raises', 'not raises'),
)
def test_stream_event_raises_for_empty_records(event_dict: dict, exc_ctx, app_runner):
    """

    Users will never receive a StreamEvent with empty records.
    So they should not be able to test with such events.
    """

    with exc_ctx:
        StreamTimeEvent(**event_dict)
