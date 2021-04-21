import pytest

from corva.handlers import scheduled, stream, task
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

    @task
    def task_app(event, api):
        return 'Task app result'

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(task_app, event) == 'Task app result'


def test_scheduled_app_runner(app_runner):
    """Should not raise."""

    @scheduled
    def scheduled_app(event, api, cache):
        return 'Scheduled app result'

    event = ScheduledEvent(asset_id=int(), start_time=int(), end_time=int())

    assert app_runner(scheduled_app, event) == 'Scheduled app result'


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

    @stream
    def stream_app(event, api, cache):
        return 'Stream app result'

    assert app_runner(stream_app, event) == 'Stream app result'
