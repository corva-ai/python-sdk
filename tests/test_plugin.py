import pytest

from corva.handlers import scheduled, stream, task
from corva.models.scheduled.scheduled import (
    ScheduledDataTimeEvent,
    ScheduledDepthEvent,
    ScheduledNaturalTimeEvent,
)
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


@pytest.mark.parametrize(
    'event',
    (
        ScheduledDataTimeEvent(
            asset_id=0,
            company_id=0,
            start_time=0,
            end_time=0,
        ),
        ScheduledDepthEvent(
            asset_id=0,
            company_id=0,
            top_depth=0.0,
            bottom_depth=0.0,
            log_identifier='',
            interval=0.0,
        ),
        ScheduledNaturalTimeEvent(
            asset_id=0,
            company_id=0,
            schedule_start=0,
            interval=0.0,
        ),
    ),
)
def test_scheduled_app_runner(event, app_runner):
    """Should not raise."""

    @scheduled
    def scheduled_app(event, api, cache):
        return 'Scheduled app result'

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
