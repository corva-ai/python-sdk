import pytest

from corva.configuration import SETTINGS
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
from corva.service.cache_sdk import UserRedisSdk


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


def test_reuse_cache(app_runner):
    """
    Testing that cache is reset or reused between runs.
    """

    @scheduled
    def scheduled_fibonacci(event, api, cache):
        number1 = int(cache.get('number1') or 1)
        number2 = int(cache.get('number2') or 1)
        number3 = number1 + number2
        cache.set('number1', number2)
        cache.set('number2', number3)

        return number3

    event = ScheduledDataTimeEvent(asset_id=0, company_id=0, start_time=0, end_time=0)

    # resetting cache - so the results should be the same
    for _ in range(5):
        assert app_runner(scheduled_fibonacci, event) == 2

    # reusing cache
    cache = UserRedisSdk(
        hash_name='hash_name',
        redis_dsn=SETTINGS.CACHE_URL,
        use_fakes=True,
    )
    expected_results = [2, 3, 5, 8, 13, 21, 34, 55]
    for expected_result in expected_results:
        assert app_runner(scheduled_fibonacci, event, cache=cache) == expected_result
