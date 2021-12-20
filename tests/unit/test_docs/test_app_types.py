from corva import (
    ScheduledDataTimeEvent,
    ScheduledDepthEvent,
    ScheduledNaturalTimeEvent,
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
    TaskEvent,
)
from docs.src.app_types import (
    tutorial001,
    tutorial002,
    tutorial003,
    tutorial004,
    tutorial005,
    tutorial006,
)


def test_tutorial001(app_runner):
    event = StreamTimeEvent(
        asset_id=0, company_id=0, records=[StreamTimeRecord(timestamp=0)]
    )

    assert app_runner(tutorial001.stream_time_app, event) == 'Hello, World!'


def test_tutorial002(app_runner):
    event = StreamDepthEvent(
        asset_id=0, company_id=0, records=[StreamDepthRecord(measured_depth=0)]
    )

    assert app_runner(tutorial002.stream_depth_app, event) == 'Hello, World!'


def test_tutorial003(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    assert app_runner(tutorial003.scheduled_app, event) == 'Hello, World!'


def test_tutorial004(app_runner):
    event = TaskEvent(asset_id=0, company_id=0)

    assert app_runner(tutorial004.task_app, event) == 'Hello, World!'


def test_tutorial005(app_runner):
    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=1.0,
        log_identifier='',
        interval=1.0,
    )

    assert app_runner(tutorial005.scheduled_app, event) == 'Hello, World!'


def test_tutorial006(app_runner):
    event = ScheduledNaturalTimeEvent(
        asset_id=0, company_id=0, schedule_start=0, interval=1
    )

    assert app_runner(tutorial006.scheduled_app, event) == 'Hello, World!'
