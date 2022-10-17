from corva import (
    Api,
    Cache,
    RerunDepth,
    RerunDepthRange,
    RerunTime,
    RerunTimeRange,
    ScheduledDataTimeEvent,
    ScheduledDepthEvent,
    ScheduledNaturalTimeEvent,
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
    scheduled,
    stream,
)


@stream
def stream_time_app(event: StreamTimeEvent, api: Api, cache: Cache):
    assert event.rerun


@stream
def stream_depth_app(event: StreamDepthEvent, api: Api, cache: Cache):
    assert event.rerun


@scheduled
def scheduled_data_time_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    assert event.rerun


@scheduled
def scheduled_depth_app(event: ScheduledDepthEvent, api: Api, cache: Cache):
    assert event.rerun


@scheduled
def scheduled_natural_time_app(
    event: ScheduledNaturalTimeEvent, api: Api, cache: Cache
):
    assert event.rerun


def test_stream_time_app(app_runner):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=0,
        records=[StreamTimeRecord(timestamp=0)],
        rerun=RerunTime(range=RerunTimeRange(start=0, end=0), invoke=0, total=0),
    )

    app_runner(stream_time_app, event)


def test_stream_depth_app(app_runner):
    event = StreamDepthEvent(
        asset_id=0,
        company_id=0,
        records=[StreamDepthRecord(measured_depth=0)],
        rerun=RerunDepth(range=RerunDepthRange(start=0.0, end=0.0), invoke=0, total=0),
    )

    app_runner(stream_depth_app, event)


def test_scheduled_data_time_app(app_runner):
    event = ScheduledDataTimeEvent(
        asset_id=0,
        start_time=0,
        end_time=0,
        company_id=0,
        rerun=RerunTime(range=RerunTimeRange(start=0, end=0), invoke=0, total=0),
    )

    app_runner(scheduled_data_time_app, event)


def test_scheduled_depth_app(app_runner):
    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=1.0,
        log_identifier="",
        interval=1.0,
        rerun=RerunDepth(range=RerunDepthRange(start=0.0, end=0.0), invoke=0, total=0),
    )

    app_runner(scheduled_depth_app, event)


def test_scheduled_natural_time_app(app_runner):
    event = ScheduledNaturalTimeEvent(
        asset_id=0,
        company_id=0,
        schedule_start=0,
        interval=1,
        rerun=RerunTime(range=RerunTimeRange(start=0, end=0), invoke=0, total=0),
    )

    app_runner(scheduled_natural_time_app, event)
