from corva import Api, Cache, ScheduledDepthEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDepthEvent, api: Api, cache: Cache):  # <.>
    return 'Hello, World!'


def test_scheduled_app(app_runner):  # <.>
    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=1.0,
        log_identifier='',
        interval=1.0,
    )  # <.>

    result = app_runner(scheduled_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
