from corva import Api, Cache, ScheduledTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledTimeEvent, api: Api, cache: Cache):  # <.>
    return 'Hello, World!'


def test_scheduled_app(app_runner):  # <.>
    event = ScheduledTimeEvent(
        asset_id=0, start_time=0, end_time=0, company_id=0
    )  # <.>

    result = app_runner(scheduled_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
