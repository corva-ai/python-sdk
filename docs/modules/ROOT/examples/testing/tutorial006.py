from corva import Api, Cache, ScheduledNaturalTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):  # <.>
    return 'Hello, World!'


def test_scheduled_app(app_runner):  # <.>
    event = ScheduledNaturalTimeEvent(
        asset_id=0, company_id=0, schedule_start=0, interval=1
    )  # <.>

    result = app_runner(scheduled_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
