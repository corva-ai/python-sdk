from corva import Api, Cache, ScheduledTimeEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledTimeEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
