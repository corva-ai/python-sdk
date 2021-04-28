from corva import Api, Cache, ScheduledEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
