from corva import Api, Cache, ScheduledDataTimeEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
