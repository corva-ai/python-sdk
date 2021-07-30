from corva import Api, Cache, ScheduledNaturalEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledNaturalEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
