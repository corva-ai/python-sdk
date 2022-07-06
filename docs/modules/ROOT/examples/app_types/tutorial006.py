from corva import Api, Cache, ScheduledNaturalTimeEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
