from corva import Api  # <1>
from corva import Cache, ScheduledNaturalTimeEvent, scheduled


@scheduled  # <3>
def scheduled_app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
