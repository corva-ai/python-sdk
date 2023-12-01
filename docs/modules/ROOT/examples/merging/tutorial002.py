from corva import Api  # <.>
from corva import Cache, ScheduledNaturalTimeEvent, scheduled


@scheduled(merge_events=True)
def app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
    return event
