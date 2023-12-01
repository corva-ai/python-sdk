from corva import Api, ScheduledNaturalTimeEvent, scheduled, Cache


@scheduled(merge_events=True)
def app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
    return event
