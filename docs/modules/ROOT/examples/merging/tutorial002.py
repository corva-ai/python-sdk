from corva import Api, Cache, ScheduledNaturalTimeEvent, scheduled


@scheduled(merge_events=True)
def app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
    return event
