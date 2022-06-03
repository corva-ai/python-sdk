from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    cache.set(key='str', value='text')  # <.>
    assert cache.get(key='str') == 'text'  # <.>
