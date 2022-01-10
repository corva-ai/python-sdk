from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    cache.set(key='key', value='value')  # <.>
    assert cache.get(key='key') == 'value'  # <.>

    cache.delete(key='key')  # <.>
    assert cache.get(key='key') is None  # <.>
