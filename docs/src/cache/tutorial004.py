from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    cache.set(key='int', value=str(0))  # <.>
    assert cache.get('int') == '0'  # <.>
    assert int(cache.get('int')) == 0  # <.>
