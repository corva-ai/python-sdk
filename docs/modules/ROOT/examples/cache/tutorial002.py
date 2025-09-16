import time

from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    cache.set(key='key', value='value', ttl=1)  # <.>
    assert cache.get('key') == 'value'

    time.sleep(2)  # <.>

    assert cache.get('key') is None  # <.>
