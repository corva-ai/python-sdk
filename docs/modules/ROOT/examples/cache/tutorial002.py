import time

from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    ttl_value = 1
    cache.set(key='key', value='value', ttl=ttl_value)  # <.>

    assert cache.get('key') == 'value'

    time.sleep(ttl_value + 0.01)  # <.>

    assert cache.get('key') is None  # <.>
