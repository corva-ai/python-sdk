import datetime
import time

from corva import Api, Cache, ScheduledEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
    cache.store(key='key', value='val', expiry=60)  # <.>
    cache.store(key='key', value='val', expiry=datetime.timedelta(seconds=60))  # <.>

    assert cache.ttl() == 60  # <.>
    assert cache.pttl() == 60000  # <.>
    assert cache.exists()  # <.>

    time.sleep(60)  # <.>

    assert not cache.exists()  # <.>
