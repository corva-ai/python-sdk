from corva import Api, Cache, ScheduledTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledTimeEvent, api: Api, cache: Cache):
    cache.store(mapping={'key1': 'val1', 'key2': 'val2', 'key3': 'val3'})  # <1>

    cache.delete(keys=['key1'])  # <2>
    assert cache.load_all() == {'key2': 'val2', 'key3': 'val3'}  # <3>

    cache.delete_all()  # <4>
    assert cache.load_all() == {}  # <5>
