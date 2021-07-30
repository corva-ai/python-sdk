from corva import Api, Cache, ScheduledTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledTimeEvent, api: Api, cache: Cache):
    cache.store(key='key', value='')  # <1>
    assert cache.load(key='key') == ''  # <2>

    cache.store(key='key', value=0)  # <3>
    assert cache.load(key='key') == '0'  # <4>

    cache.store(key='key', value=0.0)  # <5>
    assert cache.load(key='key') == '0.0'  # <4>

    cache.store(key='key', value=b'')  # <6>
    assert cache.load(key='key') == ''  # <4>

    cache.store(mapping={'key': 'val', 'other-key': 'other-val'})  # <7>
    assert cache.load_all() == {'key': 'val', 'other-key': 'other-val'}  # <8>
