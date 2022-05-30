from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    assert cache.get_all() == {}  # <.>

    cache.set_many(data=[('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3'), ('k4', 'v4')])  # <.>

    cache.delete_many(keys=['k1', 'k2'])  # <.>
    assert cache.get_all() == {'k3': 'v3', 'k4': 'v4'}  # <.>

    cache.delete_all()  # <.>
    assert cache.get_all() == {}  # <.>
