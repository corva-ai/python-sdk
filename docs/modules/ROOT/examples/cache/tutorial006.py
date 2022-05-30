import time

from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    assert cache.get_all() == {}  # <.>

    cache.set_many(  # <.>
        data=[
            ('key', 'value_1'),
            ('key_with_custom_expiry', 'value_2', 1),  # <.>
        ]
    )

    assert cache.get_many(  # <.>
        keys=[
            'key',
            'non-existent-key',  # <.>
        ]
    ) == {'key': 'value_1', 'non-existent-key': None}

    assert cache.get_all() == {  # <.>
        'key': 'value_1',
        'key_with_custom_expiry': 'value_2',
    }

    time.sleep(1)  # <.>

    assert cache.get_all() == {'key': 'value_1'}  # <.>
