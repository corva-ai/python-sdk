import json

from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    cache.set(key='json', value=json.dumps({'int': 0, 'str': 'text'}))  # <.>
    assert isinstance(cache.get('json'), str)  # <.>
    assert json.loads(cache.get('json')) == {'int': 0, 'str': 'text'}  # <.>
