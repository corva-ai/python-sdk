from types import SimpleNamespace
from unittest.mock import patch

import fakeredis
import pytest

from corva.app.base import BaseApp
from corva.event.base import BaseEvent
from corva.state.redis import RedisState


@pytest.fixture(scope='function', autouse=True)
def patch_redis():
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeRedis(server=server)
    with patch('redis.client.Redis.from_url', return_value=redis):
        yield SimpleNamespace(server=server, redis=redis)


@pytest.fixture(scope='function')
def redis(patch_redis):
    return RedisState()


@pytest.fixture(scope='function')
def patch_base_event():
    with patch.object(BaseEvent, '__abstractmethods__', set()):
        yield


@pytest.fixture(scope='function')
def patch_base_app():
    # mock abstract methods of BaseApp, so we can initialize and test the class
    # mock event_cls attribute of BaseApp, so we can use fake load function
    with patch.object(BaseApp, '__abstractmethods__', set()), \
         patch.object(BaseApp, 'event_cls') as event_cls_mock:
        event_cls_mock.load.side_effect = lambda event: event
        yield SimpleNamespace(event_cls_mock=event_cls_mock)
