from datetime import datetime, timedelta

import pytest
from fakeredis import FakeServer
from freezegun import freeze_time
from redis import ConnectionError

from corva.settings import CORVA_SETTINGS
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


@pytest.fixture(scope='function')
def redis():
    redis_adapter = RedisAdapter(name='name', cache_url=CORVA_SETTINGS.CACHE_URL)
    return RedisState(redis=redis_adapter)


def test_init_connect_exc():
    server = FakeServer()
    server.connected = False

    fake_cache_url = 'redis://random:123'

    with pytest.raises(ConnectionError) as exc:
        RedisAdapter(name='name', cache_url=fake_cache_url, server=server)
    assert str(exc.value) == f'Could not connect to Redis with URL: {fake_cache_url}'


def test_store_and_load(redis):
    assert redis.store(key='key', value='val') == 1
    assert redis.load(key='key') == 'val'


def test_store_mapping_and_load_all(redis):
    mapping = {'key1': 'val1', 'key2': 'val2'}

    assert redis.store(mapping=mapping) == len(mapping)
    assert redis.load_all() == mapping


def test_delete_and_exists(redis):
    assert redis.store(key='key', value='val') == 1
    assert redis.exists()
    assert redis.delete(keys=['key']) == 1
    assert not redis.exists()


def test_delete_all_and_exists(redis):
    assert redis.store(key='key', value='val') == 1
    assert redis.exists()
    assert redis.delete_all()
    assert not redis.exists()


def test_ttl(redis):
    with freeze_time('2020'):
        assert redis.store(key='key', value='val') == 1
        assert redis.ttl() > 0


def test_pttl(redis):
    with freeze_time('2020'):
        assert redis.store(key='key', value='val') == 1
        assert redis.pttl() > 0


def test_store_expiry_override(redis):
    with freeze_time('2020'):
        for expiry in [10, 5, 20]:
            redis.store(key='key', value='val', expiry=expiry)
            assert redis.ttl() == expiry


def test_store_expiry_disable(redis):
    with freeze_time('2020'):
        redis.store(key='key', value='val', expiry=5)
        assert redis.ttl() == 5

        redis.store(key='key', value='val', expiry=None)
        assert redis.ttl() == -1


def test_store_expiry(redis):
    with freeze_time('2020') as frozen_time:
        now = datetime.utcnow()
        redis.store(key='key', value='val', expiry=5)
        frozen_time.move_to(now + timedelta(seconds=5))
        assert redis.exists()
        frozen_time.move_to(now + timedelta(seconds=5, microseconds=1))
        assert not redis.exists()
