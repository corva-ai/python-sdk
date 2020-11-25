from datetime import datetime, timedelta

import pytest
from fakeredis import FakeServer
from freezegun import freeze_time
from redis import ConnectionError

from corva.state.redis import RedisState


def test_connect(redis):
    assert redis.ping()


def test_init_connect_exc(patch_redis):
    server = FakeServer()
    server.connected = False

    fake_cache_url = 'redis://random:123'

    with pytest.raises(ConnectionError) as exc:
        RedisState(cache_url=fake_cache_url, server=server)
    assert str(exc.value) == f'Could not connect to Redis with URL: {fake_cache_url}'


def test_hset(redis):
    name = 'name'
    key = 'key'
    value = 'val'
    assert redis.hset(name=name, key=key, value=value) == 1
    assert redis.hget(name=name, key=key).decode() == value


def test_hset_default_expiry(redis):
    name = 'name'
    with freeze_time('2020'):
        redis.hset(name=name, key='key', value='val')
        assert redis.ttl(name=name) == RedisState.DEFAULT_EXPIRY.total_seconds()


def test_hset_expiry_override(redis):
    name = 'name'
    with freeze_time('2020'):
        for expiry in [10, 5, 20]:
            redis.hset(name=name, key='key', value='val', expiry=expiry)
            assert redis.ttl(name=name) == expiry


def test_hset_expiry_disable(redis):
    name = 'name'
    key = 'key'
    value = 'val'
    with freeze_time('2020'):
        redis.hset(name=name, key=key, value=value, expiry=5)
        assert redis.ttl(name=name) == 5

        redis.hset(name=name, key=key, value=value, expiry=None)
        assert redis.ttl(name=name) == -1


def test_hset_expiry(redis):
    name = 'name'
    key = 'key'
    value = 'val'
    with freeze_time('2020') as frozen_time:
        now = datetime.utcnow()
        redis.hset(name=name, key=key, value=value, expiry=5)
        frozen_time.move_to(now + timedelta(seconds=5))
        assert redis.exists(name)
        frozen_time.move_to(now + timedelta(seconds=5, microseconds=1))
        assert not redis.exists(name)
