from datetime import datetime, timedelta

import pytest
from fakeredis import FakeServer
from freezegun import freeze_time
from redis import ConnectionError

from corva.state.custom_redis import CustomRedis

NAME = 'NAME'
KEY = 'key'
VAL = 'val'
MAPPING = {'key1': 'val1', 'key2': 'val2'}


def test_connect(custom_redis):
    assert custom_redis.ping()


def test_init_connect_exc(patch_custom_redis):
    server = FakeServer()
    server.connected = False

    fake_cache_url = 'redis://random:123'

    with pytest.raises(ConnectionError) as exc:
        CustomRedis(default_name='name', cache_url=fake_cache_url, server=server)
    assert str(exc.value) == f'Could not connect to Redis with URL: {fake_cache_url}'


@pytest.mark.parametrize('name', (None, NAME))
def test_hset_and_hget(custom_redis, name):
    assert custom_redis.hset(name=name, key=KEY, value=VAL) == 1
    assert custom_redis.hget(name=name, key=KEY) == VAL


@pytest.mark.parametrize('name', (None, NAME))
def test_hset_mapping_and_hgetall(custom_redis, name):
    assert custom_redis.hset(name=NAME, mapping=MAPPING) == len(MAPPING)
    assert custom_redis.hgetall(name=NAME) == MAPPING


@pytest.mark.parametrize('name', (None, NAME))
def test_hdel_and_exists(custom_redis, name):
    if name is None:
        exists = lambda: custom_redis.exists()
    else:
        exists = lambda: custom_redis.exists(name)

    assert custom_redis.hset(name=name, key=KEY, value=VAL) == 1
    assert exists()
    assert custom_redis.hdel(keys=[KEY], name=name) == 1
    assert not exists()


@pytest.mark.parametrize('name', (None, NAME))
def test_delete_and_exists(custom_redis, name):
    if name is None:
        exists = lambda: custom_redis.exists()
        delete = lambda: custom_redis.delete()
    else:
        exists = lambda: custom_redis.exists(name)
        delete = lambda: custom_redis.delete(name)

    assert custom_redis.hset(name=name, key=KEY, value=VAL) == 1
    assert exists()
    assert delete()
    assert not exists()


@pytest.mark.parametrize('name', (None, NAME))
def test_ttl(custom_redis, name):
    with freeze_time('2020'):
        assert custom_redis.hset(name=name, key=KEY, value=VAL) == 1
        assert custom_redis.ttl(name=name) == custom_redis.DEFAULT_EXPIRY.total_seconds()


@pytest.mark.parametrize('name', (None, NAME))
def test_pttl(custom_redis, name):
    with freeze_time('2020'):
        assert custom_redis.hset(name=name, key=KEY, value=VAL) == 1
        assert custom_redis.pttl(name=name) == custom_redis.DEFAULT_EXPIRY.total_seconds() * 1000


def test_hset_default_expiry(custom_redis):
    with freeze_time('2020'):
        custom_redis.hset(key=KEY, value=VAL)
        assert custom_redis.ttl() == CustomRedis.DEFAULT_EXPIRY.total_seconds()


def test_store_expiry_override(custom_redis):
    with freeze_time('2020'):
        for expiry in [10, 5, 20]:
            custom_redis.hset(key=KEY, value=VAL, expiry=expiry)
            assert custom_redis.ttl() == expiry


def test_store_expiry_disable(custom_redis):
    with freeze_time('2020'):
        custom_redis.hset(key=KEY, value=VAL, expiry=5)
        assert custom_redis.ttl() == 5

        custom_redis.hset(key=KEY, value=VAL, expiry=None)
        assert custom_redis.ttl() == -1


def test_store_expiry(custom_redis):
    with freeze_time('2020') as frozen_time:
        now = datetime.utcnow()
        custom_redis.hset(key=KEY, value=VAL, expiry=5)
        frozen_time.move_to(now + timedelta(seconds=5))
        assert custom_redis.exists()
        frozen_time.move_to(now + timedelta(seconds=5, microseconds=1))
        assert not custom_redis.exists()
